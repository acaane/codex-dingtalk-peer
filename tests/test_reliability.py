import json
from pathlib import Path
import subprocess
import tempfile
import time
import types
import unittest
from unittest.mock import patch

import peer
from reliability import DeliveryUnknown, PeerError, RetryablePeerError, SendRejected, Store, transient_model_error


class FakeTransport:
    def __init__(self, errors=()):
        self.errors = list(errors)
        self.sent = []

    def send(self, text):
        self.sent.append(text)
        if self.errors:
            error = self.errors.pop(0)
            if error:
                raise error


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.path = Path(self.folder.name) / "state.sqlite3"
        self.store = Store(self.path)
        self.config = {"group_id": "group", "max_steps": 6, "max_retries": 2, "task_timeout_seconds": 900,
                       "retry_initial_delay_seconds": 1, "retry_max_delay_seconds": 10,
                       "peers": [{"id": "A", "name": "A", "sender_id": "bot-A", "owner_id": "owner-A"},
                                 {"id": "B", "name": "B", "sender_id": "bot-B", "owner_id": "owner-B"}]}
        self.local = self.config["peers"][0]
        self.transport = FakeTransport()
        self.session = peer.PeerSession(self.config, self.local, self.store, self.transport, Path(self.folder.name))
        self.task = "0123456789ab"
        self.answer = {"action": "reply", "to": "B", "text": "已核对，请确认字段"}

    def tearDown(self):
        self.store.db.close()
        self.folder.cleanup()

    def event(self, step=1, kind="request", source="B", body="检查字段", task=None):
        return {"conversation_id": "group", "sender_open_dingtalk_id": "bot-" + source,
                "message_id": f"{source}-{step}-{kind}",
                "content": peer.encode(task or self.task, source, "A", step, kind, "文档核对", body)}

    def reopen(self):
        self.store.db.close()
        self.store = Store(self.path)
        self.session = peer.PeerSession(self.config, self.local, self.store, self.transport, Path(self.folder.name))

    def make_due(self):
        with self.store.db:
            self.store.db.execute("UPDATE work SET next_at=0")

    def test_timeout_retries_same_turn_after_restart_and_keeps_history(self):
        self.session.handle(self.event())
        with patch.object(peer, "agent_reply", side_effect=RetryablePeerError("timeout")):
            self.session.tick()
        self.assertEqual(self.store.get(self.task)["status"], "retry_wait")
        self.assertGreater(self.store.work(self.task)["next_at"], time.time())
        self.reopen()
        self.make_due()
        with patch.object(peer, "agent_reply", return_value=self.answer) as agent:
            self.session.tick()
        self.assertEqual(agent.call_args.args[3]["step"], 1)
        self.assertEqual(len(json.loads(self.store.get(self.task)["history"])), 2)
        self.assertEqual(len(self.transport.sent), 1)
        self.assertIsNone(self.store.work(self.task))

    def test_retries_are_bounded_and_do_not_restart_exhausted_task(self):
        self.session.handle(self.event())
        with patch.object(peer, "agent_reply", side_effect=RetryablePeerError("capacity")) as agent:
            for _ in range(5):
                self.make_due()
                self.session.tick()
        self.assertEqual(agent.call_count, 3)
        self.assertEqual(self.store.get(self.task)["status"], "retry_exhausted")
        self.assertEqual(self.transport.sent, [])
        self.reopen()
        self.assertEqual(self.store.meta("task_error:" + self.task)["reason"], "capacity")
        self.session.resume(self.task, "B")
        self.assertIn("capacity", peer.decode(self.transport.sent[-1])["body"])

    def test_cached_answer_send_retry_does_not_call_model_twice(self):
        self.transport.errors = [SendRejected("busy", retryable=True)]
        self.session.handle(self.event())
        with patch.object(peer, "agent_reply", return_value=self.answer) as agent:
            self.session.tick()
            self.reopen()
            self.make_due()
            self.session.tick()
        self.assertEqual(agent.call_count, 1)
        self.assertEqual(self.transport.sent[0], self.transport.sent[1])
        self.assertEqual(self.store.get(self.task)["step"], 2)

    def test_unknown_delivery_never_repeats_after_restart_and_can_reconcile(self):
        self.transport.errors = [DeliveryUnknown("timeout")]
        self.session.handle(self.event())
        with patch.object(peer, "agent_reply", return_value=self.answer):
            self.session.tick()
        wire = self.transport.sent[0]
        self.reopen()
        self.store.recover(self.config, time.time())
        self.make_due()
        with patch.object(peer, "agent_reply") as agent:
            self.session.tick()
            agent.assert_not_called()
        self.assertEqual(len(self.transport.sent), 1)
        self.assertEqual(self.store.get(self.task)["status"], "delivery_unknown")
        self.session.handle({"conversation_id": "group", "sender_open_dingtalk_id": "bot-A", "message_id": "reflected", "content": wire})
        self.assertEqual(self.store.get(self.task)["step"], 2)
        self.assertIsNone(self.store.work(self.task))

    def test_permanent_rejection_is_not_retried(self):
        self.transport.errors = [SendRejected("permission denied")]
        self.session.handle(self.event())
        with patch.object(peer, "agent_reply", return_value=self.answer):
            self.session.tick()
        self.assertEqual(self.store.get(self.task)["status"], "failed")
        self.assertIsNone(self.store.work(self.task))

    def test_waiting_peer_survives_overnight_and_accepts_fresh_reply(self):
        self.session.handle(self.event())
        with patch.object(peer, "agent_reply", return_value=self.answer):
            self.session.tick()
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET created=created-86400")
        self.session.tick()
        self.assertEqual(self.store.get(self.task)["status"], "waiting_peer")
        self.reopen()
        self.session.handle(self.event(step=3, kind="reply"))
        with patch.object(peer, "agent_reply", return_value={**self.answer, "action": "finish"}):
            self.session.tick()
        self.assertEqual(self.store.get(self.task)["status"], "done")

    def test_input_and_pending_work_are_durable_before_model_call(self):
        self.session.handle(self.event())
        self.reopen()
        self.assertIsNotNone(self.store.work(self.task))
        self.session.handle(self.event())
        with patch.object(peer, "agent_reply", return_value=self.answer) as agent:
            self.session.tick()
        self.assertEqual(agent.call_count, 1)

    def test_interrupted_model_is_rescheduled_but_interrupted_send_is_unknown(self):
        self.session.handle(self.event())
        self.store.begin(self.task, "agent_running")
        self.reopen()
        self.store.recover(self.config, time.time())
        self.assertEqual(self.store.work(self.task)["phase"], "agent")
        wire = peer.encode(self.task, "A", "B", 2, "reply", "文档核对", "答复")
        self.store.cache_wire(self.task, wire)
        self.store.begin(self.task, "sending")
        self.store.send_pending(self.task, 2)
        self.reopen()
        self.store.recover(self.config, time.time())
        self.assertEqual(self.store.work(self.task)["phase"], "unknown")
        self.assertIsNone(self.store.due(time.time() + 9999))

    def test_continue_creates_linked_task_preserving_completed_original(self):
        self.session.handle(self.event(kind="done", body="已核实协议，待查无效高度"))
        new_id = self.session.resume(self.task, "B")
        message = peer.decode(self.transport.sent[-1])
        self.assertNotEqual(new_id, self.task)
        self.assertEqual(self.store.get(self.task)["status"], "done")
        self.assertIn("待查无效高度", message["body"])
        self.assertEqual(self.store.db.execute("SELECT parent FROM continuations WHERE task=?", (new_id,)).fetchone()[0], self.task)

    def test_continue_keeps_large_context_under_message_limit(self):
        self.session.handle(self.event(kind="done", body="结论" * 1000))
        with self.store.db:
            self.store.db.execute("UPDATE tasks SET goal=? WHERE id=?", ("目标" * 900, self.task))
        self.session.resume(self.task, "B")
        self.assertLessEqual(len(self.transport.sent[-1]), 5000)
        self.assertIsNotNone(peer.decode(self.transport.sent[-1]))

    def test_stop_during_model_call_discards_answer(self):
        self.session.handle(self.event())
        def stopped(*args):
            self.store.close(self.task, "stopped")
            return self.answer
        with patch.object(peer, "agent_reply", side_effect=stopped):
            self.session.tick()
        self.assertEqual(self.transport.sent, [])
        self.assertEqual(self.store.get(self.task)["status"], "stopped")

    def test_recovered_own_terminal_message_prevents_old_request_resurrection(self):
        for kind in ("done", "stop"):
            task = "123456789ab" + ("c" if kind == "done" else "d")
            terminal = self.event(step=3, kind=kind, source="A", task=task)
            message = peer.decode(terminal["content"])
            terminal["content"] = peer.encode(task, "A", "ALL", 3, kind, message["goal"], message["body"])
            self.session.handle(terminal)
            self.session.handle(self.event(task=task))
            self.assertIn(self.store.get(task)["status"], ("done", "stopped"))
            self.assertIsNone(self.store.work(task))

    def test_reflection_race_cannot_duplicate_history_or_erase_newer_input(self):
        message = peer.decode(peer.encode(self.task, "A", "B", 1, "request", "文档核对", "开始"))
        self.store.queue_outgoing(message, peer.encode(self.task, "A", "B", 1, "request", "文档核对", "开始"))
        self.store.sent(message)
        self.store.sent(message)
        self.assertEqual(len(json.loads(self.store.get(self.task)["history"])), 1)
        self.session.handle(self.event(step=2, kind="reply"))
        self.store.sent(message)
        self.assertEqual(self.store.get(self.task)["step"], 2)
        self.assertIsNotNone(self.store.work(self.task))


class HistoryAndClassificationTests(unittest.TestCase):
    def transport(self):
        transport = peer.Transport.__new__(peer.Transport)
        transport.config = {"dws_path": "dws.exe", "profile": "test", "group_id": "group", "peers": [{"id": "A", "sender_id": "bot-A"}, {"id": "B", "sender_id": "bot-B"}]}
        return transport

    def item(self, task, kind, sender="bot-B"):
        return {"conversationId": "group", "senderId": sender, "messageId": kind,
                "text": peer.encode(task, "B", "A", 1, kind, "目标", "内容")}

    def test_history_terminal_messages_precede_old_requests_and_filter_forgery(self):
        task = "0123456789ab"
        data = {"complete": True, "messages": [self.item(task, "request"), self.item(task, "done"), self.item(task, "reply", "imposter"), {"conversationId": "group", "senderId": "human", "messageId": "old-start", "text": "AI联调 开始 旧任务"}]}
        with patch.object(peer.subprocess, "run", return_value=types.SimpleNamespace(returncode=0, stdout=json.dumps(data))):
            events = self.transport().history(1, 2)
        self.assertEqual([peer.decode(e["content"])["kind"] for e in events], ["done", "request"])
        with tempfile.TemporaryDirectory() as folder:
            store = Store(Path(folder) / "s.db")
            session = peer.PeerSession({"group_id": "group", "peers": self.transport().config["peers"]}, {"id": "A"}, store, FakeTransport(), Path(folder))
            for event in events:
                session.handle(event)
            self.assertEqual(store.get(task)["status"], "done")
            self.assertIsNone(store.work(task))
            store.db.close()

    def test_partial_or_drifted_history_is_never_dispatched(self):
        for data in ({"complete": False, "messages": []}, {"complete": True, "messages": [{}]}):
            with patch.object(peer.subprocess, "run", return_value=types.SimpleNamespace(returncode=0, stdout=json.dumps(data))):
                with self.assertRaises(PeerError):
                    self.transport().history(1, 2)

    def test_nontext_history_does_not_disable_recovery(self):
        data = {"complete": True, "messages": [{"conversationId": "group", "senderId": "human", "messageId": "image", "text": None}]}
        with patch.object(peer.subprocess, "run", return_value=types.SimpleNamespace(returncode=0, stdout=json.dumps(data))):
            self.assertEqual(self.transport().history(1, 2), [])

    def test_auth_errors_are_permanent_but_capacity_and_network_are_transient(self):
        for text in ("HTTP 429 capacity exhausted", "HTTP 503 unavailable", "connection reset", "request timed out"):
            self.assertTrue(transient_model_error(text))
        for text in ("HTTP 401 unauthorized", "403 permission denied", "invalid api key; capacity", "invalid output schema"):
            self.assertFalse(transient_model_error(text))

    def test_ctrl_c_and_bounded_listener_are_not_restarted(self):
        with patch.object(peer, "listen_once", return_value=130) as listen:
            self.assertEqual(peer.run_peer({}, {}, "0"), 130)
            self.assertEqual(listen.call_count, 1)
        with patch.object(peer, "listen_once", return_value=1) as listen:
            self.assertEqual(peer.run_peer({}, {}, "120s"), 1)
            self.assertEqual(listen.call_count, 1)

    def test_listener_reconnect_is_bounded(self):
        with patch.object(peer, "listen_once", return_value=1) as listen, patch.object(peer.time, "sleep"):
            self.assertEqual(peer.run_peer({"max_retries": 2}, {"id": "A"}, "0"), 1)
            self.assertEqual(listen.call_count, 3)


if __name__ == "__main__":
    unittest.main()


class SendReceiptDiagnosticsTests(unittest.TestCase):
    def transport(self):
        transport=peer.Transport.__new__(peer.Transport)
        transport.config={'profile':'profile','dws_path':'dws'}
        transport.token='synthetic-webhook-token'
        return transport

    def test_stderr_error_is_preserved_without_credentials_and_remains_unknown(self):
        transport=self.transport()
        payload={'error':{'code':4,'category':'api','reason':'connection_failed','message':'connect failed https://oapi.dingtalk.com/?access_token='+transport.token}}
        result=types.SimpleNamespace(returncode=4,stdout=b'',stderr=json.dumps(payload).encode())
        with patch.object(peer,'dws_send_capabilities',return_value={'--identity'}), \
             patch.object(peer.subprocess,'run',return_value=result),self.assertRaises(DeliveryUnknown):
            transport.send('AI联调 diagnostic')
        diagnostic=transport.last_send_diagnostic
        self.assertEqual(diagnostic['receipt_source'],'stderr')
        self.assertEqual(diagnostic['error_reason'],'connection_failed')
        self.assertNotIn(transport.token,json.dumps(diagnostic))
        self.assertNotIn('https://',json.dumps(diagnostic))

    def test_success_and_explicit_rejection_keep_delivery_semantics(self):
        transport=self.transport()
        success=types.SimpleNamespace(returncode=0,stdout=b'{"ok":true,"result":{"errcode":0}}',stderr=b'')
        rejected=types.SimpleNamespace(returncode=1,stdout=b'{"ok":false,"result":{"errcode":310000}}',stderr=b'')
        with patch.object(peer,'dws_send_capabilities',return_value={'--identity'}), \
             patch.object(peer.subprocess,'run',side_effect=[success,rejected]):
            transport.send('AI联调 diagnostic')
            with self.assertRaises(SendRejected): transport.send('AI联调 diagnostic')
        self.assertEqual(transport.last_send_diagnostic['webhook_errcode'],310000)

    def test_mcp_wrapped_rejection_ignores_success_flag_and_records_server_code(self):
        transport=self.transport()
        payload={'error':{'code':1,'category':'internal','message':'[MCP_TOOL_ERROR] '+json.dumps({'errcode':'300005','errmsg':'token is not exist','success':True})}}
        result=types.SimpleNamespace(returncode=1,stdout=b'',stderr=json.dumps(payload).encode())
        with patch.object(peer,'dws_send_capabilities',return_value={'--identity'}), \
             patch.object(peer.subprocess,'run',return_value=result),self.assertRaises(SendRejected):
            transport.send('AI联调 diagnostic')
        self.assertEqual(transport.last_send_diagnostic['webhook_errcode'],'300005')

    def test_unproven_mcp_errors_are_not_classified_as_rejections(self):
        for message in ('[MCP_TOOL_ERROR] invalid JSON','[MCP_TOOL_ERROR] {"errcode":0,"success":true}',
                        'unknown delivery 300005','[MCP_TOOL_ERROR] {"errcode":"0000"}'):
            with self.subTest(message=message):
                transport=self.transport()
                payload={'error':{'message':message}}
                result=types.SimpleNamespace(returncode=1,stdout=b'',stderr=json.dumps(payload).encode())
                with patch.object(peer,'dws_send_capabilities',return_value={'--identity'}), \
                     patch.object(peer.subprocess,'run',return_value=result),self.assertRaises(DeliveryUnknown):
                    transport.send('AI联调 diagnostic')
