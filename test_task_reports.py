import io
import json
import os
from pathlib import Path
import tempfile
import time
import types
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import peer
from reliability import DeliveryUnknown, SendRejected, Store, continuation_body


class TaskReportTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory(prefix="联调总结 测试 ")
        self.root = Path(self.folder.name)
        self.config = {"group_id": "group", "summary_send_interval_seconds": 0,
                       "retry_initial_delay_seconds": 1, "retry_max_delay_seconds": 10,
                       "peers": [{"id": "A", "sender_id": "bot-A"}, {"id": "B", "sender_id": "bot-B"}]}
        self.task = "0123456789ab"
        self.stores, self.transports, self.reporters = {}, {}, {}
        for name in ("A", "B"):
            runtime = self.root / name
            runtime.mkdir()
            store = Store(runtime / "state.sqlite3")
            transport = types.SimpleNamespace(token="private-fixture-webhook", sent=[])
            transport.send = lambda text, target=transport: target.sent.append(text)
            self.stores[name], self.transports[name] = store, transport
            self.reporters[name] = peer.TaskReporter(self.config, {"id": name}, store, transport, runtime)

    def tearDown(self):
        for store in self.stores.values():
            store.db.close()
        self.folder.cleanup()

    def task_row(self, name, status="active", body="按文档核对输入输出；异常输入的保护仍待验证。"):
        store = self.stores[name]
        source = "B" if name == "A" else "A"
        message = peer.decode(peer.encode(self.task, source, name, 1, "reply", "示例模块接口核对", body))
        self.assertTrue(store.accept("message-" + name, message))
        if status != "active":
            store.close(self.task, status)
        return store.get(self.task)

    def run_tick(self, name, **kwargs):
        with redirect_stdout(io.StringIO()) as output:
            self.reporters[name].tick(**kwargs)
        return output.getvalue()

    def event(self, name, text):
        return {"conversation_id": "group", "sender_open_dingtalk_id": "bot-" + name, "content": text}

    def reopen(self, name):
        self.stores[name].db.close()
        store = Store(self.root / name / "state.sqlite3")
        self.stores[name] = store
        self.reporters[name] = peer.TaskReporter(self.config, {"id": name}, store, self.transports[name], self.root / name)

    def test_both_ends_save_and_send_factual_completion_summaries_once(self):
        for name in ("A", "B"):
            self.task_row(name)
            parent = {"id": "abcdef012345", "status": "stopped", "history": json.dumps([{"from": "A", "text": "旧检查待继续"}])}
            self.stores[name].append(self.task, name, continuation_body(parent, 1000))
            source = "B" if name == "A" else "A"
            event = self.event(source, peer.encode(self.task, source, "ALL", 2, "done", "示例模块接口核对", "本轮核对收尾，仍需补充结果交付证据。"))
            session = peer.PeerSession(self.config, {"id": name}, self.stores[name], self.transports[name], self.root / name)
            session.reports = self.reporters[name]
            with redirect_stdout(io.StringIO()):
                session.handle(dict(event, message_id="completion-" + name))
            output = self.run_tick(name)
            self.run_tick(name)
            wires = self.transports[name].sent
            self.assertEqual(len(wires), 1)
            self.assertIn(name + "端｜任务 " + self.task, wires[0])
            self.assertIn("异常输入的保护仍待验证", wires[0])
            self.assertIn("从上次任务 abcdef012345 继续", wires[0])
            self.assertNotIn('[{"from":', wires[0])
            self.assertIn("不代表业务或设备验收通过", wires[0])
            self.assertIn("已发送到钉钉群", output)
            self.assertIsNone(peer.decode(wires[0]))
            files = list((self.root / name / "summaries").glob("*.txt"))
            self.assertEqual(files[0].read_text(encoding="utf-8"), wires[0])

    def test_stop_and_errors_have_distinct_outcomes_and_continue_entry(self):
        for status, expected in (("stopped", "收到停止指令"), ("failed", "遇到错误"), ("retry_exhausted", "重试已达上限")):
            with self.subTest(status=status):
                store = self.stores["A"]
                if store.get(self.task) is None:
                    self.task_row("A")
                store.close(self.task, status)
                self.run_tick("A")
                wire = self.transports["A"].sent[-1]
                self.assertIn(expected, wire)
                self.assertIn("AI联调 继续 " + self.task, wire)
                self.assertNotIn("本轮交流已结束", wire)

    def test_waiting_is_not_completion_and_does_not_repeat_after_restart(self):
        self.task_row("A", "waiting_peer")
        self.run_tick("A")
        self.reopen("A")
        self.run_tick("A")
        self.assertEqual(len(self.transports["A"].sent), 1)
        self.assertIn("并未判定联调完成", self.transports["A"].sent[0])

    def test_completion_supersedes_an_older_unsent_waiting_report(self):
        self.task_row("A", "waiting_peer")
        self.run_tick("A", send=False)
        self.stores["A"].close(self.task, "done")
        self.run_tick("A")
        self.assertEqual(len(self.transports["A"].sent), 1)
        self.assertIn("本轮交流已结束", self.transports["A"].sent[0])
        self.assertEqual(self.stores["A"].db.execute("SELECT status FROM task_reports WHERE reason='waiting_peer'").fetchone()[0], "superseded")

    def test_unknown_delivery_is_held_and_only_real_sender_can_reconcile(self):
        self.task_row("A", "done")
        def unknown(text):
            self.transports["A"].sent.append(text)
            raise DeliveryUnknown("fixture timeout")
        self.transports["A"].send = unknown
        self.run_tick("A")
        self.reopen("A")
        self.run_tick("A")
        wire = self.transports["A"].sent[0]
        report_id = peer.REPORT_HEADER.match(wire)[4]
        reporter = self.reporters["A"]
        reporter.observe(dict(self.event("A", wire), sender_open_dingtalk_id="forged"))
        reporter.observe(dict(self.event("A", wire), conversation_id="other-group"))
        self.assertEqual(self.stores["A"].db.execute("SELECT status FROM task_reports WHERE id=?", (report_id,)).fetchone()[0], "unknown")
        reporter.observe(self.event("A", wire))
        self.assertEqual(self.stores["A"].db.execute("SELECT status FROM task_reports WHERE id=?", (report_id,)).fetchone()[0], "sent")
        self.assertEqual(len(self.transports["A"].sent), 1)

    def test_interrupted_summary_send_is_not_replayed(self):
        self.task_row("A", "done")
        self.run_tick("A", send=False)
        with self.stores["A"].db:
            self.stores["A"].db.execute("UPDATE task_reports SET status='sending'")
        self.reopen("A")
        self.run_tick("A")
        self.assertEqual(self.transports["A"].sent, [])
        self.assertEqual(self.stores["A"].db.execute("SELECT status FROM task_reports").fetchone()[0], "unknown")

    def test_explicit_retryable_rejection_is_retried_only_once(self):
        self.task_row("A", "done")
        def rejected(text):
            self.transports["A"].sent.append(text)
            raise SendRejected("fixture rejection", retryable=True)
        self.transports["A"].send = rejected
        for _ in range(4):
            self.run_tick("A")
            with self.stores["A"].db:
                self.stores["A"].db.execute("UPDATE task_reports SET next_at=0")
        self.assertEqual(len(self.transports["A"].sent), 2)
        self.assertEqual(len(set(self.transports["A"].sent)), 1)
        self.assertEqual(self.stores["A"].db.execute("SELECT status FROM task_reports").fetchone()[0], "failed")

    def test_summary_is_bounded_and_redacts_credentials_before_saving(self):
        self.task_row("A", body="private-fixture-webhook AppSecret=fixture-secret https://oapi.dingtalk.com/robot/send?access_token=fixture-url " + "证据" * 900)
        for _ in range(11):
            self.stores["A"].append(self.task, "A", "未验证事项" * 500)
        self.stores["A"].close(self.task, "done")
        self.stores["A"].set_meta("task_error:" + self.task, {"reason": "AppSecret=fixture-secret"})
        self.run_tick("A")
        wire = self.transports["A"].sent[0]
        self.assertLessEqual(len(wire), 5000)
        self.assertNotIn("fixture-secret", wire)
        self.assertNotIn("private-fixture-webhook", wire)
        self.assertIn("节选", wire)

    def test_feature_upgrade_does_not_publish_old_completed_tasks(self):
        self.task_row("A", "done")
        self.stores["A"].set_meta("reports_initialized", False)
        self.reopen("A")
        self.run_tick("A")
        self.assertEqual(self.transports["A"].sent, [])

    def test_peer_interruption_generates_other_end_progress_without_echo_loop(self):
        self.task_row("A", "failed")
        self.task_row("B")
        self.run_tick("A")
        with redirect_stdout(io.StringIO()):
            self.reporters["B"].observe(self.event("A", self.transports["A"].sent[0]))
        self.run_tick("B")
        self.assertEqual(len(self.transports["B"].sent), 1)
        wire = self.transports["B"].sent[0]
        self.assertIn("状态：进度同步", wire)
        self.reporters["A"].observe(self.event("B", wire))
        self.run_tick("A")
        self.assertEqual(len(self.transports["A"].sent), 1)
        self.assertEqual(self.stores["B"].get(self.task)["status"], "active")

    def test_planned_restart_does_not_report_business_interruption(self):
        self.task_row("A")
        self.reporters["A"].start()
        self.reporters["A"].finish()
        self.reopen("A")
        self.reporters["A"].start()
        self.run_tick("A")
        self.assertEqual(self.transports["A"].sent, [])

    def test_planned_restart_request_must_match_owned_pid_peer_and_update(self):
        from connector_update import save_json
        path = self.root / ".local/update-reload-request.json"
        request = {"peer": "A", "pid": os.getpid(), "update": self.task, "time": time.time()}
        save_json(path, request)
        self.assertTrue(peer.planned_update_restart(self.root, "A", self.task))
        for field, value in (("peer", "B"), ("pid", os.getpid() + 1), ("update", "abcdef012345"), ("time", time.time() - 121)):
            save_json(path, dict(request, **{field: value}))
            self.assertFalse(peer.planned_update_restart(self.root, "A", self.task))

    def test_unexpected_exit_is_summarized_on_next_start(self):
        self.task_row("A")
        self.reporters["A"].start()
        self.reopen("A")
        with redirect_stdout(io.StringIO()):
            self.reporters["A"].start()
        self.run_tick("A")
        self.assertIn("上次运行意外中断", self.transports["A"].sent[0])
        self.assertIn("异常输入的保护仍待验证", self.transports["A"].sent[0])

    def test_failure_to_write_text_copy_preserves_durable_summary(self):
        self.task_row("A", "done")
        with patch("connector_update.write_atomic", side_effect=OSError("fixture disk error")):
            self.run_tick("A", send=False)
        self.assertEqual(self.stores["A"].db.execute("SELECT count(*) FROM task_reports").fetchone()[0], 1)
        self.run_tick("A")
        self.assertTrue(list((self.root / "A/summaries").glob("*.txt")))
        self.assertEqual(len(self.transports["A"].sent), 1)

    def test_history_includes_trusted_reports_without_replaying_forged_ones(self):
        self.task_row("A", "done")
        self.run_tick("A")
        wire = self.transports["A"].sent[0]
        transport = peer.Transport.__new__(peer.Transport)
        transport.config = dict(self.config, dws_path="dws.exe", profile="fixture")
        data = {"complete": True, "messages": [
            {"conversationId": "group", "senderId": "bot-A", "messageId": "real", "text": wire},
            {"conversationId": "group", "senderId": "forged", "messageId": "fake", "text": wire}]}
        with patch.object(peer.subprocess, "run", return_value=types.SimpleNamespace(returncode=0, stdout=json.dumps(data))):
            events = transport.history(1, 2)
        self.assertEqual([event["message_id"] for event in events], ["real"])


if __name__ == "__main__":
    unittest.main()
