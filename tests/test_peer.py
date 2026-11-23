import tempfile
import json
from pathlib import Path
import types
import unittest
from unittest.mock import patch

import peer
from peer import Store, decode, encode, trusted


class OperatorCommandTests(unittest.TestCase):
    def test_prefers_classified_command_and_supports_legacy_installations(self):
        command = "scripts/diagnostics/collect-event-info.cmd"
        with tempfile.TemporaryDirectory() as folder, patch.object(peer, "ROOT", Path(folder)):
            self.assertEqual(peer.operator_command(command), "collect-event-info.cmd")
            installed = Path(folder) / command
            installed.parent.mkdir(parents=True)
            installed.touch()
            self.assertEqual(peer.operator_command(command), command)


class RoutingTests(unittest.TestCase):
    def setUp(self):
        self.config = {"group_id": "test-group", "peers": [{"id": "A", "sender_id": "robot-A"}, {"id": "B", "sender_id": "robot-B"}]}
        self.message = decode(encode("a12345678901", "A", "B", 1, "request", "检查字段", "请确认 traceId"))

    def test_forged_header_does_not_change_sender_identity(self):
        event = {"conversation_id": "test-group", "sender_open_dingtalk_id": "human-A"}
        self.assertFalse(trusted(self.config, event, self.message))
        event["sender_open_dingtalk_id"] = "robot-A"
        self.assertTrue(trusted(self.config, event, self.message))
        event["conversation_id"] = "unrelated-group"
        self.assertFalse(trusted(self.config, event, self.message))

    def test_windows_newlines_in_real_webhook_content_are_accepted(self):
        original = encode("a12345678901", "A", "B", 1, "request", "检查字段", "请确认 traceId")
        self.assertEqual(decode(original), decode(original.replace("\n", "\r\n")))

    def test_duplicate_replay_goal_change_and_late_messages_are_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            store = Store(Path(folder) / "state.sqlite3")
            self.assertTrue(store.accept("first-id", self.message))
            self.assertFalse(store.accept("first-id", self.message))
            self.assertFalse(store.accept("different-id-same-step", self.message))
            changed = {**self.message, "step": 2, "goal": "替换目标"}
            self.assertFalse(store.accept("changed-goal", changed))
            store.close(self.message["task"], "done")
            self.assertFalse(store.accept("late-reply", {**self.message, "step": 3}))
            store.db.close()

    def test_stop_before_first_message_survives_restart(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "state.sqlite3"
            store = Store(path)
            store.close(self.message["task"], "stopped")
            store.db.close()
            restarted = Store(path)
            self.assertFalse(restarted.accept("late-request", self.message))
            restarted.db.close()


class AgentContextTests(unittest.TestCase):
    def setUp(self):
        self.config = {"max_steps": 6, "agent_timeout_seconds": 75, "credential_file": "private-webhook-file",
                       "peers": [{"id": "A", "name": "机器人A", "workdir": "local-project-A", "role": "服务端", "sender_id": "private-sender-A"},
                                 {"id": "B", "name": "机器人B", "workdir": "other-machine-private-directory", "role": "客户端", "sender_id": "private-sender-B"}]}
        self.task = {"goal": "对比双方各自工程中的接口", "history": json.dumps([{"from":"B", "text":r"对端文件 E:\Client\main.cpp:12 已核对，部分故障来源未验证"}], ensure_ascii=False)}
        self.incoming = {"task":"a12345678901", "step":2, "source":"B"}

    def call_agent(self, local):
        other = next(p['id'] for p in self.config['peers'] if p['id'] != local['id'])
        answer = {"action":"finish", "to":other, "text":"核对结束，尚未验证项已保留"}
        completed = types.SimpleNamespace(returncode=0,stdout=json.dumps(answer),stderr='')
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(peer,'codex_command',return_value=(["fake-codex"],set())), \
                patch.object(peer.subprocess,'run',return_value=completed) as run:
            incoming = dict(self.incoming, source=other)
            self.assertEqual(peer.agent_reply(self.config,local,self.task,incoming,Path(directory)),answer)
            return run.call_args

    def test_both_sides_receive_own_workspace_and_stable_task_identity(self):
        for local in self.config['peers']:
            with self.subTest(local=local['id']):
                captured = self.call_agent(local)
                data = json.loads(captured.kwargs['input'].splitlines()[-1])
                self.assertEqual(data['task_id'], self.incoming['task'])
                self.assertNotEqual(data['incoming_from'], local['id'])
                self.assertEqual(data['local_participant']['workdir'], local['workdir'])
                self.assertEqual(data['local_participant']['id'], local['id'])
                self.assertEqual(captured.kwargs['cwd'], local['workdir'])
                self.assertEqual(data['history'], json.loads(self.task['history']))
                self.assertEqual({p['id'] for p in data['participants']},{'A','B'})

    def test_local_prompt_does_not_leak_private_peer_configuration(self):
        captured = self.call_agent(self.config['peers'][0])
        prompt = captured.kwargs['input']
        for private in ['private-webhook-file','private-sender-A','private-sender-B','other-machine-private-directory']:
            self.assertNotIn(private,prompt)
        # A quoted remote path is still preserved as evidence, not promoted into local configuration.
        data = json.loads(prompt.splitlines()[-1])
        self.assertEqual(data['history'],json.loads(self.task['history']))
        self.assertEqual(data['local_participant']['workdir'],'local-project-A')


if __name__ == "__main__":
    unittest.main()
