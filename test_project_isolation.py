"""Changing business projects must not reuse another project's work or agent binding."""
import copy
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

import peer
import setup_helper
from reliability import PeerAlreadyRunning, PeerError, Store, acquire_instance_lock


class ProjectIsolationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="项目隔离 ")
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        self.config = {
            "project_id": "alpha", "group_id": "group", "max_steps": 6,
            "peers": [dict(id="A", sender_id="bot-A", owner_id="owner-A", workdir=str(self.root)),
                      dict(id="B", sender_id="bot-B", owner_id="owner-B", workdir=str(self.root))],
        }
        self.transport = types.SimpleNamespace(token="fixture", sent=[])
        self.transport.send = self.transport.sent.append

    def store(self, config, name="A"):
        runtime = peer.project_runtime(self.root, config, name)
        runtime.mkdir(parents=True, exist_ok=True)
        store = Store(runtime / "state.sqlite3")
        self.addCleanup(store.db.close)
        return store, runtime

    def event(self, wire, sender="bot-B", message_id="event-1"):
        return dict(conversation_id="group", sender_open_dingtalk_id=sender,
                    content=wire, message_id=message_id)

    def test_project_paths_validate_identifiers_and_keep_legacy_locations(self):
        self.assertEqual(peer.project_runtime(self.root, {}, "A"), self.root / "runtime/A")
        self.assertEqual(peer.project_local(self.root, {}), self.root / ".local")
        self.assertEqual(peer.project_runtime(self.root, self.config, "A"), self.root / "runtime/A/projects/alpha")
        for value in ("", "../other", "a/b", "A", 3, "x" * 65):
            with self.subTest(value=value), self.assertRaises(PeerError):
                peer.project_runtime(self.root, {"project_id": value}, "A")

    def test_same_group_messages_cannot_cross_projects_or_legacy_boundary(self):
        wire = peer.encode("0123456789ab", "B", "A", 1, "request", "检查接口", "开始核对", "alpha")
        for identity, accepted in (("alpha", True), ("beta", False), (None, False)):
            config = dict(self.config, project_id=identity)
            store, runtime = self.store(config)
            session = peer.PeerSession(config, config["peers"][0], store, self.transport, runtime)
            with patch.object(session, "run_work"):
                session.handle(self.event(wire))
            self.assertEqual(store.get("0123456789ab") is not None, accepted)
        legacy = peer.decode(peer.encode("111111111111", "B", "A", 1, "request", "旧项目", "历史消息"))
        self.assertFalse(peer.trusted(self.config, self.event(""), legacy))

    def test_resume_uses_only_selected_project_and_survives_store_reopen(self):
        store, runtime = self.store(self.config)
        session = peer.PeerSession(self.config, self.config["peers"][0], store, self.transport, runtime)
        with patch.object(session, "run_work"):
            session.submit("0123456789ab", "B", "继续检查", "已完成第一步")
        other = dict(self.config, project_id="beta")
        other_store, other_runtime = self.store(other)
        other_session = peer.PeerSession(other, other["peers"][0], other_store, self.transport, other_runtime)
        with self.assertRaisesRegex(PeerError, "No saved task"):
            other_session.resume("latest", "B")
        reopened = Store(runtime / "state.sqlite3")
        self.addCleanup(reopened.db.close)
        continued = peer.PeerSession(self.config, self.config["peers"][0], reopened, self.transport, runtime)
        with patch.object(continued, "run_work"):
            identity = continued.resume("latest", "B")
        self.assertEqual(reopened.get(identity)["goal"], "继续检查")
        self.assertEqual(peer.decode(reopened.work(identity)["wire"])["project_id"], "alpha")
        self.assertIn("0123456789ab", reopened.work(identity)["wire"])

    def test_business_binding_and_remote_frames_are_project_specific(self):
        store, _ = self.store(self.config, "B")
        bridge = peer.BusinessAgentBridge(self.config, self.config["peers"][1], store, self.transport, self.root)
        thread = "11111111-1111-4111-8111-111111111111"
        bridge.register_local(thread, "A")
        self.assertEqual(bridge.policy_path, self.root / ".local/projects/alpha/business-agent-B.json")
        other = dict(self.config, project_id="beta")
        other_store, _ = self.store(other, "B")
        other_bridge = peer.BusinessAgentBridge(other, other["peers"][1], other_store, self.transport, self.root)
        self.assertIsNone(other_bridge.policy())
        frame = bridge.outgoing("bound", "A", "0123456789ab", thread=thread, authorized_at=1)
        event = self.event(peer.business_frame(frame))
        self.assertIsNotNone(peer.trusted_business_frame(self.config, event))
        self.assertIsNone(peer.trusted_business_frame(other, event))
        self.assertIsNone(peer.trusted_business_frame(dict(self.config, project_id=None), event))
        changed = copy.deepcopy(self.config)
        changed["peers"][1]["workdir"] = str(self.root / "different-project")
        changed_bridge = peer.BusinessAgentBridge(changed, changed["peers"][1], store, self.transport, self.root)
        self.assertIsNone(changed_bridge.policy())

    def test_owner_commands_require_the_matching_project_tag(self):
        store, runtime = self.store(self.config)
        session = peer.PeerSession(self.config, self.config["peers"][0], store, self.transport, runtime)
        with patch.object(session, "submit") as submit:
            session.handle(self.event("AI联调 开始 检查接口", "owner-B", "old"))
            session.handle(self.event("AI联调 项目 beta 开始 检查接口", "owner-B", "wrong"))
            submit.assert_not_called()
            session.handle(self.event("AI联调 项目 alpha 开始 检查接口", "owner-B", "correct"))
            self.assertEqual(submit.call_count, 1)
            self.assertEqual(submit.call_args.args[2], "检查接口")

    def test_summary_contains_project_and_its_own_resume_path(self):
        store, runtime = self.store(self.config)
        session = peer.PeerSession(self.config, self.config["peers"][0], store, self.transport, runtime)
        with patch.object(session, "run_work"):
            session.submit("0123456789ab", "B", "检查接口", "已保存进度")
        reporter = peer.TaskReporter(self.config, self.config["peers"][0], store, self.transport, runtime)
        wire = reporter.compose(store.get("0123456789ab"), "done", "111111111111")
        self.assertIn("项目：alpha", wire)
        self.assertIn("AI联调 项目 alpha 继续 0123456789ab", wire)
        self.assertIn("runtime/A/projects/alpha/state.sqlite3", wire)
        event = self.event(wire, "bot-A")
        self.assertIsNotNone(peer.trusted_report(self.config, event))
        self.assertIsNone(peer.trusted_report(dict(self.config, project_id="beta"), event))
        business = reporter.business_record(dict(id="222222222222", target="B"), "ready", "阶段完成", "v1")
        self.assertIsNotNone(peer.trusted_report(self.config, self.event(business[3], "bot-A")))

    def test_changed_workdir_cannot_resume_an_existing_task_database(self):
        store, _ = self.store(self.config)
        peer.bind_store_context(store, self.config, self.config["peers"][0])
        with self.assertRaisesRegex(PeerError, "另一个项目"):
            peer.bind_store_context(store, self.config, dict(self.config["peers"][0], workdir=str(self.root / "other")))
        self.assertEqual(store.meta("project_context")["workdir"], str(self.root.resolve()))

    def test_named_projects_still_share_the_connector_instance_lock(self):
        lock_path = self.root / "runtime/A/run.lock"
        lock_path.parent.mkdir(parents=True)
        lock = acquire_instance_lock(lock_path, {"peer": "A"})
        try:
            with patch.object(peer, "ROOT", self.root), self.assertRaises(PeerAlreadyRunning):
                peer.listen_once(self.config, self.config["peers"][0], "0")
        finally:
            lock.close()


class ProjectConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="项目配置 ")
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        (self.root / ".local").mkdir()
        (self.root / "config.example.json").write_bytes((peer.ROOT / "config.example.json").read_bytes())
        self.tools = dict(python_path="existing-python", dws_path="existing-dws", codex_path="existing-codex")
        self.patches = [patch.object(setup_helper, "ROOT", self.root),
                        patch.object(setup_helper, "existing_connector", return_value=None),
                        patch.object(setup_helper, "prepare_credential", return_value=self.root / ".local/webhook-A.protected")]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)

    def test_new_projects_require_an_id_and_save_separate_configurations(self):
        with self.assertRaisesRegex(PeerError, "项目标识"):
            setup_helper.save_config(self.tools, "A", self.root)
        for identity in ("alpha", "beta"):
            path = setup_helper.save_config(self.tools, "A", self.root, project_id=identity)
            self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["project_id"], identity)
        for identity in ("alpha", "beta"):
            saved = json.loads((self.root / f".local/projects/{identity}/config.json").read_text(encoding="utf-8"))
            self.assertEqual(saved["project_id"], identity)
            self.assertEqual(saved["peers"][0]["workdir"], str(self.root.resolve()))

    def test_legacy_directory_change_requires_new_project_id(self):
        legacy = json.loads((self.root / "config.example.json").read_text(encoding="utf-8"))
        legacy["peers"][0]["workdir"] = str(self.root)
        active = self.root / ".local/config.json"
        active.write_text(json.dumps(legacy), encoding="utf-8")
        other = self.root / "other"
        other.mkdir()
        before = active.read_bytes()
        with self.assertRaisesRegex(PeerError, "项目标识"):
            setup_helper.save_config(self.tools, "A", other)
        self.assertEqual(active.read_bytes(), before)
        setup_helper.save_config(self.tools, "A", self.root)
        self.assertIsNone(json.loads(active.read_text(encoding="utf-8"))["project_id"])

    def test_running_connector_prevents_project_switch(self):
        with patch.object(setup_helper, "existing_connector", return_value={"pid": 123}), self.assertRaisesRegex(PeerError, "先正常停止"):
            setup_helper.save_config(self.tools, "A", self.root, project_id="alpha")
        self.assertFalse((self.root / ".local/config.json").exists())

    def test_checked_identity_is_preserved_in_project_configuration(self):
        path = setup_helper.save_config(self.tools, "A", self.root, project_id="alpha")
        resolved = peer.load_config(path)
        resolved["peers"][0]["sender_id"] = "profile-scoped-verified-sender"
        with patch.object(setup_helper, "run_json", return_value={}), patch.object(setup_helper, "resolve_group_identities", return_value=resolved):
            setup_helper.check_config(path)
        snapshot = self.root / ".local/projects/alpha/config.json"
        self.assertEqual(json.loads(snapshot.read_text(encoding="utf-8"))["peers"][0]["sender_id"], "profile-scoped-verified-sender")


if __name__ == "__main__":
    unittest.main()
