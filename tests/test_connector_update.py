import base64
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
import threading
import time
import types
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch
import zipfile

import connector_update as update
from reliability import DeliveryUnknown, PeerError, SendRejected
from supervisor import supervise


def bundle(files):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return output.getvalue()


class AtomicWriteTests(unittest.TestCase):
    @unittest.skipUnless(update.os.name == 'nt', 'Windows sharing semantics')
    def test_open_reader_does_not_drop_new_state_when_its_handle_closes_shortly(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'state.json';update.save_json(path,dict(value='old'))
            reader=path.open('rb');observed=[]
            def release():
                try:observed.append(json.loads(path.read_text())['value'])
                finally:reader.close()
            timer=threading.Timer(.06,release);timer.start()
            try:update.save_json(path,dict(value='new'))
            finally:timer.join();reader.close()
            self.assertEqual(observed,['old'])
            self.assertEqual(json.loads(path.read_text()),dict(value='new'))

    @unittest.skipUnless(update.os.name == 'nt', 'Windows sharing semantics')
    def test_permanent_reader_is_bounded_and_preserves_the_original(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'state.json';update.save_json(path,dict(value='old'))
            with path.open('rb'),patch.object(update.time,'sleep') as sleep:
                with self.assertRaises(PermissionError):update.save_json(path,dict(value='new'))
            self.assertEqual(sleep.call_count,6)
            self.assertLessEqual(sum(c.args[0] for c in sleep.call_args_list),.64)
            self.assertEqual(json.loads(path.read_text()),dict(value='old'))
            self.assertEqual(list(Path(folder).glob('.peer-update-*')),[])

    def test_unrelated_write_failure_is_not_retried(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'state.json';update.save_json(path,dict(value='old'))
            with patch.object(update.os,'replace',side_effect=PermissionError('not a sharing violation')) as replace, \
                    patch.object(update.time,'sleep') as sleep:
                with self.assertRaises(PermissionError):update.save_json(path,dict(value='new'))
            replace.assert_called_once();sleep.assert_not_called()
            self.assertEqual(json.loads(path.read_text()),dict(value='old'))


class UpdateTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory(prefix="AI connector update ")
        self.root = Path(self.folder.name)
        self.old = b'print("old")\n'
        (self.root / "peer.py").write_bytes(self.old)
        (self.root / ".local").mkdir()
        (self.root / ".local/config.json").write_text("private-local-configuration", encoding="utf-8")
        self.config = {"group_id": "group", "peers": [{"id": "A", "sender_id": "bot-A"}, {"id": "B", "sender_id": "bot-B"}]}
        update.enable(self.root, "A")
        self.receiver = update.UpdateReceiver(self.root, self.config, "B")
        self.new = b'print("new")\n'
        self.raw = bundle({"peer.py": self.new})

    def tearDown(self):
        self.folder.cleanup()

    def frames(self, raw=None, revision=100):
        raw = self.raw if raw is None else raw
        encoded = base64.b64encode(raw).decode("ascii")
        chunks = [encoded[i:i + update.CHUNK_CHARS] for i in range(0, len(encoded), update.CHUNK_CHARS)]
        common = {"id": "0123456789ab", "source": "A", "target": "ALL"}
        return [dict(common, kind="begin", revision=revision, sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw), chunks=len(chunks))] + [dict(common, kind="chunk", index=i, data=chunk) for i, chunk in enumerate(chunks)] + [dict(common, kind="commit")]

    def event(self, frame, sender="bot-A"):
        return {"conversation_id": "group", "sender_open_dingtalk_id": sender, "content": update.frame(frame)}

    def stage(self, raw=None):
        result = False
        for frame in self.frames(raw):
            result = self.receiver.receive(self.event(frame))
        self.assertTrue(result)

    def test_complete_package_is_staged_then_applied_preserving_private_config(self):
        self.stage()
        self.assertEqual((self.root / "peer.py").read_bytes(), self.old)
        manifest = update.apply_pending(self.root)
        self.assertEqual((self.root / "peer.py").read_bytes(), self.new)
        self.assertEqual((self.root / ".local/config.json").read_text(encoding="utf-8"), "private-local-configuration")
        update.mark_ready(self.root, manifest, "B")
        self.assertEqual(update.policy(self.root)["last_revision"], 100)
        self.assertFalse((self.root / ".local/update-pending.json").exists())

    def test_partial_publication_cannot_update_files(self):
        frames = self.frames()
        self.receiver.receive(self.event(frames[0]))
        self.assertFalse(self.receiver.receive(self.event(frames[-1])))
        self.assertIsNone(update.apply_pending(self.root))
        self.assertEqual((self.root / "peer.py").read_bytes(), self.old)

    def test_sender_identity_and_local_opt_in_are_required(self):
        frame = self.frames()[0]
        self.assertFalse(self.receiver.receive(self.event(frame, "human-A")))
        settings = update.policy(self.root)
        settings["enabled"] = False
        update.save_json(self.root / ".local/update-policy.json", settings)
        self.assertFalse(self.receiver.receive(self.event(frame)))
        self.assertFalse((self.root / ".local/updates").exists())

    def test_other_publisher_or_target_is_ignored(self):
        frame = self.frames()[0]
        self.assertFalse(self.receiver.receive(self.event(dict(frame, source="B"), "bot-B")))
        self.assertFalse(self.receiver.receive(self.event(dict(frame, target="A"))))

    def test_digest_failure_and_conflicting_chunks_never_install(self):
        frames = self.frames()
        self.receiver.receive(self.event(dict(frames[0], sha256="0" * 64)))
        self.receiver.receive(self.event(frames[1]))
        with self.assertRaises(PeerError):
            self.receiver.receive(self.event(dict(frames[1], data=base64.b64encode(b"changed").decode("ascii"))))
        with self.assertRaises(PeerError):
            self.receiver.receive(self.event(frames[-1]))
        self.assertEqual((self.root / "peer.py").read_bytes(), self.old)

    def test_repeated_identical_chunk_is_harmless(self):
        frames = self.frames()
        self.receiver.receive(self.event(frames[0]))
        self.receiver.receive(self.event(frames[1]))
        self.receiver.receive(self.event(frames[1]))
        self.assertTrue(self.receiver.receive(self.event(frames[-1])))

    def test_commit_before_last_chunk_applies_only_after_missing_chunk_arrives(self):
        frames = self.frames()
        self.receiver.receive(self.event(frames[0]))
        self.assertFalse(self.receiver.receive(self.event(frames[-1])))
        self.assertTrue(self.receiver.receive(self.event(frames[1])))

    def test_archive_cannot_write_private_files_traverse_or_replace_guide(self):
        for name in (".local/config.json", "../outside.py", "AGENTS.md", "runtime/state.sqlite3", "business.exe"):
            with self.assertRaises(PeerError):
                update.inspect_bundle(bundle({name: b"payload"}))

    def test_invalid_python_cmd_encoding_or_powershell_bom_is_rejected(self):
        for files in ({"peer.py": b"def broken("}, {"start.cmd": "中文".encode("utf-8")}, {"install.ps1": b"Write-Host hi"}):
            with self.assertRaises(PeerError):
                update.inspect_bundle(bundle(files))

    def test_symbolic_link_and_duplicate_entries_are_rejected(self):
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as archive:
            info = zipfile.ZipInfo("peer.py")
            info.external_attr = 0o120777 << 16
            archive.writestr(info, "outside")
        with self.assertRaises(PeerError):
            update.inspect_bundle(output.getvalue())

    def test_interrupted_install_rolls_back_on_next_start_and_rejects_replay(self):
        self.stage(bundle({"peer.py": self.new, "resume.cmd": b"@echo off\r\n"}))
        update.apply_pending(self.root)
        self.assertTrue((self.root / "resume.cmd").exists())
        update.recover_install(self.root)
        self.assertEqual((self.root / "peer.py").read_bytes(), self.old)
        self.assertFalse((self.root / "resume.cmd").exists())
        for frame in self.frames():
            self.assertFalse(self.receiver.receive(self.event(frame)))
        self.assertIsNone(update.apply_pending(self.root))

    def test_mid_install_failure_restores_already_replaced_files(self):
        self.stage(bundle({"peer.py": self.new, "resume.cmd": b"@echo off\r\n"}))
        original = update.write_atomic
        failed = [False]
        def write(path, data):
            if path == self.root / "resume.cmd" and not failed[0]:
                failed[0] = True
                raise OSError("simulated disk error")
            return original(path, data)
        with patch.object(update, "write_atomic", side_effect=write):
            with self.assertRaises(OSError):
                update.apply_pending(self.root)
        self.assertEqual((self.root / "peer.py").read_bytes(), self.old)
        self.assertFalse((self.root / ".local/update-installing.json").exists())

    def test_ready_version_is_not_rolled_back_after_journal_cleanup_interruption(self):
        self.stage()
        manifest = update.apply_pending(self.root)
        settings = update.policy(self.root)
        settings["last_revision"] = manifest["revision"]
        update.save_json(self.root / ".local/update-policy.json", settings)
        update.recover_install(self.root)
        self.assertEqual((self.root / "peer.py").read_bytes(), self.new)

    def test_supervisor_rolls_back_crashing_update_and_runs_original(self):
        self.old = b'import json\nprint(json.dumps({"event":"ready","peer":"B"}),flush=True)\n'
        (self.root / "peer.py").write_bytes(self.old)
        self.stage(bundle({"peer.py": b'raise RuntimeError("simulated startup failure")\n'}))
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(supervise(self.root / "unused.json", "B", self.root), 0)
        self.assertEqual((self.root / "peer.py").read_bytes(), self.old)
        self.assertTrue((self.root / ".local/update-failed.json").exists())
        self.assertIn("B端开始安装更新", output.getvalue())
        self.assertIn("B端更新失败，已恢复上一版本", output.getvalue())
        self.assertNotIn("B端更新完成", output.getvalue())

    def test_supervisor_confirms_real_child_ready_and_keeps_new_version(self):
        self.new = b'import json\nprint(json.dumps({"event":"ready","peer":"B"}),flush=True)\n'
        self.stage(bundle({"peer.py": self.new}))
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(supervise(self.root / "unused.json", "B", self.root), 0)
        self.assertEqual(update.policy(self.root)["last_revision"], 100)
        self.assertTrue((self.root / ".local/update-receipt.json").exists())
        text = output.getvalue()
        self.assertLess(text.index("B端开始安装更新"), text.index('"event": "ready"'))
        self.assertLess(text.index('"event": "ready"'), text.index("B端更新完成"))

    def test_publisher_rollback_uses_running_snapshot_not_already_edited_files(self):
        self.old = b'import json\nprint(json.dumps({"event":"ready","peer":"B"}),flush=True)\n'
        (self.root / "peer.py").write_bytes(self.old)
        update.capture_baseline(self.root)
        broken = b'raise RuntimeError("edited but not yet running")\n'
        (self.root / "peer.py").write_bytes(broken)
        self.stage(bundle({"peer.py": broken}))
        self.assertEqual(supervise(self.root / "unused.json", "B", self.root), 0)
        self.assertEqual((self.root / "peer.py").read_bytes(), self.old)

    def test_unresponsive_owned_process_is_never_force_killed(self):
        child = types.SimpleNamespace(poll=lambda: None, send_signal=lambda value: None,
                                      wait=lambda **kwargs: (_ for _ in ()).throw(subprocess.TimeoutExpired("child", 20)))
        with self.assertRaises(PeerError):
            update.stop_process(child)

    def test_healthy_monitor_update_requests_reload_of_supervisor(self):
        self.new = b'import json\nprint(json.dumps({"event":"ready","peer":"B"}),flush=True)\n'
        self.stage(bundle({"peer.py": self.new, "supervisor.py": b'# updated monitor source\n'}))
        self.assertEqual(supervise(self.root / "unused.json", "B", self.root), 76)
        self.assertEqual(update.policy(self.root)["last_revision"], 100)
        self.assertFalse((self.root / ".local/update-installing.json").exists())


class StartupUpdateTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory(prefix="AI startup update ")
        self.roots, self.clients, self.transports, self.logs = {}, {}, {}, {}
        self.config = {"group_id": "group", "update_frame_interval_seconds": 0, "max_retries": 1,
                       "retry_initial_delay_seconds": 0,
                       "peers": [{"id": "A", "sender_id": "bot-A"}, {"id": "B", "sender_id": "bot-B"}]}
        for peer_id in ("A", "B"):
            root = Path(self.folder.name) / peer_id
            root.mkdir()
            (root / "peer.py").write_bytes(b'print("old")\n')
            update.enable(root, "A")
            self.roots[peer_id] = root
            self.logs[peer_id] = []
            self.transports[peer_id] = types.SimpleNamespace(sent=[])
            self.transports[peer_id].send = lambda wire, p=peer_id: self.transports[p].sent.append(update.parse_frame(wire))
            self.create_client(peer_id)

    def create_client(self, peer_id):
        client = update.StartupUpdater(self.roots[peer_id], self.config, peer_id, self.transports[peer_id],
                                       lambda event, p=peer_id, **fields: self.logs[p].append(event))
        self.clients[peer_id] = client
        return client

    def tearDown(self):
        for client in self.clients.values():
            client.close()
        for client in self.clients.values():
            if client.worker:
                client.worker.join(timeout=2)
        self.folder.cleanup()

    def drain(self, peer_id):
        client = self.clients[peer_id]
        deadline = time.monotonic() + 3
        while client.jobs.unfinished_tasks and time.monotonic() < deadline:
            time.sleep(0.005)
        self.assertEqual(client.jobs.unfinished_tasks, 0, "startup worker did not finish")
        client.tick(False)

    def event(self, message, sender=None, group="group"):
        return {"conversation_id": group, "sender_open_dingtalk_id": sender or "bot-" + message["source"],
                "content": update.frame(message)}

    def publish(self, target="ALL"):
        (self.roots["A"] / "peer.py").write_bytes(b'print("published")\n')
        manifest = update.publish(self.roots["A"], self.config, {"id": "A"}, self.transports["A"], target=target)
        self.transports["A"].sent.clear()
        return manifest

    def check(self):
        self.clients["A"].tick(False)
        self.clients["B"].tick()
        self.drain("B")
        request = self.transports["B"].sent[-1]
        self.clients["A"].handle(self.event(request))
        self.drain("A")
        return request

    def test_startup_check_downloads_published_release_then_restart_skips_identical_version(self):
        manifest = self.publish()
        self.check()
        receiver = update.UpdateReceiver(self.roots["B"], self.config, "B")
        staged = False
        for message in self.transports["A"].sent:
            self.clients["B"].handle(self.event(message))
            staged = receiver.receive(self.event(message)) or staged
        self.assertTrue(staged)
        installed = update.apply_pending(self.roots["B"])
        update.mark_ready(self.roots["B"], installed, "B")
        self.assertEqual((self.roots["B"] / "peer.py").read_bytes(), b'print("published")\n')
        self.clients["B"].close()
        self.clients["B"].worker.join(timeout=2)
        self.create_client("B")
        self.transports["A"].sent.clear()
        self.check()
        self.assertEqual([m["kind"] for m in self.transports["A"].sent], ["latest"])
        self.clients["B"].handle(self.event(self.transports["A"].sent[0]))
        self.assertIn("update_already_current", self.logs["B"])
        self.assertEqual(update.policy(self.roots["B"])["last_revision"], manifest["revision"])

    def test_edited_source_is_not_automatically_published(self):
        manifest = self.publish()
        (self.roots["A"] / "peer.py").write_bytes(b'print("unpublished edits")\n')
        self.check()
        messages = self.transports["A"].sent
        self.assertEqual(messages[1]["sha256"], manifest["sha256"])
        publisher_receiver = update.UpdateReceiver(self.roots["A"], self.config, "A")
        for message in messages:
            self.assertEqual(message["target"], "B")
            self.assertFalse(publisher_receiver.receive(self.event(message)))
        self.assertFalse((self.roots["A"] / ".local/update-pending.json").exists())
        release = update.published_release(self.roots["A"], self.config, "A")
        self.assertEqual(update.inspect_bundle(release[1])["peer.py"], b'print("published")\n')

    def test_disabled_updates_and_incomplete_history_do_not_send_checks(self):
        settings = update.policy(self.roots["B"])
        settings["enabled"] = False
        update.save_json(self.roots["B"] / ".local/update-policy.json", settings)
        self.clients["B"].tick()
        self.assertIsNone(self.clients["B"].worker)
        update.enable(self.roots["B"], "A")
        self.clients["B"].tick(False)
        self.assertEqual(self.transports["B"].sent, [])
        self.clients["B"].tick()
        self.drain("B")
        self.assertEqual(len(self.transports["B"].sent), 1)

    def test_no_published_release_returns_status_without_install_or_source_publication(self):
        self.check()
        messages = self.transports["A"].sent
        self.assertEqual(len(messages), 1)
        self.assertIs(messages[0]["available"], False)
        self.clients["B"].handle(self.event(messages[0]))
        self.assertIn("update_no_published_release", self.logs["B"])
        self.assertFalse((self.roots["B"] / ".local/update-pending.json").exists())

    def test_forged_duplicate_and_historical_checks_never_trigger_transfers(self):
        self.publish()
        self.clients["A"].tick(False)
        request = {"id": "0123456789ab", "source": "B", "target": "A", "kind": "check",
                   "content_sha256": "0" * 64, "minimum_revision": 0}
        client = self.clients["A"]
        client.handle(self.event(request, sender="human-B"))
        client.handle(self.event(request, group="wrong-group"))
        client.handle(self.event(request), recovered=True)
        self.assertEqual(self.transports["A"].sent, [])
        client.handle(self.event(request))
        client.handle(self.event(request))
        self.drain("A")
        self.assertEqual(sum(m["kind"] == "begin" for m in self.transports["A"].sent), 1)

    def test_rejected_release_is_not_transferred_again(self):
        manifest = self.publish()
        settings = update.policy(self.roots["B"])
        settings["rejected_revision"] = manifest["revision"]
        update.save_json(self.roots["B"] / ".local/update-policy.json", settings)
        self.check()
        self.assertEqual([m["kind"] for m in self.transports["A"].sent], ["latest"])
        self.clients["B"].handle(self.event(self.transports["A"].sent[0]))
        self.assertIn("update_revision_withheld", self.logs["B"])

    def test_offline_publisher_times_out_without_blocking_existing_program(self):
        self.clients["B"].tick()
        self.drain("B")
        with patch.object(update.time, "time", return_value=self.clients["B"].wait_until + 1):
            self.clients["B"].tick()
            self.clients["B"].tick()
        self.assertEqual(self.logs["B"].count("update_check_unavailable"), 1)
        self.assertEqual(len(self.transports["B"].sent), 1)
        self.assertEqual((self.roots["B"] / "peer.py").read_bytes(), b'print("old")\n')

    def test_later_publisher_announcement_triggers_check_once(self):
        self.publish()
        self.clients["A"].tick()
        self.clients["B"].tick()
        self.drain("A")
        self.drain("B")
        announcement = self.transports["A"].sent[0]
        self.assertEqual(announcement["kind"], "latest")
        for _ in range(2):
            self.clients["B"].handle(self.event(announcement))
        self.drain("B")
        self.assertEqual(len(self.transports["B"].sent), 2)

    def test_unknown_transfer_is_not_replayed_after_publisher_restart(self):
        self.publish()
        transport = self.transports["A"]
        def unknown(wire):
            message = update.parse_frame(wire)
            transport.sent.append(message)
            if message["kind"] == "chunk":
                raise DeliveryUnknown("timeout")
        transport.send = unknown
        request = self.check()
        self.assertEqual(sum(m["kind"] == "chunk" for m in transport.sent), 1)
        self.clients["A"].close()
        self.clients["A"].worker.join(timeout=2)
        self.create_client("A").tick(False)
        self.clients["A"].handle(self.event(dict(request, id="1123456789ab")))
        self.drain("A")
        self.assertEqual(sum(m["kind"] == "chunk" for m in transport.sent), 1)
        self.assertIn("update_transfer_withheld", self.logs["A"])

    def test_explicit_temporary_send_rejections_retry_boundedly(self):
        transport = self.transports["B"]
        def busy(wire):
            transport.sent.append(update.parse_frame(wire))
            raise SendRejected("busy", retryable=True)
        transport.send = busy
        self.clients["B"].tick()
        self.drain("B")
        self.assertEqual(len(transport.sent), 2)
        self.assertIn("update_check_failed", self.logs["B"])

    def test_content_version_ignores_zip_metadata(self):
        first = update.build_bundle(self.roots["A"])
        second = update.build_bundle(self.roots["B"])
        self.assertEqual(first, second)
        different_zip = bundle({"peer.py": b'print("old")\n'})
        self.assertEqual(update.content_digest(first), update.content_digest(different_zip))

    def test_incomplete_publication_never_becomes_startup_release(self):
        self.transports["A"].send = lambda wire: (_ for _ in ()).throw(DeliveryUnknown("timeout"))
        with self.assertRaises(DeliveryUnknown):
            update.publish(self.roots["A"], self.config, {"id": "A"}, self.transports["A"])
        self.assertIsNone(update.published_release(self.roots["A"], self.config, "A"))

    def test_changed_cached_package_is_not_sent(self):
        manifest = self.publish()
        cached = self.roots["A"] / ".local/published-updates" / manifest["id"] / "package.zip"
        cached.write_bytes(b"corrupt")
        self.check()
        self.assertEqual(self.transports["A"].sent, [])
        self.assertIn("update_check_failed", self.logs["A"])

    def test_release_not_targeted_at_requester_is_unavailable(self):
        self.publish(target="A")
        self.check()
        messages = self.transports["A"].sent
        self.assertEqual(len(messages), 1)
        self.assertIs(messages[0]["available"], False)

    def test_disabled_policy_stops_a_queued_send(self):
        client = self.clients["B"]
        client.tick(False)
        settings = update.policy(self.roots["B"])
        settings["enabled"] = False
        update.save_json(self.roots["B"] / ".local/update-policy.json", settings)
        with self.assertRaises(PeerError):
            client._send([{"id": "0123456789ab", "kind": "check"}])
        self.assertEqual(self.transports["B"].sent, [])

    def test_malformed_release_pointer_is_rejected_without_stopping_worker(self):
        self.publish()
        pointer = self.roots["A"] / ".local/update-release.json"
        release = json.loads(pointer.read_text(encoding="utf-8"))
        release["manifest"]["target"] = []
        update.save_json(pointer, release)
        self.check()
        self.assertEqual(self.transports["A"].sent, [])
        self.assertIn("update_check_failed", self.logs["A"])
        self.assertTrue(self.clients["A"].worker.is_alive())


if __name__ == "__main__":
    unittest.main()
