import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import types
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import connector_update as update
import supervisor
from reliability import DeliveryUnknown, PeerError


class OwnedProcess:
    def __init__(self):
        self.returncode = None
    def poll(self):
        return self.returncode
    def wait(self, **kwargs):
        return self.returncode
    def send_signal(self, value):
        self.returncode = 130


class AutoPublicationTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory(prefix="AI 自动发布 ")
        self.root = Path(self.folder.name)
        for name in update.ALLOWED:
            if name.endswith(".py"):
                raw = b"# connector fixture\n"
            elif name.endswith(".cmd"):
                raw = b"@echo off\r\n"
            elif name.endswith(".ps1"):
                raw = b"\xef\xbb\xbf# connector fixture\r\n"
            else:
                raw = b'{"version":"fixture"}'
            (self.root / name).write_bytes(raw)
        self.test_file = self.root / "test_fixture.py"
        self.test_file.write_text('import unittest, json\nfrom pathlib import Path\nclass Check(unittest.TestCase):\n def test_schema(self):\n  self.assertEqual(json.loads(Path("response.schema.json").read_text())["version"], "fixture")\n', encoding="utf-8")
        self.config = {"group_id": "group", "peers": [{"id": "A", "sender_id": "bot-A"}, {"id": "B", "sender_id": "bot-B"}],
                       "max_retries": 1, "update_frame_interval_seconds": 0}
        update.enable(self.root, "A")
        self.config_path = self.root / ".local/config.json"
        self.config_path.write_text(json.dumps(self.config), encoding="utf-8")
        self.transport = types.SimpleNamespace(sent=[], token="nonproduction-private-webhook-value")
        self.transport.send = lambda text: self.transport.sent.append(update.parse_frame(text))
        self.logs, self.drivers = [], []

    def tearDown(self):
        for driver in self.drivers:
            driver.close()
        self.folder.cleanup()

    def driver(self, peer="A", quiet=30, minimum=0):
        driver = update.AutoPublisher(self.root, self.config_path, peer, lambda event, **fields: self.logs.append(event),
                                      quiet_seconds=quiet, poll_seconds=0, min_interval=minimum)
        self.drivers.append(driver)
        return driver

    def automatic(self):
        expected = update.publication_candidate(self.root)["candidate"]
        with redirect_stdout(io.StringIO()):
            return update.publish(self.root, self.config, {"id": "A"}, self.transport, automatic=True, expected_candidate=expected)

    def manual(self):
        with redirect_stdout(io.StringIO()):
            return update.publish(self.root, self.config, {"id": "A"}, self.transport)

    def state(self):
        return json.loads((self.root / ".local/auto-publication.json").read_text(encoding="utf-8"))

    def event(self, message):
        return {"conversation_id": "group", "sender_open_dingtalk_id": "bot-A", "content": update.frame(message)}

    def test_success_runs_real_tests_then_publishes_validated_snapshot(self):
        manifest = self.automatic()
        self.assertEqual(self.state()["status"], "published")
        self.assertEqual(self.state()["tests"], 1)
        self.assertEqual(manifest["auto_candidate"], update.publication_candidate(self.root)["candidate"])
        self.assertEqual(self.transport.sent[-1]["kind"], "commit")
        self.assertEqual(update.published_release(self.root, self.config, "A")[0]["id"], manifest["id"])

    def test_failed_real_tests_do_not_send_or_loop_on_restart(self):
        self.test_file.write_text('import unittest\nclass Check(unittest.TestCase):\n def test_fail(self): self.fail("fixture failure")\n', encoding="utf-8")
        with self.assertRaises(PeerError):
            self.automatic()
        self.assertEqual(self.state()["status"], "validation_failed")
        driver = self.driver(quiet=0)
        with patch.object(update.subprocess, "Popen") as launch:
            driver.tick(now=0)
            driver.tick(now=100)
            launch.assert_not_called()
        self.assertEqual(self.transport.sent, [])

    def test_test_fix_allows_fresh_validation_without_reediting_runtime(self):
        state = dict(update.publication_candidate(self.root), status="validation_failed")
        update.save_json(self.root / ".local/auto-publication.json", state)
        self.test_file.write_text(self.test_file.read_text(encoding="utf-8") + "# corrected test\n", encoding="utf-8")
        driver = self.driver(quiet=0)
        with patch.object(update.subprocess, "Popen", return_value=OwnedProcess()) as launch:
            driver.tick(now=0)
            driver.tick(now=1)
            self.assertEqual(launch.call_count, 1)

    def test_missing_tests_and_invalid_script_do_not_send(self):
        self.test_file.unlink()
        with self.assertRaises(PeerError):
            self.automatic()
        self.assertEqual(self.transport.sent, [])

    @unittest.skipUnless(os.name == "nt", "PowerShell check requires Windows")
    def test_powershell_syntax_failure_blocks_publication(self):
        (self.root / "install.ps1").write_bytes(b"\xef\xbb\xbfif (")
        with self.assertRaises(PeerError):
            self.automatic()
        self.assertEqual(self.transport.sent, [])

    def test_source_change_during_validation_sends_nothing(self):
        original = update.subprocess.run
        def edit_after_tests(command, **kwargs):
            result = original(command, **kwargs)
            if "unittest" in command:
                (self.root / "peer.py").write_bytes(b"# changed while tests ran\n")
            return result
        with patch.object(update.subprocess, "run", side_effect=edit_after_tests):
            with self.assertRaises(update.PublicationChanged):
                self.automatic()
        self.assertEqual(self.state()["status"], "source_changed")
        self.assertEqual(self.transport.sent, [])

    def test_credentials_in_source_are_never_published(self):
        (self.root / "peer.py").write_text("# " + self.transport.token, encoding="utf-8")
        with self.assertRaisesRegex(PeerError, "Credential detected"):
            self.automatic()
        self.assertEqual(self.transport.sent, [])

    def test_manual_and_automatic_publications_share_one_lock(self):
        with update.PublicationLock(self.root):
            with self.assertRaises(update.PublicationBusy):
                self.manual()
            with self.assertRaises(update.PublicationBusy):
                self.automatic()
        self.assertEqual(self.transport.sent, [])

    def test_unknown_delivery_is_held_across_controller_restart(self):
        def unknown(text):
            message = update.parse_frame(text)
            self.transport.sent.append(message)
            if message["kind"] == "chunk":
                raise DeliveryUnknown("fixture timeout")
        self.transport.send = unknown
        with self.assertRaises(DeliveryUnknown):
            self.automatic()
        self.assertEqual(self.state()["status"], "delivery_unknown")
        driver = self.driver(quiet=0)
        with patch.object(update.subprocess, "Popen") as launch:
            driver.tick(now=0)
            driver.tick(now=100)
            launch.assert_not_called()
        self.assertEqual(sum(m["kind"] == "chunk" for m in self.transport.sent), 1)

    def test_edit_during_upload_is_not_overwritten_by_own_update(self):
        old = (self.root / "peer.py").read_bytes()
        newer = b"# future editor content\n"
        def edit(text):
            message = update.parse_frame(text)
            self.transport.sent.append(message)
            if message["kind"] == "begin":
                (self.root / "peer.py").write_bytes(newer)
        self.transport.send = edit
        manifest = self.automatic()
        receiver = update.UpdateReceiver(self.root, self.config, "A")
        for message in self.transport.sent:
            self.assertFalse(receiver.receive(self.event(message)))
        self.assertEqual((self.root / "peer.py").read_bytes(), newer)
        self.assertFalse((self.root / ".local/update-pending.json").exists())
        self.assertTrue((self.root / ".local/update-self-deferred.json").exists())
        release = update.published_release(self.root, self.config, "A")
        self.assertEqual(update.inspect_bundle(release[1])["peer.py"], old)
        self.assertEqual(release[0]["id"], manifest["id"])

    def test_change_detection_debounces_multiple_edits_and_never_starts_on_B(self):
        driver = self.driver()
        with patch.object(update.subprocess, "Popen", return_value=OwnedProcess()) as launch:
            driver.tick(ready=False, now=0)
            driver.tick(now=0)
            driver.tick(now=29)
            (self.root / "peer.py").write_bytes(b"# edit again\n")
            driver.tick(now=30)
            driver.tick(now=59)
            launch.assert_not_called()
            driver.tick(now=61)
            driver.tick(now=62)
            self.assertEqual(launch.call_count, 1)
            self.assertIn("--automatic", launch.call_args.args[0])
            other = self.driver(peer="B", quiet=0)
            other.tick(now=0)
            other.tick(now=100)
            self.assertEqual(launch.call_count, 1)

    def test_already_published_version_does_not_publish_again(self):
        self.manual()
        driver = self.driver(quiet=0)
        with patch.object(update.subprocess, "Popen") as launch:
            driver.tick(now=0)
            driver.tick(now=100)
            launch.assert_not_called()

    def test_new_versions_observe_minimum_publication_interval(self):
        self.manual()
        (self.root / "peer.py").write_bytes(b"# next release\n")
        driver = self.driver(quiet=0, minimum=300)
        with patch.object(update.subprocess, "Popen", return_value=OwnedProcess()) as launch:
            driver.tick(now=0)
            driver.tick(now=31)
            launch.assert_not_called()
            with patch.object(update.time, "time", return_value=time.time() + 301):
                driver.tick(now=302)
            self.assertEqual(launch.call_count, 1)

    def test_disabled_flag_and_missing_runtime_file_prevent_launch(self):
        settings = update.policy(self.root)
        settings["auto_publish"] = False
        update.save_json(self.root / ".local/update-policy.json", settings)
        driver = self.driver(quiet=0)
        with patch.object(update.subprocess, "Popen") as launch:
            driver.tick(now=0)
            driver.tick(now=100)
            settings["auto_publish"] = True
            update.save_json(self.root / ".local/update-policy.json", settings)
            (self.root / "windows_secret.py").unlink()
            driver.tick(now=101)
            launch.assert_not_called()

    def test_close_stops_only_its_owned_publication_child(self):
        driver = self.driver(quiet=0)
        child = OwnedProcess()
        with patch.object(update.subprocess, "Popen", return_value=child):
            driver.tick(now=0)
            driver.tick(now=1)
        driver.close()
        self.assertEqual(child.returncode, 130)
        self.assertIsNone(driver.process)

    def check_transfer_milestones(self, success):
        events = []
        driver = self.driver(quiet=0)
        driver.emit = lambda event, **fields: events.append((event, fields))
        child = OwnedProcess()
        with patch.object(update.subprocess, "Popen", return_value=child):
            driver.tick(now=0)
            driver.tick(now=1)
        ledger = {"manifest": {"source": "A", "target": "ALL", "id": "abcdef012345", "auto_candidate": driver.active_candidate},
                  "recipients": ["B"], "status": "sending", "frames_sent": 1}
        path = self.root / ".local/update-publication.json"
        update.save_json(path, ledger)
        driver.tick(False, now=2)
        driver.tick(False, now=3)
        self.assertEqual([event for event, _ in events].count("update_transfer_started"), 1)
        self.assertNotIn("update_transfer_sent", [event for event, _ in events])
        state = self.state()
        state["status"] = "published" if success else "delivery_unknown"
        update.save_json(driver.state_path, state)
        ledger["status"] = "published_waiting_for_receivers" if success else "incomplete_do_not_blindly_replay"
        update.save_json(path, ledger)
        child.returncode = 0 if success else 1
        driver.tick(False, now=4)
        driver.tick(False, now=5)
        names = [event for event, _ in events]
        self.assertEqual(names.count("update_transfer_sent"), int(success))
        self.assertNotIn("update_ready", names)
        event, fields = events[names.index("update_transfer_started")]
        self.assertIn("A端开始向B端传输更新", update.update_message(event, **fields))

    def test_automatic_console_reports_transfer_boundaries_once(self):
        self.check_transfer_milestones(True)

    def test_interrupted_transfer_never_reports_finished_or_installed(self):
        self.check_transfer_milestones(False)

    def test_supervisor_publication_child_survives_peer_restart(self):
        script = '''import json,sys,time
from pathlib import Path
root=Path(__file__).resolve().parent
marker=root/'.local/publication-runs'
if 'publish-update' in sys.argv:
 marker.write_text('once')
 time.sleep(1)
else:
 count=root/'.local/peer-runs'
 value=int(count.read_text())+1 if count.exists() else 1
 count.write_text(str(value))
 print(json.dumps({'event':'ready','peer':'A'}),flush=True)
 print(json.dumps({'event':'history_recovered','peer':'A','messages':0}),flush=True)
 if value==1:
  end=time.monotonic()+5
  while not marker.exists() and time.monotonic()<end: time.sleep(.02)
  raise SystemExit(75)
 time.sleep(1.5)
'''
        (self.root / "peer.py").write_text(script, encoding="utf-8")
        original = update.AutoPublisher
        factory = lambda root, config, peer, emit: original(root, config, peer, emit, quiet_seconds=0, poll_seconds=0, min_interval=0)
        with patch.object(supervisor, "AutoPublisher", side_effect=factory), redirect_stdout(io.StringIO()):
            self.assertEqual(supervisor.supervise(self.config_path, "A", self.root), 0)
        self.assertEqual((self.root / ".local/peer-runs").read_text(), "2")
        self.assertEqual((self.root / ".local/publication-runs").read_text(), "once")


if __name__ == "__main__":
    unittest.main()
