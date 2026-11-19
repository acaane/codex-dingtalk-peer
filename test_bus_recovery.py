import base64
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("bus_recovery_info", Path(__file__).with_name("collect-event-info.py"))
info = importlib.util.module_from_spec(spec)
spec.loader.exec_module(info)


class RecoveryPlanTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name) / "connector"
        (self.root / ".local").mkdir(parents=True)
        (self.root / ".local/config.json").write_text(json.dumps({"dws_path": "existing-dws.exe", "profile": "configured-profile"}), encoding="utf-8")
        self.dws_root = Path(self.folder.name) / "dws"
        self.identity = "3f5f732e0a23dcba"
        self.status = {"bus": {"entry": {"workdir": str(self.dws_root / "events/open/personal_stream" / self.identity),
                       "identity_hash": self.identity, "edition": "open", "source_kind": "personal_stream", "holder_pid": 83428}},
                       "identity": {"identity_hash": self.identity}, "subscriptions": []}

    def tearDown(self):
        self.folder.cleanup()

    def plan(self):
        with patch.dict(os.environ, {"DWS_CONFIG_DIR": str(self.dws_root)}), patch.object(info, "run_read", return_value={"exit_code": 0, "result": self.status}):
            return info.recovery_plan(self.root)

    def test_stale_entry_plan_targets_only_current_identity_lock(self):
        plan = self.plan()
        self.assertTrue(plan["ok"])
        self.assertEqual(Path(plan["lock_file"]), self.dws_root / "events/open/personal_stream" / self.identity / "bus.lock")

    def test_live_bus_and_active_subscriptions_are_not_recovered(self):
        self.status["bus"]["live"] = {"source_state": {"state": "connected"}}
        with self.assertRaisesRegex(ValueError, "live_response"):
            self.plan()
        del self.status["bus"]["live"]
        self.status["subscriptions"] = [{"subscribe_id": "existing"}]
        with self.assertRaisesRegex(ValueError, "subscriptions_present"):
            self.plan()

    def test_other_identity_directory_is_refused(self):
        self.status["bus"]["entry"]["workdir"] = str(self.dws_root / "events/open/personal_stream/ffffffffffffffff")
        with self.assertRaisesRegex(ValueError, "does_not_match"):
            self.plan()


@unittest.skipUnless(os.name == "nt", "Recovery uses Windows PowerShell and Windows DWS locks")
class WindowsBusPidRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory(prefix="AI bus recovery ")
        self.root = Path(self.folder.name)
        (self.root / ".local").mkdir()
        self.identity = "3f5f732e0a23dcba"
        self.dws_root = self.root / "dws"
        self.bus = self.dws_root / "events/open/personal_stream" / self.identity
        self.bus.mkdir(parents=True)
        self.pid = os.getpid()
        self.lock = self.bus / "bus.lock"
        self.old = str(self.pid).encode() + b"\n"
        self.lock.write_bytes(self.old)
        self.protected = {"bus.meta": b"historical-meta", "personal_subscription_attempts.json": b"protection", "personal_subscription_attempts.lock": b"protection-lock", "personal_subscriptions.json": b"subscriptions"}
        for name, data in self.protected.items():
            (self.bus / name).write_bytes(data)

    def tearDown(self):
        self.folder.cleanup()

    def run_recovery(self, expected=None):
        def quote(value):
            return "'" + str(value).replace("'", "''") + "'"
        script = Path(__file__).with_name("recover-event-bus.ps1").resolve()
        command = ". " + quote(script) + " -LibraryOnly\nClear-StaleBusPid -DwsRoot " + quote(self.dws_root) + " -IdentityHash " + quote(self.identity) + " -ExpectedPid " + str(expected or self.pid) + " -PeerRoot " + quote(self.root)
        return subprocess.run(["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", command], capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=15)

    def test_recycled_live_pid_is_cleared_without_stopping_process_or_removing_lock(self):
        inode = self.lock.stat().st_ino
        result = self.run_recovery()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.lock.read_bytes(), b"")
        self.assertEqual(self.lock.stat().st_ino, inode)
        self.assertEqual(os.getpid(), self.pid)
        for name, data in self.protected.items():
            self.assertEqual((self.bus / name).read_bytes(), data)
        backups = list((self.root / ".local").glob("bus-pid-recovery-*.json"))
        self.assertEqual(len(backups), 1)
        record = json.loads(backups[0].read_text(encoding="utf-8-sig"))
        self.assertEqual(base64.b64decode(record["old_bytes_base64"]), self.old)
        self.assertEqual(record["status"], "stale_pid_cleared")

    def test_actual_range_lock_owner_is_never_reset(self):
        import msvcrt
        with self.lock.open("r+b") as held:
            held.seek(1073741824)
            msvcrt.locking(held.fileno(), msvcrt.LK_NBLCK, 1)
            try:
                result = self.run_recovery()
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(self.lock.read_bytes(), self.old)
            finally:
                held.seek(1073741824)
                msvcrt.locking(held.fileno(), msvcrt.LK_UNLCK, 1)
        self.assertEqual(list((self.root / ".local").glob("bus-pid-recovery-*.json")), [])

    def test_pid_changed_during_checks_is_not_reset(self):
        result = self.run_recovery(expected=self.pid + 1)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.lock.read_bytes(), self.old)


if __name__ == "__main__":
    unittest.main()
