import importlib.util
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "event_info", Path(__file__).resolve().parents[1] / "scripts/diagnostics/collect-event-info.py"
)
info = importlib.util.module_from_spec(spec)
spec.loader.exec_module(info)


class EventDiagnosticsTests(unittest.TestCase):
    def test_credentials_are_removed_and_error_details_remain(self):
        raw = {"refresh_token": "short-secret", "error": "bus child exit status 5; token=short-secret; authorization: Bearer abcdef; https://example.com?access_token=secret"}
        result = json.dumps(info.redact(raw))
        for secret in ("short-secret", "abcdef", "access_token=secret"):
            self.assertNotIn(secret, result)
        self.assertIn("exit status 5", result)
        quoted = info.redact('ERROR {"token":"tiny secret", "message":"bus failed"} Cookie: x=one; y=two')
        for secret in ("tiny secret", "x=one", "y=two"):
            self.assertNotIn(secret, quoted)

    def test_collect_reads_only_status_and_error_lines_without_modifying_shared_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "connector"
            local = root / ".local"
            local.mkdir(parents=True)
            (local / "config.json").write_text(json.dumps({"dws_path": "existing-dws.exe", "profile": "configured-profile", "credential_file": "do-not-read"}), encoding="utf-8")
            (local / "machine.json").write_text('{"peer":"B"}', encoding="utf-8")
            bus = Path(directory) / "dws/events/open/personal_stream/identity"
            bus.mkdir(parents=True)
            log = bus / "bus.log"
            log.write_text('normal private chat content\nERROR bus startup exit status 5 token=secret-value\n', encoding="utf-8")
            attempts = bus / "personal_subscription_attempts.json"
            attempts.write_text('unchanged-protection', encoding="utf-8")
            with patch.dict(os.environ, {"DWS_CONFIG_DIR": str(Path(directory) / "dws")}), patch.object(info, "run_read", return_value={"exit_code": 0}) as run:
                output = info.collect(root)
            command = run.call_args_list[0].args[0]
            self.assertEqual(command, ["existing-dws.exe", "event", "status", "--profile", "configured-profile", "--format", "json"])
            text = output.read_text(encoding="utf-8")
            self.assertNotIn("normal private chat", text)
            self.assertNotIn("secret-value", text)
            self.assertIn("exit status 5", text)
            self.assertEqual(attempts.read_text(encoding="utf-8"), 'unchanged-protection')
            self.assertTrue(output.is_relative_to(root / "runtime/B"))


if __name__ == "__main__":
    unittest.main()
