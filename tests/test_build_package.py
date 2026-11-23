"""Deployment archives must not pick up local state, business records or known credentials."""
import contextlib
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from scripts.release import build_package


class PackageTests(unittest.TestCase):
    def make_workspace(self, root):
        names = (set(build_package.SOURCE_FILES) | build_package.OPERATOR_SCRIPTS
                 | {f"docs/{name}" for name in build_package.PACKAGED_DOCS})
        files = {name: "\ufeff" if name.endswith(".ps1") else "" for name in names}
        files.update({
            "peer.py": "print('connector')\n",
            "response.schema.json": "{}",
            "README.md": "Contributor documentation\n",
            "licenses/LICENSE.txt": "License\n",
            "docs/coordinator-start.txt": "Coordinator entry\n",
            "docs/connector-recovery-update.txt": "Current connector update guide\n",
            "docs/participant-onboarding.txt": "First connection instructions\n",
            "docs/internal-note.txt": "Machine-specific maintenance note\n",
            "local.json": "{}",
            "HANDOFF.md": "Local handoff",
            "accidental.py": "A local script",
            ".local/private.json": "private machine configuration",
            "runtime/A/state.sqlite3": "private pending work",
            "联调资料/项目/模块/证据.json": "business evidence",
            "archive/old.py": "obsolete source",
            ".tools/toolchain.json": "machine-specific paths",
            "vendor/windows-x64/python-3.13.16/python313._pth": "old runtime path\n",
            "vendor/windows-x64/native-dws/dws.exe": "portable dws",
            "vendor/windows-x64/native-codex/codex.exe": "portable codex",
        })
        for name, text in files.items():
            target = root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text, encoding="utf-8")

    def test_packages_exclude_private_state_and_business_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.make_workspace(root)
            with patch.object(build_package, "ROOT", root), patch("sys.argv", ["build_package.py"]), contextlib.redirect_stdout(io.StringIO()):
                build_package.main()
            for name in ["codex-dingtalk-peer.zip", "codex-dingtalk-peer-windows-full.zip"]:
                package = root / "dist" / name
                with zipfile.ZipFile(package) as archive:
                    names = archive.namelist()
                    self.assertIn("peer.py", names)
                    self.assertIn("scripts/setup/setup.cmd", names)
                    self.assertIn("scripts/diagnostics/recover-event-bus.ps1", names)
                    self.assertIn("tests/test_peer.py", names)
                    self.assertFalse(any(n.startswith((".local/", "runtime/", "联调资料/", "archive/")) for n in names))
                    self.assertNotIn("local.json", names)
                    self.assertNotIn(".tools/toolchain.json", names)
                    self.assertNotIn("docs/internal-note.txt", names)
                    self.assertNotIn("HANDOFF.md", names)
                    self.assertNotIn("accidental.py", names)
                    if "windows-full" in name:
                        self.assertIn(".tools/native-dws/dws.exe", names)
                        self.assertEqual(archive.read(".tools/python-3.13.16/python313._pth"), b"python313.zip\n.\n../..\n")
                recorded = package.with_suffix(".zip.sha256").read_text(encoding="ascii").split()
                self.assertEqual(recorded, [hashlib.sha256(package.read_bytes()).hexdigest(), name])

    def test_recovery_archive_includes_relocated_operator_entries(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.make_workspace(root)
            with patch.object(build_package, "ROOT", root), contextlib.redirect_stdout(io.StringIO()):
                build_package.build_connector_patch()
            with zipfile.ZipFile(root / "dist/connector-recovery-update.zip") as archive:
                self.assertTrue(build_package.OPERATOR_SCRIPTS <= set(archive.namelist()))
                self.assertNotIn("HANDOFF.md", archive.namelist())

    def test_known_credential_in_allowed_source_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.make_workspace(root)
            workbook = root / "private.xlsx"
            workbook.write_bytes(b"synthetic workbook fixture")
            token = "synthetic-webhook-token-for-regression"
            (root / "local.json").write_text(json.dumps({"workbook": str(workbook)}), encoding="utf-8")
            (root / "peer.py").write_text(repr(token), encoding="utf-8")
            with patch.object(build_package, "ROOT", root), patch.object(build_package, "workbook_robots", return_value={"robot": token}), patch("sys.argv", ["build_package.py"]), contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(SystemExit, "Credential detected"):
                    build_package.main()




if __name__ == "__main__":
    unittest.main()
