"""Build a clean source archive and optional self-contained Windows archive."""
import hashlib
import argparse
import json
from pathlib import Path
import zipfile

from peer import ROOT, workbook_robots

DIAGNOSTICS = "scripts/diagnostics"
EVENT_DIAGNOSTICS = {f"{DIAGNOSTICS}/collect-event-info.{suffix}" for suffix in ("cmd", "ps1", "py")}
BUS_RECOVERY = {f"{DIAGNOSTICS}/recover-event-bus.{suffix}" for suffix in ("cmd", "ps1")}
PACKAGED_DOCS = ("coordinator-start.txt", "connector-recovery-update.txt", "participant-onboarding.txt")
OPERATOR_SCRIPTS = {
    "scripts/setup/setup.cmd",
    "scripts/run/task.cmd", "scripts/run/resume.cmd", "scripts/run/stop-task.cmd",
    "scripts/run/enable-auto-update.cmd",
    "scripts/diagnostics/check.cmd", "scripts/diagnostics/check-webhook.cmd",
    "scripts/diagnostics/collect-group-info.cmd",
    "scripts/release/publish-update.cmd",
} | EVENT_DIAGNOSTICS | BUS_RECOVERY
SOURCE_FILES = (
    ".gitignore", "AGENTS.md", "README.md", "先看这里.txt", "config.example.json",
    "integration-policy.json", "response.schema.json", "toolchain.versions.json",
    "licenses/DWS-LICENSE.txt", "licenses/OpenAI-Codex-LICENSE.txt",
    "start.cmd", "bootstrap.ps1", "install.ps1", "build_package.py", "demo.py",
    "business_consumer.py", "connector_update.py", "peer.py", "reliability.py",
    "setup_helper.py", "supervisor.py", "windows_secret.py",
    "scripts/run/demo.py", "scripts/release/build_package.py",
    "tests/__init__.py",
    "tests/test_auto_onboarding.py", "tests/test_auto_publish.py", "tests/test_build_package.py",
    "tests/test_bus_recovery.py", "tests/test_business_bridge.py", "tests/test_business_consumer.py",
    "tests/test_business_health.py", "tests/test_connector_update.py", "tests/test_discovery.py",
    "tests/test_event_diagnostics.py", "tests/test_instance_control.py", "tests/test_listener_startup.py",
    "tests/test_participants.py", "tests/test_peer.py", "tests/test_project_isolation.py",
    "tests/test_reliability.py", "tests/test_setup.py", "tests/test_task_reports.py",
)


def write_checksum(path):
    with path.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    path.with_suffix(path.suffix + ".sha256").write_text(digest + "  " + path.name + "\n", encoding="ascii")
    return digest


def local_webhook_tokens():
    legacy = ROOT / "local.json"
    if not legacy.exists():
        return []
    config = json.loads(legacy.read_text(encoding="utf-8-sig"))
    workbook = config.get("workbook")
    return [value.encode() for value in workbook_robots(workbook).values()] if workbook and Path(workbook).is_file() else []


def build_event_diagnostics(recovery=False):
    names = EVENT_DIAGNOSTICS | BUS_RECOVERY if recovery else EVENT_DIAGNOSTICS
    dist = ROOT / "dist"
    dist.mkdir(exist_ok=True)
    destination = dist / ("event-bus-recovery.zip" if recovery else "event-diagnostics.zip")
    with zipfile.ZipFile(destination, "w", zipfile.ZIP_DEFLATED) as archive:
        for name in sorted(names):
            raw = (ROOT / name).read_bytes()
            if name.endswith(".cmd"):
                raw.decode("ascii")
            elif name.endswith(".ps1"):
                if not raw.startswith(b"\xef\xbb\xbf"):
                    raise SystemExit("PowerShell diagnostic must retain BOM")
            else:
                compile(raw, name, "exec")
            archive.writestr(name, raw)
    digest = write_checksum(destination)
    print(json.dumps({"archive": str(destination), "bytes": destination.stat().st_size, "files": len(names), "sha256": digest}))


def build_connector_patch():
    from connector_update import ALLOWED, build_bundle
    raw = build_bundle(ROOT)
    dist = ROOT / "dist"
    dist.mkdir(exist_ok=True)
    destination = dist / "connector-recovery-update.zip"
    destination.write_bytes(raw)
    note = ROOT / "docs/connector-recovery-update.txt"
    with zipfile.ZipFile(destination, "a", zipfile.ZIP_DEFLATED) as archive:
        archive.write(note, "docs/connector-recovery-update.txt")
        for name in sorted(OPERATOR_SCRIPTS):
            archive.write(ROOT / name, name)
    with zipfile.ZipFile(destination) as archive:
        if set(archive.namelist()) - (ALLOWED | OPERATOR_SCRIPTS | {"docs/connector-recovery-update.txt"}):
            raise SystemExit("Unexpected patch entry")
        secrets = local_webhook_tokens()
        for name in archive.namelist():
            content = archive.read(name)
            if any(secret in content for secret in secrets):
                raise SystemExit("Credential found in patch (value hidden)")
    digest = write_checksum(destination)
    print(json.dumps({"archive": str(destination), "bytes": destination.stat().st_size, "credential_scan": "passed", "sha256": digest}))


def main():
    parser = argparse.ArgumentParser()
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--patch-only", action="store_true")
    modes.add_argument("--event-diagnostics-only", action="store_true")
    modes.add_argument("--bus-recovery-only", action="store_true")
    args = parser.parse_args()
    if args.bus_recovery_only:
        build_event_diagnostics(recovery=True)
        return
    if args.event_diagnostics_only:
        build_event_diagnostics()
        return
    if args.patch_only:
        build_connector_patch()
        return
    dist = ROOT / "dist"
    dist.mkdir(exist_ok=True)
    names = [*SOURCE_FILES, *sorted(OPERATOR_SCRIPTS),
             *(f"docs/{name}" for name in PACKAGED_DOCS)]
    source = [ROOT / name for name in names]
    for path in source:
        if not path.is_file() or path.resolve() != path.absolute():
            raise SystemExit("Missing or linked package entry: " + path.relative_to(ROOT).as_posix())
    archive = dist / "codex-dingtalk-peer.zip"
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as output:
        for name, path in zip(names, source):
            output.write(path, name)
    runtime = ROOT / "vendor/windows-x64"
    bundle = dist / "codex-dingtalk-peer-windows-full.zip"
    mapping = list(zip(source, names))
    for directory in ("python-3.13.16", "native-dws", "native-codex"):
        folder = runtime / directory
        if not folder.is_dir():
            raise SystemExit("Missing runtime staging: " + directory)
        for path in folder.rglob("*"):
            if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".lock":
                mapping.append((path, ".tools/" + path.relative_to(runtime).as_posix()))
    with zipfile.ZipFile(bundle, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as output:
        for path, name in mapping:
            if name.endswith("._pth"):
                output.writestr(name, "python313.zip\n.\n../..\n")
            else:
                output.write(path, name)
    # Check real local Webhook values without exposing them.
    secrets = local_webhook_tokens()
    for file in (archive, bundle):
        with zipfile.ZipFile(file) as package:
            if any(name.startswith((".local/", "runtime/")) or name.endswith(".xlsx") or name in ("local.json", ".tools/toolchain.json") for name in package.namelist()):
                raise SystemExit("Private file detected in package")
            for name in package.namelist():
                with package.open(name) as stream:
                    tail = b""
                    while True:
                        chunk = stream.read(1024 * 1024)
                        if not chunk: break
                        data = tail + chunk
                        if any(secret in data for secret in secrets):
                            raise SystemExit("Credential detected in package (value hidden)")
                        tail = data[-1024:]
        digest = write_checksum(file)
        print(json.dumps({"archive": str(file), "bytes": file.stat().st_size, "credential_scan": "passed", "sha256": digest}), flush=True)


if __name__ == "__main__":
    main()
