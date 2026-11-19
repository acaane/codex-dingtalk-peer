"""Build a clean source archive and optional self-contained Windows archive."""
import hashlib
import argparse
import json
from pathlib import Path
import zipfile

from peer import ROOT, workbook_robots

EVENT_DIAGNOSTICS = {"collect-event-info.cmd", "collect-event-info.ps1", "collect-event-info.py"}
PACKAGED_DOCS = ("coordinator-start.txt", "connector-recovery-update.txt", "participant-onboarding.txt")


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
    names = EVENT_DIAGNOSTICS | {"recover-event-bus.cmd", "recover-event-bus.ps1"} if recovery else EVENT_DIAGNOSTICS
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
        for name in sorted(EVENT_DIAGNOSTICS):
            archive.write(ROOT / name, name)
    with zipfile.ZipFile(destination) as archive:
        if set(archive.namelist()) - (ALLOWED | EVENT_DIAGNOSTICS | {"docs/connector-recovery-update.txt"}):
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
    source = [p for p in ROOT.iterdir() if p.is_file() and (p.suffix in (".py", ".ps1", ".cmd") or p.name in ("README.md", "AGENTS.md", "HANDOFF.md", "先看这里.txt", "config.example.json", "response.schema.json", "toolchain.versions.json", "integration-policy.json", ".gitignore"))]
    source += [p for p in (ROOT / "licenses").iterdir() if p.is_file()]
    source += [ROOT / "docs" / name for name in PACKAGED_DOCS]
    archive = dist / "codex-dingtalk-peer.zip"
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as output:
        for path in source:
            output.write(path, path.relative_to(ROOT))
    runtime = ROOT / "vendor/windows-x64"
    bundle = dist / "codex-dingtalk-peer-windows-full.zip"
    mapping = [(p, p.relative_to(ROOT).as_posix()) for p in source]
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
