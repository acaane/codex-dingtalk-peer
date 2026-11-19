"""Read-only DWS bus diagnostics. Never reads credential or subscription files."""
import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import sys


def redact(value):
    if isinstance(value, dict):
        return {key: ("<credential hidden>" if re.search(r"token|secret|password|authorization|cookie|credential|webhook", key, re.I)
                      else redact(item)) for key, item in value.items()}
    if isinstance(value, list):
        return [redact(item) for item in value]
    if not isinstance(value, str):
        return value
    value = re.sub(r"https?://[^\s\"'<>]+", "<url hidden>", value)
    value = re.sub(r"(?i)((?:access[_-]?token|refresh[_-]?token|token|appsecret|client_secret|secret|password|authorization|cookie)\s*[\"']?\s*[:=]\s*)(?:\"[^\"]*\"|'[^']*'|(?:Bearer\s+)?[^\s,;\"']+)", r"\1<credential hidden>", value)
    value = re.sub(r"(?i)((?:authorization|cookie)\s*[\"']?\s*[:=]\s*)[^\r\n]+", r"\1<credential hidden>", value)
    return re.sub(r"[A-Za-z0-9_+/=-]{24,}", "<identifier hidden>", value)


def run_read(command, timeout=30):
    try:
        result = subprocess.run(command, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"error_type": type(error).__name__}
    try:
        data = json.loads(result.stdout)
    except ValueError:
        data = (result.stdout + "\n" + result.stderr)[-12000:]
    return {"exit_code": result.returncode, "result": redact(data)}


def collect(root):
    root = Path(root).resolve()
    config = json.loads((root / ".local/config.json").read_text(encoding="utf-8-sig"))
    machine = json.loads((root / ".local/machine.json").read_text(encoding="utf-8-sig"))
    peer = machine.get("peer", "UNKNOWN")
    if not re.fullmatch(r"[A-Z][A-Z0-9_-]*", peer):
        peer = "UNKNOWN"
    dws_root = Path(os.environ.get("DWS_CONFIG_DIR") or Path.home() / ".dws")
    status = run_read([config["dws_path"], "event", "status", "--profile", config["profile"], "--format", "json"])
    report = {"created_at": datetime.datetime.now().astimezone().isoformat(), "peer": peer,
              "read_only": True, "dws_config_dir": str(dws_root), "event_status": status, "bus_logs": []}
    directory = dws_root / "events/open/personal_stream"
    logs = sorted(directory.glob("*/bus.log"), key=lambda path: path.stat().st_mtime, reverse=True)[:5]
    for path in logs:
        try:
            with path.open("rb") as stream:
                stream.seek(max(0, path.stat().st_size - 65536))
                tail = stream.read().decode("utf-8", errors="replace")
            lines = [line for line in tail.splitlines() if re.search(r"error|fail|fatal|panic|denied|timeout|timed out|cooldown|startup|exit|exception", line, re.I)][-80:]
            report["bus_logs"].append({"path": str(path), "last_write": path.stat().st_mtime,
                                       "error_lines": redact(lines)})
        except OSError as error:
            report["bus_logs"].append({"path": str(path), "error_type": type(error).__name__})
    if os.name == "nt":
        report["dws_processes"] = run_read(["powershell.exe", "-NoProfile", "-Command",
            "Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'dws.exe' } | Select-Object ProcessId,ParentProcessId,CreationDate | ConvertTo-Json -Compress"], timeout=15)
    output = root / "runtime" / peer / "listener-diagnostics.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return output


def recovery_plan(root):
    """Read-only proof and exact target for the separate Windows recovery script."""
    root = Path(root).resolve()
    config = json.loads((root / ".local/config.json").read_text(encoding="utf-8-sig"))
    status = run_read([config["dws_path"], "event", "status", "--profile", config["profile"], "--format", "json"])
    if status.get("exit_code") != 0 or not isinstance(status.get("result"), dict):
        raise ValueError("event_status_unavailable")
    data = status["result"]
    bus, identity = data.get("bus", {}), data.get("identity", {})
    if not isinstance(bus, dict) or not isinstance(identity, dict):
        raise ValueError("event_status_shape_changed")
    if bus.get("live") is not None:
        raise ValueError("bus_has_live_response_keep_running")
    if data.get("subscriptions") != []:
        raise ValueError("subscriptions_present_stop_connector_first")
    entry = bus.get("entry")
    if not isinstance(entry, dict):
        raise ValueError("no_stale_bus_entry")
    identity_hash = identity.get("identity_hash")
    if not isinstance(identity_hash, str) or not re.fullmatch(r"[0-9a-f]{16}", identity_hash):
        raise ValueError("invalid_identity_hash")
    dws_root = Path(os.environ.get("DWS_CONFIG_DIR") or Path.home() / ".dws").resolve()
    expected = dws_root / "events/open/personal_stream" / identity_hash
    if (entry.get("edition") != "open" or entry.get("source_kind") != "personal_stream"
            or entry.get("identity_hash") != identity_hash or not isinstance(entry.get("workdir"), str)
            or Path(entry["workdir"]).resolve() != expected):
        raise ValueError("bus_directory_does_not_match_current_profile")
    holder = entry.get("holder_pid")
    if type(holder) is not int or not 0 < holder < 2**31:
        raise ValueError("no_recorded_pid_to_recover")
    return {"ok": True, "dws_root": str(dws_root), "identity_hash": identity_hash,
            "holder_pid": holder, "dws_executable_name": Path(config["dws_path"]).name,
            "lock_file": str(expected / "bus.lock")}


if __name__ == "__main__":
    if sys.argv[1:] == ["--bus-plan"]:
        try:
            print(json.dumps(recovery_plan(Path(__file__).resolve().parent), ensure_ascii=False))
        except (OSError, ValueError, KeyError) as error:
            print(json.dumps({"ok": False, "reason": str(error) if isinstance(error, ValueError) else type(error).__name__}))
        raise SystemExit(0)
    print("Read-only DWS diagnostics. No login, subscription changes, or messages.", flush=True)
    try:
        print("Saved: " + str(collect(Path(__file__).resolve().parent)), flush=True)
    except (OSError, ValueError, KeyError) as error:
        print("Collection failed: " + type(error).__name__, flush=True)
        raise SystemExit(1)
