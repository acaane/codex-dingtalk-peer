"""Foreground connector supervisor. Never launches or restarts business programs."""
import argparse
from contextlib import redirect_stdout
import hashlib
import json
import os
from pathlib import Path
import queue
import re
import signal
import subprocess
import sys
import threading
import time

from connector_update import AutoPublisher, apply_pending, capture_baseline, emit_update, mark_ready, recover_install, rollback, safe_path, save_json, stop_process
from reliability import PeerAlreadyRunning, PeerError, acquire_instance_lock, instance_lock_owner

ROOT = Path(__file__).resolve().parent


def monitor_signature(root):
    return tuple(hashlib.sha256((root / name).read_bytes()).hexdigest() if (root / name).is_file() else None
                 for name in ("supervisor.py", "connector_update.py", "reliability.py"))


def supervise(config, peer_id, root=ROOT, startup_timeout=90):
    if not re.fullmatch(r"[A-Z][A-Z0-9_-]*", peer_id):
        raise PeerError("Invalid peer ID")
    root = Path(root).resolve()
    runtime = safe_path(root, f"runtime/{peer_id}")
    # Acquire before update recovery/install: a second launcher must not touch live files.
    lock = acquire_instance_lock(runtime / "supervisor.lock", {"peer": peer_id, "started_at": time.time()})
    try:
        if instance_lock_owner(runtime / "run.lock") is not None:
            raise PeerAlreadyRunning("连接器已有实例运行；不会重复启动。")
        with (runtime / "connector.log").open("a", encoding="utf-8", buffering=1) as journal:
            class Output:
                def __init__(self, console):
                    self.console = console
                def write(self, text):
                    journal.write(text)
                    return self.console.write(text)
                def flush(self):
                    journal.flush()
                    self.console.flush()
            with redirect_stdout(Output(sys.stdout)):
                print(f"【连接器】{peer_id}端监护器已启动，进程 {os.getpid()}。", flush=True)
                return supervise_owned(config, peer_id, root, startup_timeout)
    finally:
        lock.close()


def supervise_owned(config, peer_id, root, startup_timeout):
    def emit(event, **fields):
        emit_update(event, **fields)
    publisher = AutoPublisher(root, config, peer_id, emit)
    try:
        return supervise_loop(config, peer_id, root, startup_timeout, publisher)
    finally:
        try:
            publisher.close()
        except PeerError as error:
            emit("auto_publisher_still_running", reason=str(error))
            publisher.process.wait()
            publisher.close()


def supervise_loop(config, peer_id, root, startup_timeout, publisher):
    root = Path(root).resolve()
    loaded_monitor = monitor_signature(root)
    recover_install(root)
    while True:
        try:
            update = apply_pending(root, peer_id=peer_id)
        except (PeerError, OSError) as error:
            recover_install(root)
            pending = safe_path(root, ".local/update-pending.json")
            if pending.exists():
                pending.unlink()
            emit_update("update_install_failed", peer=peer_id, reason=str(error))
            update = None
        command = [sys.executable, str(root / "peer.py"), "--config", str(config), "--peer", peer_id, "run"]
        child_env = dict(os.environ, PEER_UPDATE_STATUS_OWNER="supervisor")
        child = subprocess.Popen(command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                 text=True, encoding="utf-8", errors="replace", bufsize=1,
                                 env=child_env,
                                 creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0)
        output = queue.Queue()

        def read_output():
            for line in child.stdout:
                output.put(line)
            output.put(None)

        reader = threading.Thread(target=read_output, daemon=True)
        reader.start()
        deadline = time.monotonic() + startup_timeout
        timed_out = False
        baseline_captured = False
        restart_supervisor = False
        restart_update = None
        history_complete = False
        history_enabled = json.loads(Path(config).read_text(encoding="utf-8-sig")).get("history_recovery", True) if Path(config).exists() else True
        try:
            while True:
                publisher.tick(baseline_captured and (history_complete or not history_enabled))
                if update and time.monotonic() >= deadline:
                    stop_process(child)
                    timed_out = True
                    break
                try:
                    line = output.get(timeout=0.5)
                except queue.Empty:
                    if update and time.monotonic() >= deadline:
                        stop_process(child)
                        timed_out = True
                        break
                    if child.poll() is not None:
                        break
                    continue
                if line is None:
                    break
                print(line, end="", flush=True)
                try:
                    item = json.loads(line)
                except ValueError:
                    item = {}
                if item.get("peer") == peer_id and item.get("event") in ("history_recovered", "history_recovery_unavailable"):
                    history_complete = True
                if update and item.get("event") == "ready" and item.get("peer") == peer_id:
                    mark_ready(root, update, peer_id)
                    emit_update("update_ready", peer=peer_id, update=update["id"])
                    restart_update = update["id"]
                    update = None
                    restart_supervisor = monitor_signature(root) != loaded_monitor
                if not baseline_captured and item.get("event") == "ready" and item.get("peer") == peer_id:
                    try:
                        capture_baseline(root)
                        baseline_captured = True
                    except OSError:
                        print(json.dumps({"event": "stable_snapshot_failed"}), flush=True)
                if restart_supervisor:
                    # Complete the final upload receipt before replacing its owner.
                    if publisher.process is not None:
                        try:
                            publisher.process.wait(timeout=60)
                        except subprocess.TimeoutExpired:
                            publisher.close()
                        publisher.tick(False)
                    save_json(safe_path(root, ".local/update-reload-request.json"), {"peer": peer_id, "pid": child.pid, "update": restart_update, "time": time.time()})
                    stop_process(child)
                    break
            code = child.wait()
        except KeyboardInterrupt:
            try:
                stop_process(child)
            except PeerError as error:
                print(str(error), flush=True)
                child.wait()
            return 130
        except PeerError:
            # Keep ownership; rollback must not race a still-running new version.
            child.wait()
            raise
        finally:
            if child.poll() is not None:
                reader.join(timeout=2)
                child.stdout.close()
        if update:
            rollback(root, update)
            emit_update("update_rolled_back", peer=peer_id, update=update["id"], startup_timeout=timed_out)
            continue
        if restart_supervisor:
            safe_path(root, ".local/update-reload-request.json").unlink(missing_ok=True)
            print(json.dumps({"event": "supervisor_reloading", "peer": peer_id}), flush=True)
            return 76
        if code != 75:
            return code


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--peer", required=True)
    args = parser.parse_args()
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, signal.default_int_handler)
    return supervise(Path(args.config).resolve(), args.peer)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except PeerAlreadyRunning as error:
        print(str(error), flush=True)
        sys.exit(20)
    except PeerError as error:
        print(str(error), flush=True)
        sys.exit(1)
