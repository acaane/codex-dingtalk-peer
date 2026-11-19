"""Small connector-only updates carried by the existing authenticated group bots."""
import base64
import hashlib
import io
import json
import os
from pathlib import Path
import queue
import re
import stat
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid
import zipfile

from reliability import DeliveryUnknown, PeerError, SendRejected, retry_delay

PREFIX = "【AI联调更新】"
MAX_BYTES = 96 * 1024
CHUNK_CHARS = 3000
ALLOWED = frozenset(("peer.py", "reliability.py", "connector_update.py", "supervisor.py",
                     "setup_helper.py", "business_consumer.py", "windows_secret.py", "response.schema.json",
                     "install.ps1", "bootstrap.ps1", "start.cmd", "setup.cmd", "task.cmd",
                     "stop-task.cmd", "check.cmd", "resume.cmd", "enable-auto-update.cmd", "publish-update.cmd"))


def update_message(event, **fields):
    """Describe milestones locally; sending a package does not confirm installation."""
    def endpoint(value, fallback):
        return value + "端" if isinstance(value, str) and re.fullmatch(r"[A-Z][A-Z0-9_-]{0,63}", value) else fallback
    peer = endpoint(fields.get("peer"), "本端")
    source = endpoint(fields.get("source", fields.get("peer")), "发布端")
    recipients = fields.get("recipients", [])
    target = "、".join(endpoint(item, "接收端") for item in recipients) if recipients else endpoint(fields.get("target"), "接收端")
    identity = fields.get("update", fields.get("release"))
    suffix = " 更新编号：" + identity if isinstance(identity, str) and re.fullmatch(r"[0-9a-f]{12}", identity) else ""
    messages = {
        "update_transfer_started": f"{source}开始向{target}传输更新。",
        "update_transfer_sent": f"{source}向{target}的更新传输结束，等待接收端安装并确认就绪。",
        "update_staged": f"{peer}已接收并校验更新，开始更新和重启连接器。",
        "update_install_started": f"{peer}开始安装更新，安装后将重启连接器。",
        "update_ready": f"{peer}更新完成，连接器已重启并就绪。",
        "update_peer_ready": f"已收到{peer}的更新完成确认，连接器已就绪。",
        "update_rolled_back": f"{peer}更新失败，已恢复上一版本。",
        "update_install_failed": f"{peer}未能完成更新安装，请查看错误日志。",
    }
    return "【连接器更新】" + messages[event] + suffix if event in messages else None


def emit_update(event, **fields):
    print(json.dumps(dict(event=event, **fields), ensure_ascii=False), flush=True)
    message = update_message(event, **fields)
    if message:
        print(message, flush=True)


def safe_path(root, name):
    root = Path(root).resolve()
    path = root / name
    try:
        path.resolve().relative_to(root)
    except ValueError:
        raise PeerError("Update path escapes connector directory") from None
    for part in [path, *path.parents]:
        if part == root:
            break
        if part.exists() and (part.is_symlink() or getattr(part.lstat(), "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)):
            raise PeerError("Update path contains a link or reparse point")
    return path


def write_atomic(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".peer-update-", delete=False) as output:
        output.write(data)
        temporary = Path(output.name)
    try:
        # Windows readers can briefly deny replacement even though writing the
        # temporary file succeeded. Retain the old complete document throughout.
        for attempt in range(7):
            try:
                os.replace(str(temporary), str(path))
                break
            except OSError as error:
                if getattr(error, 'winerror', None) not in (5, 32, 33) or attempt == 6:
                    raise
                time.sleep(.01 * (2 ** attempt))
    finally:
        if temporary.exists():
            temporary.unlink()


def save_json(path, value):
    write_atomic(path, json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8"))


def policy(root):
    path = safe_path(root, ".local/update-policy.json")
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"enabled": False}


def enable(root, publisher):
    settings = policy(root)
    settings.update(enabled=True, publisher=publisher)
    settings.setdefault("auto_publish", True)
    save_json(safe_path(root, ".local/update-policy.json"), settings)


def frame(data):
    text = PREFIX + json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    if len(text) > 5000:
        raise PeerError("Update frame too large")
    return text


def parse_frame(text):
    if not text.startswith(PREFIX) or len(text) > 5000:
        return None
    try:
        data = json.loads(text[len(PREFIX):])
    except ValueError:
        return None
    if not isinstance(data, dict) or not re.fullmatch(r"[0-9a-f]{12}", str(data.get("id", ""))):
        return None
    return data


def trusted_frame(config, event):
    data = parse_frame(event.get("content", ""))
    if not data or event.get("conversation_id") != config["group_id"]:
        return None
    sender = next((p for p in config["peers"] if p["id"] == data.get("source")), None)
    return data if sender and event.get("sender_open_dingtalk_id") == sender["sender_id"] else None


def inspect_bundle(raw):
    if len(raw) > MAX_BYTES:
        raise PeerError("Connector update exceeds size limit")
    files = {}
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            names = archive.namelist()
            if len(names) != len(set(names)) or not names or any(name not in ALLOWED for name in names):
                raise PeerError("Update contains a non-allowlisted file")
            if sum(item.file_size for item in archive.infolist()) > 1024 * 1024:
                raise PeerError("Expanded connector update too large")
            for item in archive.infolist():
                if stat.S_ISLNK(item.external_attr >> 16):
                    raise PeerError("Update archive contains a link")
                data = archive.read(item)
                if item.filename.endswith(".py"):
                    compile(data, item.filename, "exec")
                elif item.filename.endswith(".cmd"):
                    data.decode("ascii")
                elif item.filename.endswith(".ps1"):
                    if not data.startswith(b"\xef\xbb\xbf"):
                        raise PeerError("PowerShell update must retain UTF-8 BOM")
                    data.decode("utf-8-sig")
                elif item.filename.endswith(".json"):
                    json.loads(data)
                files[item.filename] = data
    except (zipfile.BadZipFile, SyntaxError, UnicodeError, ValueError) as error:
        raise PeerError("Invalid connector update content") from error
    return files


def build_bundle(root):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        for name in sorted(ALLOWED):
            path = safe_path(root, name)
            if path.is_file():
                # Stable bytes across computers and restarts; ZIP timestamps are not versions.
                info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                info.create_system = 3
                info.external_attr = 0o100644 << 16
                archive.writestr(info, path.read_bytes())
    raw = output.getvalue()
    inspect_bundle(raw)
    return raw


def capture_baseline(root):
    identity = uuid.uuid4().hex[:12]
    files = {}
    for name in sorted(ALLOWED):
        source = safe_path(root, name)
        if source.is_file():
            raw = source.read_bytes()
            write_atomic(safe_path(root, ".local/stable-versions/" + identity + "/" + name), raw)
            files[name] = hashlib.sha256(raw).hexdigest()
    save_json(safe_path(root, ".local/stable-version.json"), {"id": identity, "files": files})


def content_digest(raw):
    files = inspect_bundle(raw)
    return files_digest(files)


def files_digest(files):
    fingerprint = {name: hashlib.sha256(data).hexdigest() for name, data in sorted(files.items())}
    return hashlib.sha256(json.dumps(fingerprint, sort_keys=True, separators=(",", ":")).encode("ascii")).hexdigest()


class PublicationBusy(PeerError):
    pass


class PublicationChanged(PeerError):
    pass


class PublicationLock:
    """Serialize manual publications, automatic publications and startup transfers."""
    def __init__(self, root):
        self.path = safe_path(root, ".local/update-publish.lock")
        self.file = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file = self.path.open("a+b")
        self.file.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.file.close()
            self.file = None
            raise PublicationBusy("A connector publication is already running") from None
        try:
            if not self.path.stat().st_size:
                self.file.write(b"0")
                self.file.flush()
        except OSError:
            self.file.close()
            raise
        return self

    def __exit__(self, *args):
        self.file.close()


def publication_candidate(root):
    files = {}
    for name in sorted(ALLOWED):
        path = safe_path(root, name)
        if not path.is_file():
            raise PeerError("Automatic publication requires all connector runtime files")
        files[name] = path.read_bytes()
    runtime_digest = files_digest(files)
    tests = sorted(Path(root).glob("test_*.py"))
    for path in tests:
        files[path.name] = safe_path(root, path.name).read_bytes()
    return {"candidate": files_digest(files), "content_sha256": runtime_digest, "test_files": len(tests)}


def validate_publication(root, config, peer, transport, expected):
    candidate = publication_candidate(root)
    if candidate["candidate"] != expected:
        raise PublicationChanged("Connector changed before validation")
    raw = build_bundle(root)
    files = inspect_bundle(raw)
    if content_digest(raw) != candidate["content_sha256"]:
        raise PublicationChanged("Connector changed during snapshot")
    if not candidate["test_files"]:
        raise PeerError("Automatic publication requires local unit tests")
    for data in files.values():
        if (getattr(transport, "token", "").encode() and transport.token.encode() in data
                or re.search(rb"https://oapi\.dingtalk\.com/robot/send\?access_token=[A-Za-z0-9_-]{8,}", data)):
            raise PeerError("Credential detected in connector source (value hidden)")
    cache = str(safe_path(root, ".local/validation-cache/" + expected))
    result = subprocess.run([sys.executable, "-B", "-X", "pycache_prefix=" + cache, "-W", "error::ResourceWarning", "-m", "unittest", "discover", "-q"],
                            cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=180)
    # Keep raw test output in memory only; test code may print private values.
    match = re.search(rb"Ran (\d+) tests? in", result.stdout)
    if result.returncode or not match or int(match[1]) == 0:
        raise PeerError("Automatic publication unit tests failed or no tests ran")
    if os.name == "nt":
        script = "$failed = $false\n"
        for name in sorted(files):
            if name.endswith(".ps1"):
                path = str(safe_path(root, name)).replace("'", "''")
                script += "$t=$null; $e=$null; [System.Management.Automation.Language.Parser]::ParseFile('" + path + "',[ref]$t,[ref]$e) | Out-Null; if ($e.Count) {$failed=$true}\n"
        script += "if ($failed) {exit 1}"
        parsed = subprocess.run(["powershell.exe", "-NoProfile", "-Command", script], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30)
        if parsed.returncode:
            raise PeerError("Automatic publication PowerShell syntax check failed")
    if publication_candidate(root)["candidate"] != expected:
        raise PublicationChanged("Connector or tests changed during validation")
    return raw, candidate, int(match[1])


def publish(root, config, peer, transport, target="ALL", automatic=False, expected_candidate=None):
    settings = policy(root)
    if not settings.get("enabled") or settings.get("publisher") != peer["id"] or automatic and settings.get("auto_publish", True) is False:
        raise PeerError("Connector publication is not enabled for this peer")
    with PublicationLock(root):
        state_path = safe_path(root, ".local/auto-publication.json")
        state = None
        if automatic:
            candidate = publication_candidate(root)
            state = dict(candidate, status="validating", time=time.time(), peer=peer["id"])
            save_json(state_path, state)
            try:
                raw, candidate, count = validate_publication(root, config, peer, transport, expected_candidate)
                state.update(status="publishing", tests=count)
                save_json(state_path, state)
            except (PeerError, OSError, subprocess.TimeoutExpired, KeyboardInterrupt) as error:
                state.update(status="source_changed" if isinstance(error, PublicationChanged) else "validation_failed", reason=type(error).__name__)
                save_json(state_path, state)
                raise
        else:
            raw = build_bundle(root)
        try:
            manifest = _publish_bundle(root, config, peer, transport, target, raw, expected_candidate if automatic else None)
        except (PeerError, OSError, KeyboardInterrupt) as error:
            if state is not None:
                state.update(status="delivery_unknown" if isinstance(error, (DeliveryUnknown, KeyboardInterrupt, OSError)) else "publication_failed", reason=type(error).__name__)
                save_json(state_path, state)
            raise
        if state is not None:
            state.update(status="published", time=time.time(), manifest_id=manifest["id"])
            save_json(state_path, state)
        return manifest


def _publish_bundle(root, config, peer, transport, target, raw, auto_candidate=None):
    settings = policy(root)
    if not settings.get("enabled") or settings.get("publisher") != peer["id"]:
        raise PeerError("Enable automatic updates locally with this peer as publisher first")
    encoded = base64.b64encode(raw).decode("ascii")
    chunks = [encoded[i:i + CHUNK_CHARS] for i in range(0, len(encoded), CHUNK_CHARS)]
    common = {"id": uuid.uuid4().hex[:12], "source": peer["id"], "target": target}
    previous = safe_path(root, ".local/update-release.json")
    last_revision = json.loads(previous.read_text(encoding="utf-8"))["manifest"]["revision"] if previous.exists() else 0
    revision = max(int(time.time() * 1000), last_revision + 1, settings.get("last_revision", 0) + 1, settings.get("rejected_revision", 0) + 1)
    begin = dict(common, kind="begin", revision=revision, sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw), chunks=len(chunks))
    if auto_candidate:
        begin["auto_candidate"] = auto_candidate
    begin["content_sha256"] = content_digest(raw)
    write_atomic(safe_path(root, ".local/published-updates/" + common["id"] + "/package.zip"), raw)
    # Preserve attempts before sending. Unknown delivery stops this publication;
    # partial packages cannot be committed or executed by the receiver.
    ledger = safe_path(root, ".local/update-publication.json")
    recipients = [p["id"] for p in config["peers"] if p["id"] != peer["id"] and target in ("ALL", p["id"])]
    fields = dict(source=peer["id"], target=target, recipients=recipients, update=common["id"])
    record = {"manifest": begin, "recipients": recipients, "frames_sent": 0, "status": "sending", "started_at": time.time()}
    save_json(ledger, record)
    messages = [begin] + [dict(common, kind="chunk", index=i, data=chunk) for i, chunk in enumerate(chunks)] + [dict(common, kind="commit")]
    emit_update("update_transfer_started", **fields)
    try:
        for message in messages:
            current = policy(root)
            if not current.get("enabled") or current.get("publisher") != peer["id"] or auto_candidate and current.get("auto_publish", True) is False:
                raise PeerError("Connector publication disabled locally")
            for attempt in range(config.get("max_retries", 3) + 1):
                try:
                    transport.send(frame(message))
                    break
                except SendRejected as error:
                    if not error.retryable or attempt >= config.get("max_retries", 3):
                        raise
                    time.sleep(retry_delay(config, attempt + 1))
            record["frames_sent"] += 1
            save_json(ledger, record)
            print(json.dumps({"event": "update_uploading", "update": common["id"], "frames_sent": record["frames_sent"], "frames_total": len(messages)}), flush=True)
            if record["frames_sent"] < len(messages):
                time.sleep(config.get("update_frame_interval_seconds", 4))
    except (PeerError, OSError, KeyboardInterrupt):
        record["status"] = "incomplete_do_not_blindly_replay"
        save_json(ledger, record)
        raise
    record["status"] = "published_waiting_for_receivers"
    record["finished_at"] = time.time()
    save_json(ledger, record)
    # Only a completed publication becomes the startup update source.
    save_json(safe_path(root, ".local/update-release.json"), {"group_id": config["group_id"], "manifest": begin})
    emit_update("update_transfer_sent", **fields)
    return begin


class AutoPublisher:
    """Supervisor-owned worker; a peer restart never interrupts its publication."""
    def __init__(self, root, config_path, peer_id, emit, quiet_seconds=30, poll_seconds=5, min_interval=300):
        self.root, self.config_path, self.peer_id = Path(root).resolve(), str(config_path), peer_id
        self.emit = emit
        self.quiet_seconds, self.poll_seconds, self.min_interval = quiet_seconds, poll_seconds, min_interval
        self.candidate, self.since, self.next_poll, self.process = None, 0, 0, None
        self.progress, self.last_error, self.active_candidate = None, None, None
        self.state_path = safe_path(self.root, ".local/auto-publication.json")

    def _log(self, event, **fields):
        self.emit(event, peer=self.peer_id, **fields)

    def _state(self):
        return json.loads(self.state_path.read_text(encoding="utf-8")) if self.state_path.exists() else {}

    def _report_transfer(self, completed=False):
        # Automatic workers have no console. Relay durable progress to this one.
        ledger = safe_path(self.root, ".local/update-publication.json")
        if not ledger.exists():
            return
        progress = json.loads(ledger.read_text(encoding="utf-8"))
        manifest = progress.get("manifest", {})
        if not self.active_candidate or manifest.get("auto_candidate") != self.active_candidate:
            return
        fields = dict(source=manifest["source"], target=manifest["target"], update=manifest["id"], recipients=progress.get("recipients", []))
        if self.progress is None:
            self._log("update_transfer_started", **fields)
        sent = progress.get("frames_sent", 0)
        if sent != self.progress:
            self.progress = sent
            self._log("auto_publish_progress", frames_sent=sent)
        if completed and progress.get("status") == "published_waiting_for_receivers":
            self._log("update_transfer_sent", **fields)

    def tick(self, ready=True, now=None):
        now = time.monotonic() if now is None else now
        if self.process is not None:
            code = self.process.poll()
            if code is None:
                self._report_transfer()
                return
            self.process.wait()
            self._report_transfer(completed=True)
            self.process = None
            state = self._state()
            if code == 73:  # Another publisher owns the lock; no message sent.
                state.update(status="busy", time=time.time())
                save_json(self.state_path, state)
                self.since, self.next_poll = now, now + self.poll_seconds
            elif code == 74:
                self.candidate = None
            elif state.get("status") not in ("published", "validation_failed", "publication_failed", "delivery_unknown", "source_changed"):
                state.update(status="publication_failed", time=time.time())
                save_json(self.state_path, state)
            self._log("auto_publish_completed" if code == 0 and state.get("status") == "published" else "auto_publish_stopped", exit_code=code)
        if not ready or now < self.next_poll:
            return
        self.next_poll = now + self.poll_seconds
        settings = policy(self.root)
        if not settings.get("enabled") or settings.get("publisher") != self.peer_id or settings.get("auto_publish", True) is False:
            return
        try:
            candidate = publication_candidate(self.root)
            release = published_release(self.root, json.loads(Path(self.config_path).read_text(encoding="utf-8-sig")), self.peer_id)
            if release and release[0]["target"] == "ALL" and release[0]["content_sha256"] == candidate["content_sha256"]:
                return
            state = self._state()
            if state.get("content_sha256") == candidate["content_sha256"] and state.get("status") in ("publishing", "delivery_unknown", "publication_failed"):
                return
            if state.get("candidate") == candidate["candidate"] and state.get("status") in ("queued", "validating", "validation_failed"):
                return
            failed = safe_path(self.root, ".local/update-failed.json")
            if failed.exists() and json.loads(failed.read_text(encoding="utf-8")).get("content_sha256") == candidate["content_sha256"]:
                return
            if self.candidate != candidate["candidate"]:
                self.candidate, self.since = candidate["candidate"], now
                self._log("auto_publish_change_detected", quiet_seconds=self.quiet_seconds)
                return
            if now - self.since < self.quiet_seconds:
                return
            ledger = safe_path(self.root, ".local/update-publication.json")
            if ledger.exists():
                previous = json.loads(ledger.read_text(encoding="utf-8"))
                if previous.get("status") == "incomplete_do_not_blindly_replay" and previous.get("manifest", {}).get("content_sha256") == candidate["content_sha256"]:
                    return
                published_at = previous.get("finished_at", previous.get("started_at", previous.get("manifest", {}).get("revision", 0) / 1000))
                if time.time() - published_at < self.min_interval:
                    return
            state = dict(candidate, status="queued", peer=self.peer_id, time=time.time())
            save_json(self.state_path, state)
            command = [sys.executable, str(self.root / "peer.py"), "--config", self.config_path, "--peer", self.peer_id,
                       "publish-update", "--automatic", "--expected-candidate", candidate["candidate"]]
            try:
                self.process = subprocess.Popen(command, cwd=self.root, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
                                                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0)
            except OSError:
                state.update(status="publication_failed")
                save_json(self.state_path, state)
                raise
            self.progress, self.active_candidate = None, candidate["candidate"]
            self._log("auto_publish_started")
            self.last_error = None
        except (PeerError, OSError, ValueError, KeyError) as error:
            reason = type(error).__name__
            if reason != self.last_error:
                self._log("auto_publish_unavailable", reason=reason)
                self.last_error = reason

    def close(self):
        if self.process is not None:
            stop_process(self.process)
            self.process.wait()
            self.process = None


def published_release(root, config, publisher):
    path = safe_path(root, ".local/update-release.json")
    if not path.exists():
        return None
    release = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(release, dict) or not isinstance(release.get("manifest"), dict):
        raise PeerError("Invalid published connector release")
    manifest = release.get("manifest", {})
    if (release.get("group_id") != config["group_id"] or manifest.get("source") != publisher
            or not re.fullmatch(r"[0-9a-f]{12}", str(manifest.get("id", "")))
            or type(manifest.get("revision")) is not int or manifest["revision"] <= 0
            or manifest.get("kind") != "begin"
            or not isinstance(manifest.get("target"), str)
            or manifest.get("target") not in {"ALL", *(p["id"] for p in config["peers"])}
            or type(manifest.get("bytes")) is not int or not 0 < manifest["bytes"] <= MAX_BYTES
            or type(manifest.get("chunks")) is not int or not 0 < manifest["chunks"] <= 64):
        raise PeerError("Invalid published connector release")
    if manifest["revision"] <= policy(root).get("rejected_revision", 0):
        return None
    raw = safe_path(root, ".local/published-updates/" + manifest["id"] + "/package.zip").read_bytes()
    if len(raw) != manifest.get("bytes") or hashlib.sha256(raw).hexdigest() != manifest.get("sha256"):
        raise PeerError("Published connector release changed")
    if content_digest(raw) != manifest.get("content_sha256"):
        raise PeerError("Published connector content changed")
    return manifest, raw


def release_frames(manifest, raw):
    common = {k: manifest[k] for k in ("id", "source", "target")}
    encoded = base64.b64encode(raw).decode("ascii")
    chunks = [encoded[i:i + CHUNK_CHARS] for i in range(0, len(encoded), CHUNK_CHARS)]
    if len(chunks) != manifest["chunks"]:
        raise PeerError("Published connector chunks changed")
    return [manifest] + [dict(common, kind="chunk", index=i, data=chunk) for i, chunk in enumerate(chunks)] + [dict(common, kind="commit")]


class StartupUpdater:
    """One bounded startup check; serve immutable, completed releases only."""

    def __init__(self, root, config, peer_id, transport, emit):
        self.root, self.config, self.peer_id = Path(root).resolve(), config, peer_id
        self.transport, self.emit = transport, emit
        self.jobs, self.results = queue.Queue(maxsize=8), queue.Queue()
        self.stopping = threading.Event()
        self.started, self.worker = False, None
        self.handled, self.requested = {}, {}
        self.wait_until, self.waiting = 0, False
        self.digest, self.unavailable = None, False

    def _log(self, event, **fields):
        self.emit(event, peer=self.peer_id, **fields)

    def _queue(self, function, *args):
        try:
            self.jobs.put_nowait((function, args))
        except queue.Full:
            self._log("update_check_busy")

    def _work(self):
        while not self.stopping.is_set():
            try:
                function, args = self.jobs.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                function(*args)
            except (PeerError, OSError, ValueError) as error:
                self.results.put(("update_check_failed", {"reason": type(error).__name__}))
            finally:
                self.jobs.task_done()

    def _send(self, messages, record_path=None):
        record = {"status": "sending", "frames_sent": 0, "time": time.time()}
        if record_path:
            record["manifest"] = messages[0]
            save_json(record_path, record)
        try:
            for message in messages:
                if self.stopping.is_set():
                    raise PeerError("Startup update transfer stopped")
                if not policy(self.root).get("enabled"):
                    raise PeerError("Automatic updates disabled locally")
                for attempt in range(self.config.get("max_retries", 3) + 1):
                    try:
                        self.transport.send(frame(message))
                        break
                    except SendRejected as error:
                        if not error.retryable or attempt >= self.config.get("max_retries", 3):
                            raise
                        if self.stopping.wait(retry_delay(self.config, attempt + 1)):
                            raise PeerError("Startup update transfer stopped")
                record["frames_sent"] += 1
                if record_path:
                    save_json(record_path, record)
                if record["frames_sent"] < len(messages) and self.stopping.wait(self.config.get("update_frame_interval_seconds", 4)):
                    raise PeerError("Startup update transfer stopped")
        except (PeerError, OSError):
            record["status"] = "incomplete_do_not_blindly_replay"
            if record_path:
                save_json(record_path, record)
            raise
        record.update(status="sent", time=time.time())
        if record_path:
            save_json(record_path, record)

    def _release(self):
        return published_release(self.root, self.config, self.peer_id)

    def _latest(self, identity, target, release):
        data = {"id": identity, "source": self.peer_id, "target": target, "kind": "latest", "available": bool(release)}
        if release:
            manifest, _ = release
            data.update(release=manifest["id"], sha256=manifest["sha256"], content_sha256=manifest["content_sha256"], revision=manifest["revision"])
        return data

    def _announce(self):
        release = self._release()
        if release:
            self._send([self._latest(uuid.uuid4().hex[:12], release[0]["target"], release)])
            self.results.put(("update_release_announced", {}))
        else:
            self.results.put(("update_no_published_release", {}))

    def _answer(self, request):
        release = self._release()
        if release and release[0]["target"] not in ("ALL", request["source"]):
            release = None
        latest = self._latest(request["id"], request["source"], release)
        self._send([latest])
        if not release:
            return
        manifest, raw = release
        if (request["content_sha256"] == manifest["content_sha256"] or manifest["revision"] <= request["minimum_revision"]
                or manifest["target"] not in ("ALL", request["source"])):
            return
        with PublicationLock(self.root):
            self._transfer(manifest, raw, request["source"])

    def _transfer(self, manifest, raw, target):
        key = hashlib.sha256((self.config["group_id"] + target).encode()).hexdigest()[:16]
        ledger = safe_path(self.root, ".local/update-sync/" + key + "-" + manifest["id"] + ".json")
        if ledger.exists():
            previous = json.loads(ledger.read_text(encoding="utf-8"))
            if previous.get("status") != "sent" or time.time() - previous.get("time", 0) < 300:
                self.results.put(("update_transfer_withheld", {"release": manifest["id"]}))
                return
        # Address the replay only to the requesting computer. Reusing an ALL
        # envelope could overwrite the publisher's newer, unpublished edits.
        transfer = dict(manifest, id=uuid.uuid4().hex[:12], target=target)
        fields = {"release": manifest["id"], "update": transfer["id"], "target": target}
        self.results.put(("update_transfer_started", fields))
        self._send(release_frames(transfer, raw), ledger)
        self.results.put(("update_transfer_sent", fields))

    def _request(self, settings):
        identity = uuid.uuid4().hex[:12]
        message = {"id": identity, "source": self.peer_id, "target": settings["publisher"], "kind": "check",
                   "content_sha256": self.digest, "minimum_revision": max(settings.get("last_revision", 0), settings.get("rejected_revision", 0))}
        self.waiting, self.wait_until = True, time.time() + 180
        self._queue(self._send, [message])
        self._log("update_check_started")

    def tick(self, history_ready=True):
        while not self.results.empty():
            event, fields = self.results.get_nowait()
            self._log(event, **fields)
        settings = policy(self.root)
        if self.unavailable:
            return
        if not settings.get("enabled"):
            if not self.started:
                self._log("update_check_disabled")
                self.started = True
            return
        if self.worker is None:
            try:
                self.digest = content_digest(build_bundle(self.root))
            except (PeerError, OSError, ValueError) as error:
                self.unavailable = True
                self._log("update_check_failed", reason=type(error).__name__)
                return
            self.worker = threading.Thread(target=self._work, daemon=True)
            self.worker.start()
            # Defer the check until complete history has been processed first.
            self.started = False
        if not self.started and history_ready:
            self.started = True
            if settings["publisher"] == self.peer_id:
                self._queue(self._announce)
            else:
                self._request(settings)
        if self.waiting and time.time() >= self.wait_until:
            self.waiting = False
            self._log("update_check_unavailable", reason="publisher_unreachable_keep_current_version")

    def handle(self, event, recovered=False):
        if recovered or self.worker is None:
            return
        data, settings = trusted_frame(self.config, event), policy(self.root)
        if not data or not settings.get("enabled") or data.get("target") not in (self.peer_id, "ALL"):
            return
        if data.get("kind") == "check" and self.peer_id == settings["publisher"] and data["source"] != self.peer_id:
            if (data["id"] in self.handled
                    or not re.fullmatch(r"[0-9a-f]{64}", str(data.get("content_sha256", "")))
                    or type(data.get("minimum_revision")) is not int or data["minimum_revision"] < 0):
                return
            if len(self.handled) >= 128:
                self.handled.pop(next(iter(self.handled)))
            self.handled[data["id"]] = True
            self._queue(self._answer, data)
        elif (self.started and data.get("kind") == "latest" and data["source"] == settings["publisher"]
              and data["source"] != self.peer_id):
            if data.get("available") is False:
                self.waiting = False
                self._log("update_no_published_release")
                return
            if (data.get("available") is not True or type(data.get("revision")) is not int
                    or not re.fullmatch(r"[0-9a-f]{64}", str(data.get("content_sha256", "")))):
                return
            self.waiting = False
            if data["content_sha256"] == self.digest:
                self._log("update_already_current")
            elif data["revision"] <= max(settings.get("last_revision", 0), settings.get("rejected_revision", 0)):
                self._log("update_revision_withheld")
            elif data["target"] == "ALL" and data["id"] not in self.requested:
                if len(self.requested) >= 128:
                    self.requested.pop(next(iter(self.requested)))
                self.requested[data["id"]] = True
                self._request(settings)
            else:
                self._log("update_available", release=data.get("release"))

    def close(self):
        self.stopping.set()


class UpdateReceiver:
    def __init__(self, root, config, peer_id):
        self.root, self.config, self.peer_id = Path(root).resolve(), config, peer_id

    def receive(self, event):
        data = trusted_frame(self.config, event)
        settings = policy(self.root)
        if not data or not settings.get("enabled") or data.get("source") != settings.get("publisher") or data.get("target") not in ("ALL", self.peer_id):
            return False
        if data.get("kind") not in ("begin", "chunk", "commit"):
            return False
        folder = safe_path(self.root, ".local/updates/" + data["id"])
        manifest_path = safe_path(self.root, ".local/updates/" + data["id"] + "/manifest.json")
        if data["kind"] == "begin":
            if type(data.get("revision")) is not int or data["revision"] <= max(settings.get("last_revision", 0), settings.get("rejected_revision", 0)):
                return False
            if type(data.get("bytes")) is not int or not 0 < data["bytes"] <= MAX_BYTES or type(data.get("chunks")) is not int or not 0 < data["chunks"] <= 64 or not re.fullmatch(r"[0-9a-f]{64}", str(data.get("sha256", ""))):
                raise PeerError("Invalid update manifest")
            if manifest_path.exists() and json.loads(manifest_path.read_text(encoding="utf-8")) != data:
                raise PeerError("Conflicting update manifest")
            save_json(manifest_path, data)
            return False
        if not manifest_path.exists():
            return False
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest["revision"] <= max(settings.get("last_revision", 0), settings.get("rejected_revision", 0)):
            return False
        if data["kind"] == "chunk":
            index = data.get("index")
            if type(index) is not int or not 0 <= index < manifest["chunks"] or not isinstance(data.get("data"), str) or len(data["data"]) > CHUNK_CHARS:
                raise PeerError("Invalid update chunk")
            try:
                raw = base64.b64decode(data["data"], validate=True)
            except ValueError:
                raise PeerError("Invalid update chunk encoding") from None
            part = safe_path(folder, f"chunk-{index}")
            if part.exists() and part.read_bytes() != raw:
                raise PeerError("Conflicting update chunk")
            write_atomic(part, raw)
            if safe_path(folder, "commit-seen").exists():
                return self.receive(dict(event, content=frame({"id": data["id"], "source": data["source"], "target": data["target"], "kind": "commit"})))
            return False
        write_atomic(safe_path(folder, "commit-seen"), b"")
        parts = [safe_path(folder, f"chunk-{i}") for i in range(manifest["chunks"])]
        if not all(part.is_file() for part in parts):
            return False
        raw = b"".join(part.read_bytes() for part in parts)
        if len(raw) != manifest["bytes"] or hashlib.sha256(raw).hexdigest() != manifest["sha256"]:
            raise PeerError("Update package digest mismatch")
        inspect_bundle(raw)
        if self.peer_id == manifest["source"] and manifest.get("content_sha256"):
            try:
                current = content_digest(build_bundle(self.root))
            except (PeerError, OSError):
                current = None
            if current != manifest["content_sha256"]:
                save_json(safe_path(self.root, ".local/update-self-deferred.json"), dict(manifest, reason="publisher_source_changed"))
                return False
        pending = safe_path(self.root, ".local/update-pending.json")
        if pending.exists() and json.loads(pending.read_text(encoding="utf-8"))["revision"] > manifest["revision"]:
            return False
        write_atomic(safe_path(folder, "package.zip"), raw)
        save_json(pending, manifest)
        return True


def apply_pending(root, peer_id=None):
    root = Path(root).resolve()
    pending = safe_path(root, ".local/update-pending.json")
    if not pending.exists():
        return None
    manifest = json.loads(pending.read_text(encoding="utf-8"))
    settings = policy(root)
    if not settings.get("enabled") or manifest["source"] != settings.get("publisher") or manifest["revision"] <= max(settings.get("last_revision", 0), settings.get("rejected_revision", 0)):
        return None
    if not re.fullmatch(r"[0-9a-f]{12}", str(manifest.get("id", ""))):
        raise PeerError("Invalid pending update identity")
    raw = safe_path(root, ".local/updates/" + manifest["id"] + "/package.zip").read_bytes()
    if hashlib.sha256(raw).hexdigest() != manifest["sha256"]:
        raise PeerError("Staged update changed")
    files = inspect_bundle(raw)
    backup = safe_path(root, ".local/update-backups/" + manifest["id"])
    baseline_path = safe_path(root, ".local/stable-version.json")
    baseline = json.loads(baseline_path.read_text(encoding="utf-8")) if baseline_path.exists() else None
    if baseline and not re.fullmatch(r"[0-9a-f]{12}", str(baseline.get("id", ""))):
        raise PeerError("Invalid stable connector snapshot")
    old = {}
    for name in files:
        path = safe_path(root, name)
        old[name] = name in baseline["files"] if baseline else path.exists()
        if old[name]:
            if baseline:
                raw_old = safe_path(root, ".local/stable-versions/" + baseline["id"] + "/" + name).read_bytes()
                if hashlib.sha256(raw_old).hexdigest() != baseline["files"][name]:
                    raise PeerError("Stable connector snapshot changed")
            else:
                raw_old = path.read_bytes()
            write_atomic(safe_path(backup, name), raw_old)
    save_json(safe_path(backup, "rollback.json"), old)
    manifest["backup"] = manifest["id"]
    save_json(safe_path(root, ".local/update-installing.json"), manifest)
    try:
        if peer_id is not None:
            emit_update("update_install_started", peer=peer_id, source=manifest["source"], update=manifest["id"])
        for name, content in files.items():
            write_atomic(safe_path(root, name), content)
    except OSError:
        rollback(root, manifest)
        raise
    return manifest


def rollback(root, manifest):
    if not re.fullmatch(r"[0-9a-f]{12}", str(manifest.get("id", ""))):
        raise PeerError("Invalid rollback identity")
    backup = safe_path(root, ".local/update-backups/" + manifest["id"])
    old = json.loads(safe_path(backup, "rollback.json").read_text(encoding="utf-8"))
    for name, existed in old.items():
        if name not in ALLOWED:
            raise PeerError("Invalid rollback entry")
        target = safe_path(root, name)
        if existed:
            write_atomic(target, safe_path(backup, name).read_bytes())
        elif target.exists():
            target.unlink()
    failed = safe_path(root, ".local/update-failed.json")
    save_json(failed, manifest)
    settings = policy(root)
    settings["rejected_revision"] = max(settings.get("rejected_revision", 0), manifest["revision"])
    save_json(safe_path(root, ".local/update-policy.json"), settings)
    pending = safe_path(root, ".local/update-pending.json")
    if pending.exists():
        pending.unlink()
    journal = safe_path(root, ".local/update-installing.json")
    if journal.exists():
        journal.unlink()


def mark_ready(root, manifest, peer_id):
    settings = policy(root)
    settings["last_revision"] = manifest["revision"]
    save_json(safe_path(root, ".local/update-policy.json"), settings)
    save_json(safe_path(root, ".local/update-receipt.json"), {"id": manifest["id"], "source": peer_id, "target": "ALL", "kind": "ready", "sha256": manifest["sha256"]})
    pending = safe_path(root, ".local/update-pending.json")
    if pending.exists():
        pending.unlink()
    journal = safe_path(root, ".local/update-installing.json")
    if journal.exists():
        journal.unlink()


def recover_install(root):
    journal = safe_path(root, ".local/update-installing.json")
    if not journal.exists():
        return
    manifest = json.loads(journal.read_text(encoding="utf-8"))
    if policy(root).get("last_revision", 0) < manifest["revision"]:
        rollback(root, manifest)
    else:
        journal.unlink()
        pending = safe_path(root, ".local/update-pending.json")
        if pending.exists():
            pending.unlink()


def stop_process(process):
    """Stop only our owned child; never terminate or kill an unresponsive process."""
    if process.poll() is not None:
        return
    try:
        process.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGINT)
    except OSError:
        raise PeerError("Cannot signal owned child gracefully; update/restart withheld") from None
    try:
        process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        raise PeerError("Owned child did not stop gracefully; update/restart withheld") from None
