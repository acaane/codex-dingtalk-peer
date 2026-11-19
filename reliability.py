"""Durable work checkpoints; no networking or business-project access."""
import json
import errno
import os
from pathlib import Path
import re
import sqlite3
import time


class PeerError(Exception):
    pass


class PeerAlreadyRunning(PeerError):
    """An OS lock is held by another connector; this is not a startup failure."""


def _lock_file(handle):
    handle.seek(0)
    if os.name == "nt":
        import msvcrt
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        import fcntl
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)


def acquire_instance_lock(path, owner=None):
    """Keep the OS lock until close; stale PID text never blocks a new owner."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b")
    try:
        if path.stat().st_size == 0:
            handle.write(b"0")
            handle.flush()
        try:
            _lock_file(handle)
        except OSError as error:
            if error.errno in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                raise PeerAlreadyRunning("连接器已有实例运行；不会重复启动。") from None
            raise
        if owner is not None:
            # Metadata begins after the locked byte; a reader can inspect it on Windows.
            handle.truncate(1)
            handle.write(json.dumps(dict(owner, pid=os.getpid()), ensure_ascii=False).encode("utf-8"))
            handle.flush()
        return handle
    except BaseException:
        handle.close()
        raise


def instance_lock_owner(path):
    """Return metadata only when an actual lock is held, never from PID text alone."""
    path = Path(path)
    if not path.exists():
        return None
    with path.open("r+b") as handle:
        try:
            _lock_file(handle)
        except OSError as error:
            if error.errno not in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                raise
            try:
                handle.seek(1)
                owner = json.loads(handle.read(4096).decode("utf-8"))
            except (ValueError, UnicodeError):
                owner = {}
            return owner if isinstance(owner, dict) else {}
    return None


class RetryablePeerError(PeerError):
    """A read-only model call can safely be repeated."""


class DeliveryUnknown(PeerError):
    """A message may have been accepted; never resend automatically."""


class SendRejected(PeerError):
    def __init__(self, message, retryable=False):
        super().__init__(message)
        self.retryable = retryable


def transient_model_error(text):
    text = text.lower()
    if re.search(r"\b(?:401|403)\b|unauthorized|authentication|invalid.api.key|permission.denied", text):
        return False
    return bool(re.search(r"\b(?:429|502|503|504)\b|rate.limit|capacity|overload|temporar.*unavailable|connection.reset|timed.out|connection.refused|network.*unreachable", text))


def retry_delay(config, attempt):
    return min(config.get("retry_max_delay_seconds", 300), config.get("retry_initial_delay_seconds", 15) * 2 ** min(max(attempt - 1, 0), 10))


class Store:
    def __init__(self, path):
        self.db = sqlite3.connect(path, timeout=15)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS inbox (message_id TEXT PRIMARY KEY, task TEXT);
        CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, goal TEXT, step INTEGER, status TEXT, created REAL, history TEXT);
        CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY, task TEXT, step INTEGER, status TEXT);
        CREATE TABLE IF NOT EXISTS work (
            task TEXT PRIMARY KEY, incoming TEXT, wire TEXT, phase TEXT,
            attempts INTEGER, next_at REAL, error TEXT);
        CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS continuations (task TEXT PRIMARY KEY, parent TEXT);
        CREATE TABLE IF NOT EXISTS task_reports (
            id TEXT PRIMARY KEY, task TEXT, reason TEXT, wire TEXT, status TEXT,
            attempts INTEGER, next_at REAL, error TEXT, created REAL);
        """)
        self.db.commit()

    def get(self, task):
        row = self.db.execute("SELECT * FROM tasks WHERE id=?", (task,)).fetchone()
        return dict(row) if row else None

    def accept(self, event_id, message, schedule=False):
        if self.db.execute("SELECT 1 FROM inbox WHERE message_id=?", (event_id,)).fetchone():
            return False
        row = self.get(message["task"])
        if row and (row["status"] not in ("active", "waiting_peer", "retry_wait", "delivery_unknown") or row["goal"] != message["goal"] or message["step"] <= row["step"]):
            return False
        history = json.loads(row["history"]) if row else []
        history.append({"from": message["source"], "text": message["body"]})
        with self.db:
            self.db.execute("INSERT INTO inbox VALUES (?,?)", (event_id, message["task"]))
            if not row:
                self.db.execute("INSERT INTO tasks VALUES (?,?,?,?,?,?)", (message["task"], message["goal"], 0, "active", time.time(), "[]"))
            self.db.execute("UPDATE tasks SET step=?,status='active',history=? WHERE id=?", (message["step"], json.dumps(history[-12:], ensure_ascii=False), message["task"]))
            # A newer trusted reply is also evidence that an earlier outgoing turn arrived.
            self.db.execute("UPDATE outbox SET status='sent' WHERE task=? AND step<? AND status IN ('pending','unknown')", (message["task"], message["step"]))
            self.db.execute("DELETE FROM work WHERE task=?", (message["task"],))
            if schedule:
                self.db.execute("INSERT INTO work VALUES (?,?,?,'agent',0,0,'')", (message["task"], json.dumps(message, ensure_ascii=False), None))
        return True

    def observe_own_request(self, message):
        if self.get(message["task"]) is None:
            with self.db:
                self.db.execute("INSERT INTO tasks VALUES (?,?,?,?,?,?)", (message["task"], message["goal"], message["step"], "active", time.time(), "[]"))
            self.append(message["task"], message["source"], message["body"])

    def append(self, task, source, body):
        history = json.loads(self.get(task)["history"])
        history.append({"from": source, "text": body})
        with self.db:
            self.db.execute("UPDATE tasks SET history=? WHERE id=?", (json.dumps(history[-12:], ensure_ascii=False), task))

    def close(self, task, status):
        with self.db:
            if self.get(task) is None:
                self.db.execute("INSERT INTO tasks VALUES (?,?,?,?,?,?)", (task, "停止任务", 0, status, time.time(), "[]"))
            self.db.execute("UPDATE tasks SET status=? WHERE id=?", (status, task))
            self.db.execute("DELETE FROM work WHERE task=?", (task,))

    def send_pending(self, task, step):
        with self.db:
            cur = self.db.execute("INSERT INTO outbox(task,step,status) VALUES (?,?,'pending')", (task, step))
        return cur.lastrowid

    def meta(self, key, default=None):
        row = self.db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_meta(self, key, value):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO metadata VALUES (?,?)", (key, json.dumps(value, ensure_ascii=False)))

    def work(self, task):
        row = self.db.execute("SELECT * FROM work WHERE task=?", (task,)).fetchone()
        return dict(row) if row else None

    def due(self, now):
        row = self.db.execute("SELECT task FROM work WHERE phase IN ('agent','send') AND next_at<=? ORDER BY next_at, rowid LIMIT 1", (now,)).fetchone()
        return row[0] if row else None

    def queue_outgoing(self, message, wire, parent=None):
        with self.db:
            self.db.execute("INSERT INTO tasks VALUES (?,?,?,?,?,?)", (message["task"], message["goal"], 0, "active", time.time(), "[]"))
            self.db.execute("INSERT INTO work VALUES (?,NULL,?,'send',0,0,'')", (message["task"], wire))
            if parent:
                self.db.execute("INSERT INTO continuations VALUES (?,?)", (message["task"], parent))

    def cache_wire(self, task, wire):
        with self.db:
            self.db.execute("UPDATE work SET wire=?,phase='send',attempts=0,next_at=0,error='' WHERE task=?", (wire, task))

    def begin(self, task, phase):
        with self.db:
            self.db.execute("UPDATE work SET phase=?,attempts=attempts+1 WHERE task=?", (phase, task))
        return self.work(task)["attempts"]

    def retry(self, task, phase, error, config, now):
        work = self.work(task)
        if work["attempts"] > config.get("max_retries", 3):
            self.set_meta("task_error:" + task, {"reason": error, "attempts": work["attempts"], "phase": phase})
            self.close(task, "retry_exhausted")
            return None
        next_at = now + retry_delay(config, work["attempts"])
        with self.db:
            self.db.execute("UPDATE work SET phase=?,next_at=?,error=? WHERE task=?", (phase, next_at, error, task))
            self.db.execute("UPDATE tasks SET status='retry_wait' WHERE id=?", (task,))
        return next_at

    def unknown(self, task, error):
        with self.db:
            self.db.execute("UPDATE work SET phase='unknown',error=? WHERE task=?", (error, task))
            self.db.execute("UPDATE outbox SET status='unknown' WHERE task=? AND status='pending'", (task,))
            self.db.execute("UPDATE tasks SET status='delivery_unknown' WHERE id=?", (task,))

    def sent(self, message):
        task = message["task"]
        row = self.get(task)
        if not row or row["status"] in ("stopped", "done") or row["step"] >= message["step"]:
            return
        history = json.loads(row["history"])
        history.append({"from": message["source"], "text": message["body"]})
        with self.db:
            self.db.execute("UPDATE tasks SET step=?,status=?,history=? WHERE id=?", (message["step"], "done" if message["kind"] == "done" else "active", json.dumps(history[-12:], ensure_ascii=False), task))
            self.db.execute("UPDATE outbox SET status='sent' WHERE task=? AND step=?", (task, message["step"]))
            self.db.execute("DELETE FROM work WHERE task=?", (task,))

    def recover(self, config, now):
        # Only call under the listener's exclusive peer lock, never from a concurrent CLI.
        for row in self.db.execute("SELECT * FROM work WHERE phase IN ('agent_running','sending')").fetchall():
            if row["phase"] == "sending":
                self.unknown(row["task"], "Interrupted while sending; awaiting history evidence")
            else:
                self.retry(row["task"], "agent", "Interrupted model call", config, now)
        with self.db:
            self.db.execute("UPDATE outbox SET status='unknown' WHERE status='pending'")

    def mark_waiting(self, age):
        rows = self.db.execute("SELECT id FROM tasks WHERE status='active' AND created<? AND NOT EXISTS (SELECT 1 FROM work WHERE work.task=tasks.id)", (time.time() - age,)).fetchall()
        with self.db:
            for row in rows:
                self.db.execute("UPDATE tasks SET status='waiting_peer' WHERE id=?", (row[0],))
        return [row[0] for row in rows]

    def latest(self):
        row = self.db.execute("SELECT id FROM tasks ORDER BY created DESC, rowid DESC LIMIT 1").fetchone()
        return self.get(row[0]) if row else None


def continuation_body(parent, budget):
    prefix = f"继续上次任务 {parent['id']}（上次状态：{parent['status']}）。以下是已保存进度；先复核待办和当前文件，再继续，只读边界不变。\n"
    if parent.get("last_error"):
        prefix += "上次失败原因：" + str(parent["last_error"])[0:160] + "\n"
    kept = []
    for item in reversed(json.loads(parent["history"])):
        candidate = [item] + kept
        if len(prefix + json.dumps(candidate, ensure_ascii=False)) > budget:
            if not kept:
                kept = [{"from": item["from"], "text": item["text"][:max(0, budget - len(prefix) - 100)]}]
            break
        kept = candidate
    return prefix + json.dumps(kept, ensure_ascii=False)
