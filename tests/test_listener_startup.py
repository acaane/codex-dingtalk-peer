import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest.mock import patch

import peer


class ListenerStartupTests(unittest.TestCase):
    def test_unbounded_listener_survives_closed_launcher_input(self):
        with tempfile.TemporaryDirectory(prefix="listener stdin ownership ") as directory:
            root = Path(directory)
            command = [sys.executable, "-u", "-c",
                       'import sys; print("[event] ready",file=sys.stderr,flush=True); sys.stdin.read()']
            transport = types.SimpleNamespace(token="fake-webhook", command=lambda *args: command)
            config = {"group_id": "group", "task_timeout_seconds": 900,
                      "peers": [{"id": "B", "owner_id": "owner-B"}]}
            real_popen = subprocess.Popen
            observed = {}

            def launch(*args, **kwargs):
                # Reproduce a detached launcher whose inherited stdin is EOF.
                kwargs.setdefault("stdin", subprocess.DEVNULL)
                child = real_popen(*args, **kwargs)
                observed["child"] = child
                return child

            def observe_lifetime(child, ready, timeout=40):
                self.assertTrue(ready.wait(3))
                time.sleep(.15)
                observed["alive"] = child.poll() is None
                return False  # End this isolated startup probe without a real session.

            def stop(child):
                if child.stdin is not None:
                    child.stdin.close()
                child.wait(timeout=3)

            with patch.object(peer, "ROOT", root), patch.object(peer, "Transport", return_value=transport), \
                 patch.object(peer.subprocess, "Popen", side_effect=launch), \
                 patch.object(peer, "wait_listener_ready", side_effect=observe_lifetime), \
                 patch.object(peer, "stop_process", side_effect=stop):
                with self.assertRaises(peer.ListenerStartupError):
                    peer.listen_once(config, {"id": "B"}, "0")
            self.assertTrue(observed["alive"], "Launcher EOF must not end the owned listener")
            self.assertEqual(observed["child"].returncode, 0)

    def test_unbounded_listener_stops_cleanly_through_owned_input(self):
        child = subprocess.Popen([sys.executable, "-u", "-c",
                                  'import sys; sys.stdin.read(); print("clean EOF exit",flush=True)'],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 text=True, creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        try:
            peer.stop_listener_process(child, "0")
            self.assertEqual(child.returncode, 0)
            self.assertEqual(child.stdout.read().strip(), "clean EOF exit")
        finally:
            child.stdin.close()
            child.stdout.close()
            child.stderr.close()

    def test_creation_retry_budgets_are_zero_two_one(self):
        for retryable, count in ((False, 1), (True, 3), (None, 2)):
            error = peer.ListenerStartupError("creation failed", retryable=retryable)
            with patch.object(peer, "listen_once", side_effect=error) as listen, patch.object(peer.time, "sleep"):
                self.assertEqual(peer.run_peer({"max_retries": 5}, {"id": "B"}, "0"), 1)
                self.assertEqual(listen.call_count, count)

    def test_unknown_then_retryable_failure_does_not_expand_original_budget(self):
        errors = [peer.ListenerStartupError("unknown"), peer.ListenerStartupError("temporary", True), peer.ListenerStartupError("should not run", True)]
        with patch.object(peer, "listen_once", side_effect=errors) as listen, patch.object(peer.time, "sleep"):
            self.assertEqual(peer.run_peer({"max_retries": 5}, {"id": "B"}, "0"), 1)
            self.assertEqual(listen.call_count, 2)

    def test_protected_subscription_never_retries_even_when_retryable(self):
        error = peer.listener_startup_error([{"error": {"reason": "personal_subscription_attempt_cooldown", "retryable": True}}], "fake-webhook")
        self.assertTrue(error.protected)
        with patch.object(peer, "listen_once", side_effect=error) as listen, patch.object(peer.time, "sleep"):
            self.assertEqual(peer.run_peer({"max_retries": 5}, {"id": "B"}, "0"), 1)
            self.assertEqual(listen.call_count, 1)

    def test_server_retry_after_and_next_retry_at_are_respected(self):
        with patch.object(peer.time, "time", return_value=1000):
            error = peer.listener_startup_error([{"error": {"retryable": True, "retry_after_seconds": 30, "next_retry_at": 1100}}], "fake-webhook")
        with patch.object(peer, "listen_once", side_effect=[error, 130]), patch.object(peer.time, "sleep") as sleep:
            self.assertEqual(peer.run_peer({"max_retries": 3}, {"id": "B"}, "0"), 130)
            self.assertEqual(sleep.call_args.args[0], 100)

    def test_bounded_listener_creation_failure_is_not_retried(self):
        with patch.object(peer, "listen_once", side_effect=peer.ListenerStartupError("failed", True)) as listen:
            self.assertEqual(peer.run_peer({}, {"id": "B"}, "120s"), 1)
            self.assertEqual(listen.call_count, 1)

    def test_dead_child_does_not_wait_entire_readiness_timeout(self):
        child = subprocess.Popen([sys.executable, "-c", "raise SystemExit(5)"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        started = time.monotonic()
        try:
            self.assertFalse(peer.wait_listener_ready(child, threading.Event(), timeout=40))
            self.assertLess(time.monotonic() - started, 5)
        finally:
            child.wait(timeout=5)

    def test_real_failing_listener_saves_redacted_error_and_releases_run_lock(self):
        with tempfile.TemporaryDirectory(prefix="AI bus startup ") as directory:
            root = Path(directory)
            command = [sys.executable, "-c", 'import json; print(json.dumps({"error":{"reason":"bus child exit status 5", "retryable":False, "message":"token=fake-webhook"}})); raise SystemExit(5)']
            transport = types.SimpleNamespace(token="fake-webhook", command=lambda *args: command)
            config = {"group_id": "group", "max_retries": 3, "task_timeout_seconds": 900, "peers": [{"id": "B", "owner_id": "owner-B"}]}
            with patch.object(peer, "ROOT", root), patch.object(peer, "Transport", return_value=transport):
                for _ in range(2):
                    with self.assertRaises(peer.ListenerStartupError) as caught:
                        peer.listen_once(config, {"id": "B"}, "0")
                    self.assertIs(caught.exception.retryable, False)
            output = root / "runtime/B/listener-startup-error.json"
            text = output.read_text(encoding="utf-8")
            self.assertNotIn("fake-webhook", text)
            self.assertIn("exit status 5", text)
            self.assertEqual(json.loads(text)["exit_code"], 5)


if __name__ == "__main__":
    unittest.main()
