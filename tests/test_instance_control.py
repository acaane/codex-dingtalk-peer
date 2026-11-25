"""Real instance-lock and duplicate-launch coverage; no account/network access."""
from contextlib import contextmanager, redirect_stdout
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

import peer
import setup_helper
import supervisor
from reliability import PeerAlreadyRunning, acquire_instance_lock, instance_lock_owner


@contextmanager
def locked_by_child(path):
    script = ('import sys; from reliability import acquire_instance_lock; '
              'lock=acquire_instance_lock(sys.argv[1], {"peer":"A"}); '
              'print("locked",flush=True); sys.stdin.readline(); lock.close()')
    child = subprocess.Popen([sys.executable, '-X', 'utf8', '-c', script, str(path)],
                             cwd=peer.ROOT, stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
    try:
        if child.stdout.readline().strip() != 'locked':
            raise AssertionError('Isolated lock holder failed to start')
        yield child
    finally:
        child.communicate('\n', timeout=10)


class InstanceControlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='AI instance 中文 ')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.runtime = self.root/'runtime/A'
        self.runtime.mkdir(parents=True)

    def test_real_other_process_excludes_duplicate_and_release_ignores_stale_pid(self):
        path = self.runtime/'run.lock'
        with locked_by_child(path) as child:
            self.assertEqual(instance_lock_owner(path)['pid'], child.pid)
            with self.assertRaises(PeerAlreadyRunning):
                acquire_instance_lock(path, {'peer':'B'})
            self.assertEqual(instance_lock_owner(path)['peer'], 'A')
        self.assertIsNone(instance_lock_owner(path))
        with acquire_instance_lock(path, {'peer':'A'}):
            self.assertEqual(instance_lock_owner(path)['pid'], os.getpid())
        self.assertIsNone(instance_lock_owner(path))

    def test_legacy_one_byte_lock_is_recognized_without_pid_metadata(self):
        with acquire_instance_lock(self.runtime/'run.lock'):
            self.assertEqual(setup_helper.existing_connector(self.root, 'A'), {})
        self.assertIsNone(setup_helper.existing_connector(self.root, 'A'))

    def test_null_stdin_is_not_an_interactive_windows_console(self):
        script = 'import json; from setup_helper import interactive_console; print(json.dumps(interactive_console()))'
        child = subprocess.run([sys.executable,'-c',script], cwd=peer.ROOT,
                               stdin=subprocess.DEVNULL,capture_output=True,text=True,timeout=10)
        self.assertEqual(child.returncode, 0)
        self.assertIs(json.loads(child.stdout), False)

    def test_existing_start_does_not_refresh_identity_or_launch_more_processes(self):
        with acquire_instance_lock(self.runtime/'run.lock', {'peer':'A'}), \
                patch.object(setup_helper, 'ROOT', self.root), \
                patch.object(setup_helper.sys, 'stdin', types.SimpleNamespace(isatty=lambda:False)), \
                patch.object(setup_helper, 'check_config') as check, \
                patch.object(setup_helper, 'run_supervisor') as launch, redirect_stdout(io.StringIO()) as out:
            self.assertEqual(setup_helper.start_connector(self.root/'unused.json', 'A'), 0)
            check.assert_not_called()
            launch.assert_not_called()
            self.assertIn('不是启动失败', out.getvalue())
            self.assertNotIn('This peer is already running', out.getvalue())

    def test_daily_start_repairs_missing_update_policy_and_preserves_existing_choice(self):
        from connector_update import policy, save_json

        cfg = {'send_authorized': True, 'peers': [{'id': 'B'}, {'id': 'A'}]}
        policy_path = self.root / '.local/update-policy.json'
        policy_path.parent.mkdir()
        for existing in (None, {'enabled': False, 'publisher': 'B', 'auto_publish': False}):
            with self.subTest(existing=existing):
                if existing is not None:
                    save_json(policy_path, existing)
                with patch.object(setup_helper, 'ROOT', self.root), \
                        patch.object(setup_helper, 'check_config', return_value=cfg), \
                        patch.object(setup_helper, 'prepare_local_codex'), \
                        patch.object(setup_helper, 'ensure_business_agent', return_value={'state': 'reused'}), \
                        patch.object(setup_helper, 'business_wake_adapter', return_value='cli'), \
                        patch.object(setup_helper, 'run_supervisor', return_value=0):
                    self.assertEqual(setup_helper.start_connector(self.root / 'config.json', 'B'), 0)
                expected = existing or {'enabled': True, 'publisher': 'A', 'auto_publish': True}
                self.assertEqual(policy(self.root), expected)

    def test_viewer_shows_log_and_ctrl_c_keeps_original_lock(self):
        (self.runtime/'connector.log').write_text('ready\n最近一次同步正常\n', encoding='utf-8')
        with acquire_instance_lock(self.runtime/'run.lock', {'peer':'A'}), \
                patch.object(setup_helper.time, 'sleep', side_effect=KeyboardInterrupt), \
                redirect_stdout(io.StringIO()) as out:
            self.assertEqual(setup_helper.follow_existing_connector(self.root, 'A', interactive=True), 0)
            self.assertIn('最近一次同步正常', out.getvalue())
            self.assertIn('原连接器未被本窗口停止', out.getvalue())
            with self.assertRaises(PeerAlreadyRunning):
                acquire_instance_lock(self.runtime/'run.lock')

    def test_managed_legacy_log_is_supported_but_paths_outside_runtime_are_rejected(self):
        log = self.runtime/'managed.stdout.log'
        log.write_text('ready', encoding='utf-8')
        marker = self.runtime/'managed-launcher.json'
        marker.write_text(json.dumps({'stdout':str(log)}), encoding='utf-8')
        self.assertEqual(setup_helper.connector_log(self.root, 'A'), log)
        other = self.root/'private.txt'
        other.write_text('must not read', encoding='utf-8')
        marker.write_text(json.dumps({'stdout':str(other)}), encoding='utf-8')
        self.assertIsNone(setup_helper.connector_log(self.root, 'A'))

    def test_start_race_is_handled_as_existing_instance_without_retrying_launch(self):
        with patch.object(setup_helper, 'follow_existing_connector', side_effect=[None, 0]), \
                patch.object(setup_helper, 'check_config', return_value={}), \
                patch.object(setup_helper, 'prepare_local_codex'), \
                patch.object(setup_helper, 'business_wake_adapter', return_value='cli'), \
                patch.object(setup_helper, 'run_supervisor', return_value=20) as launch, \
                redirect_stdout(io.StringIO()):
            self.assertEqual(setup_helper.start_connector(self.root/'unused.json', 'A'), 0)
            launch.assert_called_once()

    def test_duplicate_supervisor_is_rejected_before_update_recovery(self):
        with locked_by_child(self.runtime/'supervisor.lock'), \
                patch.object(supervisor, 'supervise_owned') as run:
            with self.assertRaises(PeerAlreadyRunning):
                supervisor.supervise(self.root/'unused.json', 'A', self.root)
            run.assert_not_called()

    def test_direct_listener_blocks_supervisor_before_pending_update_is_touched(self):
        with acquire_instance_lock(self.runtime/'run.lock'), \
                patch.object(supervisor, 'supervise_owned') as run:
            with self.assertRaises(PeerAlreadyRunning):
                supervisor.supervise(self.root/'unused.json', 'A', self.root)
            run.assert_not_called()
            self.assertIsNone(instance_lock_owner(self.runtime/'supervisor.lock'))

    def test_duplicate_listener_never_opens_transport_or_task_store(self):
        with acquire_instance_lock(self.runtime/'run.lock'), patch.object(peer,'ROOT',self.root), \
                patch.object(peer,'Store') as store, patch.object(peer,'Transport') as transport:
            with self.assertRaises(PeerAlreadyRunning):
                peer.listen_once({}, {'id':'A'}, '0')
            store.assert_not_called()
            transport.assert_not_called()

    def test_supervisor_journals_output_and_releases_lock_on_failure(self):
        def fail(*args):
            print('中文诊断 ready', flush=True)
            raise RuntimeError('isolated failure')
        with patch.object(supervisor,'supervise_owned',side_effect=fail), redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(RuntimeError, 'isolated failure'):
                supervisor.supervise(self.root/'unused.json','A',self.root)
        self.assertIn('中文诊断 ready', (self.runtime/'connector.log').read_text(encoding='utf-8'))
        self.assertIsNone(instance_lock_owner(self.runtime/'supervisor.lock'))


if __name__ == '__main__':
    unittest.main()
