"""First-run coordination tests: no accounts, models, network or business actions."""
import json
from pathlib import Path
import tempfile
import time
import types
import unittest
from unittest.mock import Mock, patch
import subprocess

import peer
import setup_helper as setup
from reliability import PeerError, acquire_instance_lock, PeerAlreadyRunning

THREAD = '11111111-1111-4111-8111-111111111111'


class Native:
    def __init__(self):
        self.calls = []
        self.closed = False

    def call(self, method, params, **kwargs):
        self.calls.append((method, params))
        return {'thread': {'id': THREAD}} if method == 'thread/start' else {}

    def send(self, data):
        self.calls.append((data['method'], data['params']))

    def close(self):
        self.closed = True


class AutomaticCoordinatorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='AI auto 中文 ')
        self.addCleanup(self.temp.cleanup)
        base = Path(self.temp.name)
        self.root, self.business = base / 'connector', base / 'business'
        self.root.mkdir(); self.business.mkdir()
        self.config = {'project_id': 'project-one', 'group_id': 'group', 'auto_coordinate': True,
                       'send_authorized': True, 'codex_path': 'existing.exe', 'peers': [
                           {'id': 'A', 'owner_member_id': 'member-A'},
                           {'id': 'B', 'owner_member_id': 'member-B', 'workdir': str(self.business)}]}
        for name, value in [('ROOT', self.root), ('Transport', Mock(return_value=types.SimpleNamespace(token='synthetic')))]:
            p = patch.object(setup, name, value); p.start(); self.addCleanup(p.stop)
        p = patch('business_consumer.cli_command', return_value=['existing.exe', 'app-server'])
        p.start(); self.addCleanup(p.stop)
        self.native = Native()
        self.factory = Mock(return_value=self.native)
        self.local = peer.project_local(self.root, self.config)
        self.binding = self.local / 'business-agent-B.json'
        self.attempt = self.local / 'business-bootstrap-B.json'

    def ensure(self):
        return setup.ensure_business_agent(self.config, 'B', self.factory)

    def test_create_once_register_and_restart_reuses_exact_thread(self):
        self.assertEqual(self.ensure(), {'state': 'created', 'thread': THREAD})
        self.assertTrue(self.native.closed)
        self.assertEqual(self.ensure(), {'state': 'reused', 'thread': THREAD})
        self.factory.assert_called_once()
        self.assertEqual([m for m, _ in self.native.calls], ['initialize', 'initialized', 'thread/start'])
        params = self.native.calls[-1][1]
        self.assertEqual(params['cwd'], str(self.business))
        self.assertIs(params['ephemeral'], False)
        self.assertNotIn('model', params)
        self.assertNotIn('approvalPolicy', params)
        self.assertNotIn('sandbox', params)
        self.assertIn('默认只读', params['developerInstructions'])
        self.assertIn('结束当前AI回合', params['developerInstructions'])
        self.assertEqual(json.loads(self.binding.read_text())['coordinator'], 'A')

    def test_existing_manual_binding_is_never_replaced(self):
        self.local.mkdir(parents=True)
        original = json.dumps({'thread': THREAD, 'workdir': str(self.business), 'registration': 'local_session'})
        self.binding.write_text(original)
        self.ensure()
        self.assertEqual(self.binding.read_text(), original)
        self.factory.assert_not_called()

    def test_creation_unknown_is_not_repeated_after_restart(self):
        def call(method, params, **kwargs):
            if method == 'thread/start':
                raise TimeoutError
            return {}
        self.native.call = Mock(side_effect=call)
        for _ in range(2):
            with self.assertRaises(PeerError): self.ensure()
        self.factory.assert_called_once()
        self.assertEqual(json.loads(self.attempt.read_text())['state'], 'creating')
        self.assertFalse(self.binding.exists())

    def test_initialize_failure_can_retry_without_unknown_creation_record(self):
        self.native.call = Mock(side_effect=TimeoutError)
        with self.assertRaises(PeerError): self.ensure()
        self.assertFalse(self.attempt.exists())
        self.native = Native(); self.factory.return_value = self.native
        self.assertEqual(self.ensure()['state'], 'created')

    def test_created_thread_recovers_registration_without_second_native_create(self):
        self.local.mkdir(parents=True)
        self.attempt.write_text(json.dumps({'state': 'created', 'thread': THREAD, 'context': {
            'project_id': 'project-one', 'peer': 'B', 'workdir': str(self.business)}}))
        self.ensure()
        self.factory.assert_not_called()
        self.assertEqual(json.loads(self.binding.read_text())['thread'], THREAD)

    def test_wrong_directory_or_unverified_identity_cannot_create(self):
        self.config['send_authorized'] = False
        with self.assertRaises(PeerError): self.ensure()
        self.config['send_authorized'] = True
        self.config['peers'][1]['workdir'] = str(self.root)
        with self.assertRaises(PeerError): self.ensure()
        self.factory.assert_not_called()

    def test_directory_change_does_not_overwrite_previous_binding(self):
        self.ensure(); old = self.binding.read_bytes()
        other = self.business.parent / 'other'; other.mkdir()
        self.config['peers'][1]['workdir'] = str(other)
        with self.assertRaises(PeerError): self.ensure()
        self.assertEqual(self.binding.read_bytes(), old)
        self.assertEqual(self.factory.call_count, 1)

    def test_concurrent_launch_cannot_create_a_second_thread(self):
        self.local.mkdir(parents=True)
        with acquire_instance_lock(self.local / 'business-bootstrap-B.lock'):
            with self.assertRaises(PeerAlreadyRunning): self.ensure()
        self.factory.assert_not_called()

    def test_opt_out_preserves_legacy_start_without_native_calls(self):
        self.config.pop('auto_coordinate')
        self.assertEqual(self.ensure()['state'], 'manual')
        self.factory.assert_not_called()


class DetectionTests(unittest.TestCase):
    def message(self, **changes):
        data = {'version': 1, 'kind': 'bound', 'source': 'A', 'target': 'B',
                'project_id': 'project-one', 'authorized_at': time.time()}
        data.update(changes)
        return {'conversationId': 'group', 'senderId': 'sender-A',
                'text': peer.BUSINESS_PREFIX + json.dumps(data)}

    def detect(self, messages, **changes):
        history = dict(complete=True, messages=messages); history.update(changes)
        return setup.invited_project(history, 'group', 'B', 'A', 'sender-A')

    def test_unique_current_real_robot_invitation_is_detected(self):
        self.assertEqual(self.detect([self.message()]), 'project-one')

    def test_forgery_expiration_and_incomplete_history_never_select_project(self):
        wrong = self.message(); wrong['senderId'] = 'personal-account'
        for message in [wrong, self.message(target='other'), self.message(authorized_at=time.time()-90000),
                        self.message(kind='request', expires_at=time.time()-1), self.message(project_id='../other')]:
            with self.subTest(message=message): self.assertIsNone(self.detect([message]))
        self.assertIsNone(self.detect([self.message()], complete=False))
        self.assertIsNone(self.detect([self.message()], hasMore=True))
        self.assertIsNone(self.detect([self.message()], partial=True))
        self.assertIsNone(self.detect([self.message()], failedCount=1))
        malformed = self.message(); malformed['text'] = peer.BUSINESS_PREFIX + '[]'
        self.assertIsNone(self.detect([None, malformed]))

    def test_multiple_projects_require_choice_instead_of_latest_guess(self):
        self.assertIsNone(self.detect([self.message(), self.message(project_id='project-two')]))

    def test_only_unique_nearby_repository_is_selected(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)/'connector'; root.mkdir(); (root/'.git').mkdir()
            with patch.object(setup, 'ROOT', root):
                self.assertIsNone(setup.detect_project_directory())
                repo = root.parent/'business'; repo.mkdir(); (repo/'CMakeLists.txt').touch()
                self.assertEqual(setup.detect_project_directory(), str(repo.resolve()))
                other = root.parent/'other'; other.mkdir(); (other/'Cargo.toml').touch()
                self.assertIsNone(setup.detect_project_directory())

    def test_model_selection_uses_account_default_instead_of_fixed_model(self):
        self.assertEqual(setup.select_model([{'id': 'gpt-6-sol'}, {'id': 'account-default', 'isDefault': True}]), 'account-default')

    def test_initial_timeout_has_chinese_actionable_error_without_raw_command(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(setup, 'ROOT', Path(folder)), \
                patch.object(setup, 'codex_command', return_value=(['private-command'], set())), \
                patch.object(setup.subprocess, 'run', side_effect=subprocess.TimeoutExpired('private-command', 120)):
            with self.assertRaises(PeerError) as caught: setup.check_codex({})
            self.assertIn('网络/代理', str(caught.exception))
            self.assertNotIn('private-command', str(caught.exception))


if __name__ == '__main__':
    unittest.main()
