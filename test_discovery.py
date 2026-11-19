import copy
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

import setup_helper
from reliability import DeliveryUnknown, PeerError, SendRejected


class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.template = {'group_id': 'group', 'peers': [
            {'id': 'A', 'bot_id': 'bot-A', 'owner_member_id': 'member-A'},
            {'id': 'B', 'bot_id': 'bot-B', 'owner_member_id': 'member-B'}]}
        self.membership = {'conversationId': 'group', 'complete': True, 'hasMore': False, 'users': [
            {'name': '同事A', 'memberDingtalkId': 'member-A', 'openDingtalkId': 'user-A'},
            {'name': '同事C', 'memberDingtalkId': 'member-C', 'openDingtalkId': 'user-C'}], 'bots': [
            {'name': '联调-AI-同事A', 'openBotId': 'bot-A', 'openDingtalkId': 'sender-A', 'robotCode': 'normal'},
            {'name': '联调-AI-同事C', 'openBotId': 'bot-C', 'openDingtalkId': 'sender-C', 'robotCode': 'normal'}]}
        self.self_data = {'result': [{'orgEmployeeModel': {'userId': 'contact-C', 'orgUserName': '同事C'}}]}
        self.search = {'result': [{'userId': 'contact-C', 'openDingTalkId': 'user-C'}]}
        self.profile = {'userName': '同事C', 'profile': 'corp:contact-C'}

    def discover(self, **kwargs):
        return setup_helper.discover_participants({}, self.profile, self.membership, self.self_data, self.search,
                                                  self.template, project_id='project-one', **kwargs)

    def test_new_colleague_is_discovered_without_roster_or_name_authorization(self):
        roster, own = self.discover(input_fn=lambda _: self.fail('All inputs are detectable'))
        self.assertTrue(own.startswith('P'))
        self.assertEqual(len(roster['peers']), 2)
        found = next(p for p in roster['peers'] if p['id'] == own)
        self.assertEqual(found['sender_id'], 'sender-C')
        self.assertEqual(found['owner_member_id'], 'member-C')
        self.assertEqual(next(p for p in roster['peers'] if p['id'] == 'A')['sender_id'], 'sender-A')

    def test_same_display_name_with_different_contact_id_does_not_authorize(self):
        self.search['result'][0]['userId'] = 'someone-else'
        with self.assertRaisesRegex(PeerError, '不能只凭姓名'):
            self.discover()

    def test_incomplete_or_wrong_group_membership_cannot_be_saved(self):
        for key, value in [('complete', False), ('hasMore', True), ('failedCount', 1), ('conversationId', 'other')]:
            saved = copy.deepcopy(self.membership)
            self.membership[key] = value
            with self.subTest(key=key), self.assertRaises(PeerError):
                self.discover()
            self.membership = saved

    def test_renamed_own_bot_requires_actual_selection(self):
        self.membership['bots'][1]['name'] = '自定义播报机器人'
        roster, own = self.discover(input_fn=lambda _: '2')
        self.assertEqual(next(p for p in roster['peers'] if p['id'] == own)['bot_id'], 'bot-C')

    def test_generated_peer_id_is_stable_across_account_scoped_sender_ids(self):
        _, first = self.discover()
        self.membership['bots'][1]['openDingtalkId'] = 'other-account-sender-C'
        _, second = self.discover()
        self.assertEqual(first, second)


class CredentialIdentityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)
        patcher = patch.object(setup_helper, 'ROOT', self.root)
        patcher.start(); self.addCleanup(patcher.stop)
        patcher = patch.object(setup_helper.time, 'sleep')
        patcher.start(); self.addCleanup(patcher.stop)
        self.config = {'profile': 'corp:user', 'dws_path': 'dws', 'group_id': 'group',
                       'peers': [{'id': 'A', 'sender_id': 'sender-A'}]}
        self.transport = types.SimpleNamespace(token='synthetic-credential', send=unittest.mock.Mock())
        patcher = patch.object(setup_helper, 'Transport', return_value=self.transport)
        patcher.start(); self.addCleanup(patcher.stop)

    def history(self, sender):
        record = json.loads((self.root / '.local/identity-check-A.json').read_text(encoding='utf-8'))
        return {'complete': True, 'messages': [{'messageId': 'real-message', 'conversationId': 'group',
                                               'senderId': sender, 'text': record['text']}]}

    def test_actual_sender_is_verified_once_and_reused(self):
        with patch.object(setup_helper, 'run_json', side_effect=lambda _: self.history('sender-A')):
            setup_helper.verify_webhook_identity(self.config, 'A')
            setup_helper.verify_webhook_identity(self.config, 'A')
        self.assertEqual(self.transport.send.call_count, 1)

    def test_wrong_actual_sender_rejects_even_if_message_text_matches(self):
        with patch.object(setup_helper, 'run_json', side_effect=lambda _: self.history('other-bot')):
            with self.assertRaises(PeerError):
                setup_helper.verify_webhook_identity(self.config, 'A')
            with self.assertRaises(PeerError):
                setup_helper.verify_webhook_identity(self.config, 'A')
        self.assertEqual(self.transport.send.call_count, 1)

    def test_unknown_send_is_reconciled_without_resending(self):
        self.transport.send.side_effect = DeliveryUnknown('unknown')
        with patch.object(setup_helper, 'run_json', return_value={'messages': []}), self.assertRaises(PeerError):
            setup_helper.verify_webhook_identity(self.config, 'A')
        with patch.object(setup_helper, 'run_json', side_effect=lambda _: self.history('sender-A')):
            setup_helper.verify_webhook_identity(self.config, 'A')
        self.assertEqual(self.transport.send.call_count, 1)

    def test_delayed_history_is_polled_without_resending_probe(self):
        reads = 0
        def delayed(_):
            nonlocal reads
            reads += 1
            return self.history('sender-A') if reads == 3 else {'complete': True, 'messages': []}
        with patch.object(setup_helper, 'run_json', side_effect=delayed):
            setup_helper.verify_webhook_identity(self.config, 'A')
        self.assertEqual(reads, 3)
        self.assertEqual(self.transport.send.call_count, 1)

    def test_rejection_is_distinct_from_unknown_and_not_automatically_resent(self):
        self.transport.send.side_effect = SendRejected('rejected')
        with patch.object(setup_helper, 'run_json') as history:
            for _ in range(2):
                with self.assertRaisesRegex(PeerError, '明确拒绝'):
                    setup_helper.verify_webhook_identity(self.config, 'A')
        record = json.loads((self.root / '.local/identity-check-A.json').read_text(encoding='utf-8'))
        self.assertEqual(record['state'], 'rejected')
        self.assertEqual(self.transport.send.call_count, 1)
        history.assert_not_called()

    def test_manual_setup_can_retry_a_confirmed_rejection_once(self):
        self.transport.send.side_effect = [SendRejected('rejected'), None]
        with self.assertRaises(PeerError):
            setup_helper.verify_webhook_identity(self.config, 'A')
        with patch.object(setup_helper, 'run_json', side_effect=lambda _: self.history('sender-A')):
            setup_helper.verify_webhook_identity(self.config, 'A', retry_rejected=True)
        self.assertEqual(self.transport.send.call_count, 2)

    def test_manual_setup_keeps_unknown_delivery_without_resending(self):
        self.transport.send.side_effect = DeliveryUnknown('unknown')
        with patch.object(setup_helper, 'run_json', return_value={'complete': True, 'messages': []}):
            for _ in range(2):
                with self.assertRaises(PeerError):
                    setup_helper.verify_webhook_identity(self.config, 'A', retry_rejected=True)
        self.assertEqual(self.transport.send.call_count, 1)

    def test_legacy_rejection_can_be_recovered_without_treating_unknown_as_rejected(self):
        self.transport.send.side_effect = DeliveryUnknown('unknown')
        with patch.object(setup_helper, 'run_json', return_value={'complete': True, 'messages': []}), self.assertRaises(PeerError):
            setup_helper.verify_webhook_identity(self.config, 'A')
        path = self.root / '.local/identity-check-A.json'
        record = json.loads(path.read_text(encoding='utf-8'))
        record['error_type'] = 'SendRejected'
        path.write_text(json.dumps(record), encoding='utf-8')
        self.transport.send.side_effect = None
        with patch.object(setup_helper, 'run_json', side_effect=lambda _: self.history('sender-A')):
            setup_helper.verify_webhook_identity(self.config, 'A', retry_rejected=True)
        self.assertEqual(self.transport.send.call_count, 2)

    def test_incomplete_history_does_not_authorize_or_resend(self):
        def partial(_):
            return dict(self.history('sender-A'), complete=False)
        with patch.object(setup_helper, 'run_json', side_effect=partial), self.assertRaisesRegex(PeerError, '不完整'):
            setup_helper.verify_webhook_identity(self.config, 'A')
        self.assertEqual(self.transport.send.call_count, 1)
        record = json.loads((self.root / '.local/identity-check-A.json').read_text(encoding='utf-8'))
        self.assertEqual(record['state'], 'accepted')


class WebhookSendDiagnosticsTests(unittest.TestCase):
    setUp = CredentialIdentityTests.setUp

    def prepare_failed_setup(self):
        (self.root / '.local').mkdir(exist_ok=True)
        (self.root / '.local/config.json').write_text(json.dumps(dict(self.config,send_authorized=False)),encoding='utf-8')
        (self.root / '.local/machine.json').write_text('{"peer":"A"}',encoding='utf-8')
        fingerprint=setup_helper.hashlib.sha256(('corp:user'+'group'+'sender-A'+self.transport.token).encode()).hexdigest()
        p=self.root / '.local/identity-check-A.json'
        p.write_text(json.dumps({'fingerprint':fingerprint,'state':'unknown','error_type':'DeliveryUnknown','text':'old-probe','started_at':100}),encoding='utf-8')
        return p,p.read_bytes()

    def diagnostic_history(self, _):
        p=self.root / '.local/webhook-diagnostic-A.json'
        if not p.exists(): return {'complete':True}
        record=json.loads(p.read_text(encoding='utf-8'))
        return {'complete':True,'messages':[{'conversationId':'group','senderId':'sender-A','messageId':'new-proof','text':record['text']}]}

    def test_distinct_probe_once_proves_identity_and_keeps_original_unknown(self):
        original,data=self.prepare_failed_setup()
        config_before=(self.root / '.local/config.json').read_bytes()
        with patch.object(setup_helper,'configuration_template',return_value={}), \
             patch.object(setup_helper,'resolve_group_identities',side_effect=lambda c,*_:c), \
             patch.object(setup_helper,'run_json',side_effect=self.diagnostic_history):
            output=setup_helper.diagnose_webhook_send()
            setup_helper.diagnose_webhook_send()
            setup_helper.verify_webhook_identity(self.config,'A')
        self.assertEqual(self.transport.send.call_count,1)
        self.assertNotEqual(self.transport.send.call_args.args[0],'old-probe')
        self.assertEqual(original.read_bytes(),data)
        self.assertEqual((self.root / '.local/config.json').read_bytes(),config_before)
        self.assertEqual(json.loads(output.read_text(encoding='utf-8'))['state'],'verified')

    def test_new_unknown_diagnostic_is_not_resent_after_restart(self):
        self.prepare_failed_setup();self.transport.send.side_effect=DeliveryUnknown('unknown')
        with patch.object(setup_helper,'configuration_template',return_value={}), \
             patch.object(setup_helper,'resolve_group_identities',side_effect=lambda c,*_:c), \
             patch.object(setup_helper,'run_json',return_value={'complete':True,'messages':[]}):
            setup_helper.diagnose_webhook_send()
            setup_helper.diagnose_webhook_send()
        self.assertEqual(self.transport.send.call_count,1)

    def test_fingerprint_mismatch_never_sends(self):
        self.prepare_failed_setup();self.transport.token='different-credential'
        with patch.object(setup_helper,'configuration_template',return_value={}), \
             patch.object(setup_helper,'resolve_group_identities',side_effect=lambda c,*_:c), \
             patch.object(setup_helper,'run_json',return_value={'complete':True}),self.assertRaises(PeerError):
            setup_helper.diagnose_webhook_send()
        self.transport.send.assert_not_called()


    def test_cached_mcp_rejection_stops_setup_without_resending_unknown_original(self):
        original,data=self.prepare_failed_setup()
        record=json.loads(data)
        record['send_receipt']={'error_message':'[MCP_TOOL_ERROR] {"errcode":"300005","errmsg":"token is not exist","success":true}'}
        path=self.root / '.local/webhook-diagnostic-A.json'
        path.write_text(json.dumps(record),encoding='utf-8')
        with self.assertRaisesRegex(PeerError,'300005'):
            setup_helper.verify_webhook_identity(self.config,'A',retry_rejected=True)
        self.transport.send.assert_not_called()
        self.assertEqual(original.read_bytes(),data)
        self.assertEqual(json.loads(path.read_text(encoding='utf-8'))['state'],'rejected')

    def test_new_credential_archives_original_unknown_before_a_new_identity_probe(self):
        original,data=self.prepare_failed_setup()
        self.transport.token='corrected-credential'
        def history(_):
            record=json.loads(original.read_text(encoding='utf-8'))
            return {'complete':True,'messages':[{'conversationId':'group','senderId':'sender-A','messageId':'corrected-proof','text':record['text']}]}
        with patch.object(setup_helper,'run_json',side_effect=history):
            setup_helper.verify_webhook_identity(self.config,'A',retry_rejected=True)
        self.assertEqual(self.transport.send.call_count,1)
        archived=list((self.root / '.local').glob('identity-check-A-*.json'))
        self.assertEqual(len(archived),1)
        self.assertEqual(json.loads(archived[0].read_text(encoding='utf-8')),json.loads(data))
        self.assertEqual(json.loads(original.read_text(encoding='utf-8'))['state'],'verified')


class OnboardingDiagnosticsTests(unittest.TestCase):
    def test_reads_failed_setup_without_enabling_or_sending(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / '.local').mkdir(); (root / '.tools').mkdir()
            config = {'group_id':'group', 'profile':'profile', 'project_id':'project-one', 'send_authorized':False,
                      'peers':[{'id':'A','sender_id':'sender-A'}], 'credential_file':'private'}
            (root / '.local/config.json').write_text(json.dumps(config), encoding='utf-8')
            (root / '.local/machine.json').write_text(json.dumps({'peer':'A'}), encoding='utf-8')
            (root / '.tools/toolchain.json').write_text(json.dumps({'dws_path':'dws'}), encoding='utf-8')
            record = {'state':'unknown','error_type':'DeliveryUnknown','started_at':100,'text':'unique probe'}
            (root / '.local/identity-check-A.json').write_text(json.dumps(record), encoding='utf-8')
            before = {p:p.read_bytes() for p in (root / '.local').iterdir()}
            responses = [types.SimpleNamespace(returncode=0,stdout='dws version v1.0.66',stderr=''),
                         types.SimpleNamespace(returncode=0,stdout=json.dumps({'parameters':['body-stdin','identity']}),stderr=''),
                         types.SimpleNamespace(returncode=0,stdout=json.dumps({'complete':True,'conversationId':'group',
                             'bots':[{'openDingtalkId':'sender-A','openBotId':'bot-A','name':'Own'}], 'users':[]}),stderr=''),
                         types.SimpleNamespace(returncode=0,stdout=json.dumps({'complete':True,'messages':[
                             {'conversationId':'group','text':'unique probe','senderId':'sender-A','messageId':'m1'}]}),stderr='')]
            transport = types.SimpleNamespace(token='never-export-secret')
            with patch.object(setup_helper,'ROOT',root), patch.object(setup_helper,'Transport',return_value=transport), \
                 patch.object(setup_helper.subprocess,'run',side_effect=responses) as runner:
                output = setup_helper.collect_onboarding_diagnostics()
            report = json.loads(output.read_text(encoding='utf-8'))
            self.assertFalse(report['send_authorized'])
            self.assertEqual(report['probe']['state'],'unknown')
            self.assertEqual(report['probe_history']['matching_sender_ids'],['sender-A'])
            self.assertNotIn('never-export-secret',output.read_text(encoding='utf-8'))
            for p,data in before.items(): self.assertEqual(p.read_bytes(),data)
            self.assertEqual(runner.call_count,4)
            self.assertFalse(any('+messages-send' in call.args[0] for call in runner.call_args_list))

    def test_failure_output_is_not_exported(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); (root / '.local').mkdir(); (root / '.tools').mkdir()
            (root / '.local/config.json').write_text(json.dumps({'group_id':'g','profile':'p','peers':[{'id':'A'}]}),encoding='utf-8')
            (root / '.local/machine.json').write_text('{"peer":"A"}',encoding='utf-8')
            (root / '.tools/toolchain.json').write_text('{"dws_path":"dws"}',encoding='utf-8')
            raw = json.dumps({'error':{'code':'permission_denied','message':'hidden-token-secret'},'retryable':False})
            response = types.SimpleNamespace(returncode=1,stdout=raw,stderr='hidden-token-secret')
            with patch.object(setup_helper,'ROOT',root),patch.object(setup_helper.subprocess,'run',return_value=response), \
                 patch.object(setup_helper,'Transport',side_effect=PeerError('hidden-token-secret')):
                output=setup_helper.collect_onboarding_diagnostics()
            content=output.read_text(encoding='utf-8')
            self.assertNotIn('hidden-token-secret',content)
            self.assertEqual(json.loads(content)['membership']['error_code'],'permission_denied')


if __name__ == '__main__':
    unittest.main()
