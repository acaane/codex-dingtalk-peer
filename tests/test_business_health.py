import json
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import time
import types
import unittest
from unittest.mock import patch

import setup_helper
from reliability import PeerError
from tests import test_business_bridge as bridge_fixtures

THREAD = bridge_fixtures.THREAD
OTHER = bridge_fixtures.OTHER


class QueueHealthTests(unittest.TestCase):
    def test_enqueue_does_not_override_saved_background_settings(self):
        with tempfile.TemporaryDirectory() as directory:
            message = Path(directory) / '任务.txt'
            message.write_text('只读核查现有任务，不重复启动。', encoding='utf-8')
            response = types.SimpleNamespace(returncode=0, stdout=f'Queued message {OTHER} for thread {THREAD}', stderr='')
            with patch.object(setup_helper, 'business_queue_entry', return_value='codex'), \
                    patch.object(setup_helper.subprocess, 'run', return_value=response) as run:
                result = setup_helper.queue_business_message({'codex_path': 'codex'}, message, THREAD)
            self.assertEqual(result['state'], 'queued')
            self.assertEqual(run.call_count, 1)
            self.assertIn('queue --thread', run.call_args.args[0][-1])
            self.assertNotIn('--enable', run.call_args.args[0][-1])

    def test_metadata_is_read_only_scoped_and_excludes_report_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / 'queue_1.sqlite'
            db = sqlite3.connect(database)
            db.execute('CREATE TABLE queued_items(id TEXT, thread_id TEXT, payload_json TEXT, created_at_ms INTEGER)')
            db.executemany('INSERT INTO queued_items VALUES(?,?,?,?)', [
                (OTHER, THREAD, 'secret message and token', 1000),
                (THREAD, OTHER, 'different conversation secret', 2000)])
            db.commit(); db.close()
            before = database.read_bytes()
            report = dict(codexVersion='0.157.1', checks={
                'app_server.status': dict(status='warning', details=dict(mode='ephemeral')),
                'auth.credentials': dict(token='private-token'),
                'config.load': dict(details={'sqlite home': str(root), 'feature flag overrides': 'daemon_auto_start=false'})})
            result = types.SimpleNamespace(returncode=0, stdout=json.dumps(report))
            with patch.object(setup_helper, 'business_queue_entry', return_value='codex'), \
                    patch.object(setup_helper.subprocess, 'run', return_value=result) as run:
                data = setup_helper.business_queue_health({'codex_path': 'codex'}, THREAD, [OTHER, THREAD])
            self.assertEqual(data['queued_items'], [dict(queue_message_id=OTHER, pending=True, created_at_ms=1000),
                                                    dict(queue_message_id=THREAD, pending=False)])
            self.assertFalse(data['auto_start'])
            self.assertEqual(data['runtime_status'], 'warning')
            self.assertNotIn('secret', json.dumps(data))
            self.assertNotIn('private-token', json.dumps(data))
            self.assertEqual(database.read_bytes(), before)
            self.assertIn('doctor --json', run.call_args.args[0][-1])

    def test_invalid_ids_are_rejected_before_local_tools(self):
        with patch.object(setup_helper, 'business_queue_entry') as entry:
            with self.assertRaises(PeerError):
                setup_helper.business_queue_health({}, 'invalid', [])
            with self.assertRaises(PeerError):
                setup_helper.business_queue_health({}, THREAD, [123])
            entry.assert_not_called()

    def test_unavailable_doctor_has_no_repair_or_message_retry(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.dict(setup_helper.os.environ, {'CODEX_HOME': directory}), \
                patch.object(setup_helper, 'business_queue_entry', return_value='codex'), \
                patch.object(setup_helper.subprocess, 'run', side_effect=subprocess.TimeoutExpired('doctor', 15)) as run:
            data = setup_helper.business_queue_health({'codex_path': 'codex'}, THREAD, [OTHER])
            self.assertEqual(data['runtime_status'], 'unavailable')
            self.assertFalse(data['queue_available'])
            self.assertEqual(run.call_count, 1)


class HealthProtocolTests(unittest.TestCase):
    def setUp(self):
        self.fixture = bridge_fixtures.BusinessBridgeTests(methodName='test_local_self_registration_is_automatic_idempotent_and_reaches_other_peer')
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.bind()

    def test_only_registered_coordinator_can_request_diagnostics(self):
        f = self.fixture
        identity = f.bridges['A'].probe('B')
        wire = f.transports['A'].sent[-1]
        forged = f.event(wire); forged['sender_open_dingtalk_id'] = 'owner-A'
        f.bridges['B'].receive(forged)
        self.assertIsNone(f.stores['B'].meta('business_health_local:' + identity))
        f.bridges['B'].receive(f.event(wire))
        self.assertEqual(f.stores['B'].meta('business_health_local:' + identity)['state'], 'pending')

    def test_diagnostic_round_trip_never_queues_business_work(self):
        f = self.fixture; identity = f.bridges['A'].probe('B'); f.deliver('A', 'B')
        health = dict(schema_version=1, checked_at=time.time(), thread=THREAD, runtime_status='warning',
                      auto_start=False, queue_available=True, queued_items=[])
        with patch.object(setup_helper, 'business_queue_health', return_value=health) as inspect, \
                patch.object(setup_helper, 'queue_business_message') as submit:
            f.bridges['B'].tick()
            f.bridges['B'].health_worker.join(timeout=3)
            f.bridges['B'].tick()
            f.deliver('B', 'A')
            self.assertEqual(inspect.call_count, 1)
            submit.assert_not_called()
            self.assertEqual(f.stores['A'].meta('business_health_remote:' + identity)['health'], health)
            f.deliver('A', 'B'); f.bridges['B'].tick()
            self.assertEqual(inspect.call_count, 1)

    def test_reply_must_match_requested_peer_and_thread(self):
        f = self.fixture; identity = f.bridges['A'].probe('B')
        from peer import business_frame
        wire = business_frame(dict(version=1, kind='health_reply', source='B', target='A', id=identity,
                                   thread=OTHER, health=dict(schema_version=1, thread=OTHER)))
        with self.assertRaises(PeerError):
            f.bridges['A'].receive(f.event(wire))

    def test_read_only_diagnostics_include_exact_owned_expired_queue_ids(self):
        f=self.fixture;identity,_=f.request();row=f.stores['B'].meta('business_job:'+identity)
        row.update(state='queued',queue_message_id=OTHER,expires_at=time.time()-60)
        f.stores['B'].set_meta('business_job:'+identity,row)
        health_id=f.bridges['A'].probe('B');f.deliver('A','B')
        health=dict(schema_version=1,thread=THREAD,queue_available=True,queued_items=[])
        with patch.object(setup_helper,'business_queue_health',return_value=health) as inspect:
            f.bridges['B'].health_tick();f.bridges['B'].health_worker.join(timeout=3)
            f.bridges['B'].health_tick(start=False)
        self.assertIn(OTHER,inspect.call_args.args[2])
        self.assertEqual(f.stores['B'].meta('business_job:'+identity)['expires_at'],row['expires_at'])
        self.assertEqual(f.stores['B'].meta('business_health_local:'+health_id)['state'],'done')

    def test_current_consumer_status_is_not_attributed_to_expired_handoffs(self):
        import business_consumer
        f=self.fixture;identity,_=f.request();row=f.stores['B'].meta('business_job:'+identity)
        row.update(state='queued',queue_message_id=OTHER,expires_at=time.time()-60,reason='yesterday')
        f.stores['B'].set_meta('business_job:'+identity,row)
        _,state=business_consumer.paths(f.root,f.config,'B',THREAD)
        state.write_text(json.dumps(dict(thread=THREAD,state='turn_started',reason='today has started')),encoding='utf-8')
        f.bridges['B'].wake_tick(start=False)
        self.assertEqual(f.stores['B'].meta('business_job:'+identity)['reason'],'yesterday')


class QueueWakeTests(unittest.TestCase):
    def setUp(self):
        adapter=patch.object(setup_helper,'business_wake_adapter',return_value='app')
        adapter.start();self.addCleanup(adapter.stop)

    def health(self, pending=True, available=True, desktop=True):
        return dict(queue_available=available, desktop_wakeup_available=desktop,
                    queued_items=[dict(queue_message_id=OTHER, pending=pending)])

    def test_opens_exact_existing_chat_without_resubmitting_a_prompt(self):
        with patch.object(setup_helper, 'business_queue_health', return_value=self.health()), \
                patch.object(setup_helper.subprocess, 'run', return_value=types.SimpleNamespace(returncode=0)) as run:
            result=setup_helper.wake_queued_business({},THREAD,[OTHER],time.time()+60)
        self.assertEqual(result['state'],'requested')
        command=run.call_args.args[0]
        self.assertIn('codex://threads/'+THREAD,command[-1])
        self.assertIn('-WindowStyle Hidden',command[-1])
        self.assertNotIn('--message',command[-1])
        self.assertNotIn('new?',command[-1])
        self.assertEqual(run.call_count,1)

    def test_missing_handler_unavailable_queue_and_consumed_messages_never_launch(self):
        for health,state in [(self.health(desktop=False),'unavailable'),
                             (self.health(available=False),'unavailable'),(self.health(pending=False),'consumed')]:
            with self.subTest(health=health), \
                    patch.object(setup_helper,'business_queue_health',return_value=health), \
                    patch.object(setup_helper.subprocess,'run') as run:
                result=setup_helper.wake_queued_business({},THREAD,[OTHER],time.time()+60)
                self.assertEqual(result['state'],state);run.assert_not_called()

    def test_expiry_before_and_during_diagnostics_prevents_launch(self):
        for times in ([100],[90,100]):
            with self.subTest(times=times),patch.object(setup_helper.time,'time',side_effect=times), \
                    patch.object(setup_helper,'business_queue_health',return_value=self.health()), \
                    patch.object(setup_helper.subprocess,'run') as run:
                result=setup_helper.wake_queued_business({},THREAD,[OTHER],99)
                self.assertEqual(result['state'],'expired');run.assert_not_called()

    def test_timeout_is_unknown_and_not_retried(self):
        with patch.object(setup_helper,'business_queue_health',return_value=self.health()), \
                patch.object(setup_helper.subprocess,'run',side_effect=subprocess.TimeoutExpired('wake',15)) as run:
            result=setup_helper.wake_queued_business({},THREAD,[OTHER],time.time()+60)
        self.assertEqual(result['state'],'unknown');self.assertEqual(run.call_count,1)

    def test_cli_host_is_kept_even_when_native_queue_already_consumed_the_message(self):
        import business_consumer
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);local=root/'.local';local.mkdir()
            (local/'machine.json').write_text(json.dumps(dict(peer='B')),encoding='utf-8')
            (local/'business-agent-B.json').write_text(json.dumps(dict(thread=THREAD)),encoding='utf-8')
            with patch.object(setup_helper,'ROOT',root), \
                    patch.object(setup_helper,'business_queue_health',return_value=self.health(pending=False)), \
                    patch.object(setup_helper,'business_wake_adapter',return_value='cli'), \
                    patch.object(business_consumer,'launch',return_value=dict(state='requested',reason='宿主等待后续工作')) as launch, \
                    patch.object(setup_helper,'queue_business_message') as enqueue:
                result=setup_helper.wake_queued_business({},THREAD,[OTHER],time.time()+60)
            self.assertEqual(result['state'],'requested');launch.assert_called_once();enqueue.assert_not_called()

    def test_bridge_wakes_existing_queue_once_and_restart_does_not_replay(self):
        f=bridge_fixtures.BusinessBridgeTests(methodName='test_local_self_registration_is_automatic_idempotent_and_reaches_other_peer')
        f.setUp();self.addCleanup(f.doCleanups)
        identity,_=f.request();b=f.bridges['B']
        with patch.object(setup_helper,'queue_business_message',return_value=dict(state='queued',queue_message_id=OTHER)), \
                patch.object(setup_helper,'wake_queued_business',return_value=dict(state='requested',reason='原会话已请求打开')) as wake:
            b.tick();f.finish_worker(b)
            if b.wake_worker:b.wake_worker.join(timeout=3)
            b.tick();b.tick()
            b.close()
            from peer import BusinessAgentBridge
            replacement=BusinessAgentBridge(f.config,b.peer,b.store,b.transport,f.root,recover=True)
            replacement.tick();replacement.close()
            self.assertEqual(wake.call_count,1)
            self.assertEqual(wake.call_args.args[1:3],(THREAD,[OTHER]))
            self.assertEqual(b.store.meta('business_job:'+identity)['state'],'queued')

    def test_bridge_never_wakes_expired_changed_binding_or_reported_jobs(self):
        f=bridge_fixtures.BusinessBridgeTests(methodName='test_local_self_registration_is_automatic_idempotent_and_reaches_other_peer')
        f.setUp();self.addCleanup(f.doCleanups)
        identity,_=f.request();b=f.bridges['B'];key='business_job:'+identity
        original=b.store.meta(key)
        for fields in [dict(expires_at=time.time()-1),dict(thread=OTHER),dict(source='untrusted'),dict(result_version='already-reported')]:
            b.store.set_meta(key,dict(original,state='queued',queue_message_id=OTHER,**fields))
            with patch.object(setup_helper,'wake_queued_business') as wake:
                b.wake_tick();self.assertIsNone(b.wake_worker);wake.assert_not_called()


if __name__ == '__main__':
    unittest.main()
