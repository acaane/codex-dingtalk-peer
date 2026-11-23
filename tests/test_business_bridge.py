import json
from pathlib import Path
import tempfile
import sqlite3
import threading
import time
import types
import unittest
from unittest.mock import patch

import peer
import setup_helper
from reliability import DeliveryUnknown, PeerError, SendRejected, Store

THREAD='11111111-1111-4111-8111-111111111111'
OTHER='22222222-2222-4222-8222-222222222222'


class Transport:
    token='test-private-webhook-token'
    def __init__(self,errors=()):
        self.sent=[];self.errors=list(errors)
    def send(self,text):
        self.sent.append(text)
        if self.errors:
            raise self.errors.pop(0)


class BusinessBridgeTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='AI bridge 中文 ')
        self.root=Path(self.temp.name)
        wake=patch.object(setup_helper,'wake_queued_business',return_value=dict(state='consumed',reason='test queue consumed'))
        wake.start();self.addCleanup(wake.stop)
        self.config={'group_id':'group','codex_path':'existing-codex','retry_initial_delay_seconds':1,
                     'peers':[dict(id='A',sender_id='bot-A',owner_id='owner-A',owner_member_id='member-A'),
                              dict(id='B',sender_id='bot-B',owner_id='owner-B',owner_member_id='member-B')]}
        self.stores={};self.transports={};self.bridges={}
        for p in self.config['peers']:
            directory=self.root/'runtime'/p['id'];directory.mkdir(parents=True)
            self.stores[p['id']]=Store(directory/'state.sqlite3')
            self.transports[p['id']]=Transport()
            self.bridges[p['id']]=peer.BusinessAgentBridge(self.config,p,self.stores[p['id']],self.transports[p['id']],self.root)
        self.addCleanup(self.cleanup)

    def cleanup(self):
        for b in self.bridges.values():b.close()
        for s in self.stores.values():s.db.close()
        self.temp.cleanup()

    def event(self,wire,source=None):
        if wire.startswith(peer.BUSINESS_PREFIX):
            frame=json.loads(wire[len(peer.BUSINESS_PREFIX):])
        else:
            report=peer.REPORT_HEADER.match(wire)
            frame={'source':report[1],'id':report[4]}
        return dict(conversation_id='group',sender_open_dingtalk_id='bot-'+(source or frame['source']),message_id='message-'+frame['id'],content=wire)

    def deliver(self,source,target):
        for wire in list(self.transports[source].sent):
            self.bridges[target].receive(self.event(wire))

    def bind(self):
        self.bridges['B'].register_local(THREAD,'A')
        self.bridges['B'].flush_one()
        self.deliver('B','A')

    def request(self):
        self.bind()
        identity=self.bridges['A'].submit('B','请检查并报告，不启动生产设备。',time.time()+300)
        wire=self.transports['A'].sent[-1]
        self.bridges['B'].receive(self.event(wire))
        return identity,wire

    def finish_worker(self,bridge):
        if bridge.worker:bridge.worker.join(timeout=3)
        bridge.tick()

    def test_local_self_registration_is_automatic_idempotent_and_reaches_other_peer(self):
        self.bind()
        policy=self.bridges['B'].policy()
        self.assertEqual(policy['thread'],THREAD)
        self.assertEqual(policy['registration'],'local_session')
        self.assertEqual(self.stores['A'].meta('business_remote_binding:B')['thread'],THREAD)
        self.bridges['B'].register_local(THREAD,'A');self.bridges['B'].flush_one()
        self.assertEqual(len(self.transports['B'].sent),1)

    def test_late_history_cannot_restore_an_older_registered_session(self):
        self.bind();old=self.transports['B'].sent[-1]
        self.bridges['B'].register_local(OTHER,'A');self.bridges['B'].flush_one();self.deliver('B','A')
        self.bridges['A'].receive(self.event(old))
        self.assertEqual(self.stores['A'].meta('business_remote_binding:B')['thread'],OTHER)

    def test_json_and_plaintext_credentials_are_removed_from_progress(self):
        text='{"service_token":"sensitive-one","api_key":"sensitive-two"} token=sensitive-three Bearer sensitive-four'
        clean=peer.clean_business_text(text)
        for value in ['sensitive-one','sensitive-two','sensitive-three','sensitive-four']:
            self.assertNotIn(value,clean)

    def test_register_entry_uses_own_environment_id_without_manual_copy(self):
        tools=self.root/'.tools';tools.mkdir(exist_ok=True)
        (tools/'toolchain.json').write_text(json.dumps({'dws_path':'local-dws','codex_path':'local-codex'}),encoding='utf-8')
        config=dict(self.config,dws_path='local-dws',codex_path='local-codex',send_authorized=True)
        local=self.root/'.local';local.mkdir(exist_ok=True)
        (local/'config.json').write_text(json.dumps(config),encoding='utf-8')
        (local/'machine.json').write_text(json.dumps({'peer':'B','checked':True}),encoding='utf-8')
        with patch.object(setup_helper,'ROOT',self.root),patch.object(setup_helper,'Transport',return_value=self.transports['B']), \
                patch.object(setup_helper.sys,'argv',['setup_helper.py','register-agent']),patch.dict(setup_helper.os.environ,{'CODEX_THREAD_ID':THREAD}):
            self.assertEqual(setup_helper.main(),0)
        self.assertEqual(self.bridges['B'].policy()['thread'],THREAD)
        self.assertEqual(len(self.transports['B'].sent),1)

    def test_forged_bot_and_wrong_room_cannot_register_or_dispatch(self):
        self.bind();wire=self.transports['B'].sent[-1]
        self.stores['A'].db.execute("DELETE FROM metadata WHERE key='business_remote_binding:B'");self.stores['A'].db.commit()
        forged=self.event(wire);forged['sender_open_dingtalk_id']='owner-B'
        self.bridges['A'].receive(forged)
        wrong=self.event(wire);wrong['conversation_id']='other-room'
        self.bridges['A'].receive(wrong)
        self.assertIsNone(self.stores['A'].meta('business_remote_binding:B'))

    def test_natural_binding_uses_real_owner_and_relays_own_message_gap(self):
        event=dict(conversation_id='group',sender_open_dingtalk_id='owner-B',message_id='human-message',content='AI联调 绑定协调agent '+THREAD)
        self.bridges['A'].receive(event);self.bridges['A'].flush_one();self.deliver('A','B')
        self.assertEqual(self.bridges['B'].policy()['thread'],THREAD)
        self.assertEqual(self.bridges['B'].policy()['coordinator'],'A')

    def test_duplicate_request_queues_local_agent_once_and_result_is_redacted(self):
        identity,wire=self.request();b=self.bridges['B']
        outcome={'state':'queued','thread':THREAD,'queue_message_id':OTHER,'reason':'queued'}
        with patch.object(setup_helper,'queue_business_message',return_value=outcome) as enqueue:
            b.tick();self.finish_worker(b)
            b.receive(self.event(wire));b.tick()
            self.assertEqual(enqueue.call_count,1)
        self.deliver('B','A')
        self.assertEqual(self.stores['A'].meta('business_job:'+identity)['remote_state'],'queued')
        response=b.runtime/(identity+'-response.json')
        response.write_text(json.dumps(dict(handoff_id=identity,status='ready',summary='准备就绪 '+self.transports['B'].token)),encoding='utf-8')
        b.last_poll=0;b.tick();self.deliver('B','A')
        result=self.stores['A'].meta('business_job:'+identity)['latest_result']
        self.assertEqual(result['state'],'ready')
        self.assertNotIn(self.transports['B'].token,result['summary'])
        self.assertIn('凭据已隐藏',result['summary'])

    def test_unregistered_or_changed_session_never_queues(self):
        with self.assertRaises(PeerError):
            self.bridges['A'].submit('B','检查',time.time()+60)
        identity,wire=self.request()
        self.bridges['B'].register_local(OTHER,'A')
        with patch.object(setup_helper,'queue_business_message') as enqueue:
            self.bridges['B'].tick();enqueue.assert_not_called()
        self.assertEqual(self.stores['B'].meta('business_job:'+identity)['state'],'rejected')

    def test_expired_job_is_not_started(self):
        identity,wire=self.request();row=self.stores['B'].meta('business_job:'+identity)
        row['expires_at']=time.time()-1;self.stores['B'].set_meta('business_job:'+identity,row)
        with patch.object(setup_helper,'queue_business_message') as enqueue:
            self.bridges['B'].tick();enqueue.assert_not_called()
        self.assertEqual(self.stores['B'].meta('business_job:'+identity)['state'],'expired')

    def test_delayed_receipt_and_result_cannot_roll_progress_back(self):
        identity,_=self.request();a=self.bridges['A'];b=self.bridges['B']
        newer=b.outgoing('result','A',identity,thread=THREAD,state='ready',summary='当前就绪',result_seq=2)
        older=b.outgoing('result','A',identity,thread=THREAD,state='in_progress',summary='过去进度',result_seq=1)
        delayed=b.outgoing('receipt','A',identity,thread=THREAD,state='queued',receipt_seq=1)
        a.receive(self.event(peer.business_frame(newer)));a.receive(self.event(peer.business_frame(older)));a.receive(self.event(peer.business_frame(delayed)))
        self.assertEqual(self.stores['A'].meta('business_job:'+identity)['remote_state'],'ready')

    def test_progress_outbox_failure_does_not_lose_the_saved_result(self):
        identity,_=self.request();b=self.bridges['B']
        with patch.object(setup_helper,'queue_business_message',return_value={'state':'queued','thread':THREAD}):
            b.tick();self.finish_worker(b)
        path=b.runtime/(identity+'-response.json')
        path.write_text(json.dumps(dict(handoff_id=identity,status='ready',summary='有效阶段结果')),encoding='utf-8')
        with self.stores['B'].db:
            self.stores['B'].db.execute("CREATE TRIGGER fail_result BEFORE INSERT ON metadata WHEN NEW.key LIKE 'business_outbox:%' BEGIN SELECT RAISE(ABORT,'injected outbox failure'); END")
        b.last_poll=0
        with self.assertRaises(sqlite3.IntegrityError):b.tick()
        self.assertNotIn('result_version',self.stores['B'].meta('business_job:'+identity))
        with self.stores['B'].db:self.stores['B'].db.execute('DROP TRIGGER fail_result')
        b.last_poll=0;b.tick();self.deliver('B','A')
        self.assertEqual(self.stores['A'].meta('business_job:'+identity)['latest_result']['summary'],'有效阶段结果')

    def test_conflicting_payload_for_same_id_is_rejected(self):
        identity,wire=self.request();data=json.loads(wire[len(peer.BUSINESS_PREFIX):]);data['text']='替换业务内容'
        with self.assertRaises(PeerError):
            self.bridges['B'].receive(self.event(peer.business_frame(data)))
        self.assertEqual(self.stores['B'].meta('business_job:'+identity)['text'],'请检查并报告，不启动生产设备。')

    def test_interrupted_local_queue_is_unknown_and_not_retried_on_restart(self):
        identity,wire=self.request();row=self.stores['B'].meta('business_job:'+identity)
        row['state']='queueing';self.stores['B'].set_meta('business_job:'+identity,row)
        b=peer.BusinessAgentBridge(self.config,self.config['peers'][1],self.stores['B'],self.transports['B'],self.root,recover=True)
        self.bridges['B']=b
        with patch.object(setup_helper,'queue_business_message') as enqueue:
            b.receive(self.event(wire));b.tick();enqueue.assert_not_called()
        self.assertEqual(self.stores['B'].meta('business_job:'+identity)['state'],'unknown')

    def test_unknown_group_send_is_not_blindly_retried_and_history_reconciles(self):
        b=self.bridges['B'];self.transports['B'].errors=[DeliveryUnknown('timeout')]
        b.register_local(THREAD,'A');b.flush_one();b.flush_one()
        self.assertEqual(len(self.transports['B'].sent),1)
        wire=self.transports['B'].sent[0];b.receive(self.event(wire));b.flush_one()
        self.assertEqual(len(self.transports['B'].sent),1)
        self.assertEqual(b.entries('business_outbox:')[0][1]['state'],'sent')

    def test_explicit_group_rejection_has_one_bounded_retry(self):
        b=self.bridges['B'];self.transports['B'].errors=[SendRejected('temporary',True),SendRejected('temporary',True)]
        b.register_local(THREAD,'A');b.flush_one()
        key,row=b.entries('business_outbox:')[0];row['next_at']=0;self.stores['B'].set_meta(key,row)
        b.flush_one();b.flush_one()
        self.assertEqual(len(self.transports['B'].sent),2)
        self.assertEqual(b.entries('business_outbox:')[0][1]['state'],'failed')

    def test_concurrent_launcher_and_listener_claim_group_send_once(self):
        b=self.bridges['B'];b.register_local(THREAD,'A')
        barrier=threading.Barrier(2);errors=[]
        def send():
            store=Store(self.root/'runtime/B/state.sqlite3')
            try:
                local=peer.BusinessAgentBridge(self.config,self.config['peers'][1],store,self.transports['B'],self.root)
                barrier.wait(timeout=3);local.flush_one()
            except Exception as error:errors.append(error)
            finally:store.db.close()
        workers=[threading.Thread(target=send) for _ in range(2)]
        for w in workers:w.start()
        for w in workers:w.join(timeout=5)
        self.assertEqual(errors,[])
        self.assertTrue(all(not w.is_alive() for w in workers))
        self.assertEqual(len(self.transports['B'].sent),1)

    def test_own_historical_receipt_prevents_old_request_reexecution(self):
        identity,wire=self.request()
        self.stores['B'].db.execute('DELETE FROM metadata WHERE key=?',('business_job:'+identity,));self.stores['B'].db.commit()
        receipt=self.bridges['B'].outgoing('receipt','A',identity,state='queued',thread=THREAD)
        self.bridges['B'].receive(self.event(peer.business_frame(receipt)))
        with patch.object(setup_helper,'queue_business_message') as enqueue:
            self.bridges['B'].receive(self.event(wire));self.bridges['B'].tick();enqueue.assert_not_called()

    def attach_reporters(self,enabled=True):
        self.config['task_summaries']=enabled
        for name,bridge in self.bridges.items():
            bridge.reporter=peer.TaskReporter(self.config,bridge.peer,self.stores[name],self.transports[name],self.root/'runtime'/name)

    def completed_local_progress(self):
        identity,_=self.request();self.attach_reporters()
        b=self.bridges['B']
        with patch.object(setup_helper,'queue_business_message',return_value={'state':'queued','thread':THREAD}):
            b.tick();self.finish_worker(b)
        path=b.runtime/(identity+'-response.json')
        path.write_text(json.dumps({'handoff_id':identity,'status':'ready','summary':'首次快照与连续心跳已发送，A接收仍待核验。'}),encoding='utf-8')
        b.last_poll=0
        return identity,path

    def test_business_stage_reports_are_factual_bilateral_and_deduplicated(self):
        identity,path=self.completed_local_progress();b=self.bridges['B'];a=self.bridges['A']
        b.tick();self.deliver('B','A')
        for bridge in (b,a):bridge.reporter.tick()
        for name in ('A','B'):
            reports=[text for text in self.transports[name].sent if text.startswith('【AI联调协作总结】')]
            self.assertEqual(len(reports),1)
            self.assertIn('首次快照与连续心跳已发送',reports[0])
            self.assertIn('仍待核验',reports[0])
            self.assertIn('状态：进度同步',reports[0])
            self.assertIsNotNone(peer.trusted_report(self.config,self.event(reports[0])))
            self.assertIsNone(self.stores[name].get(identity))
            self.assertEqual(len(list((self.root/'runtime'/name/'summaries').glob('*.txt'))),1)
        b.last_poll=0;b.tick();self.deliver('B','A')
        for bridge in (b,a):bridge.reporter.last_send=0;bridge.reporter.tick()
        self.assertTrue(all(len([text for text in self.transports[n].sent if text.startswith('【AI联调协作总结】')])==1 for n in ('A','B')))

    def test_local_result_and_human_report_are_one_atomic_commit(self):
        identity,path=self.completed_local_progress();b=self.bridges['B'];store=self.stores['B']
        store.db.executescript("CREATE TRIGGER reject_business_report BEFORE INSERT ON task_reports BEGIN SELECT RAISE(ABORT,'injected'); END;")
        with self.assertRaises(sqlite3.IntegrityError):b.tick()
        self.assertNotIn('result_version',store.meta('business_job:'+identity))
        self.assertEqual(store.db.execute('SELECT count(*) FROM task_reports').fetchone()[0],0)
        self.assertFalse(any('"kind":"result"' in r['wire'] for _,r in b.entries('business_outbox:')))
        store.db.execute('DROP TRIGGER reject_business_report');store.db.commit();b.last_poll=0;b.tick()
        self.assertIn('result_version',store.meta('business_job:'+identity))
        self.assertEqual(store.db.execute('SELECT count(*) FROM task_reports').fetchone()[0],1)

    def test_remote_progress_cannot_commit_without_its_human_report(self):
        identity,path=self.completed_local_progress();b=self.bridges['B'];a=self.bridges['A'];store=self.stores['A']
        b.tick();wire=next(text for text in reversed(self.transports['B'].sent) if '"kind":"result"' in text)
        store.db.executescript("CREATE TRIGGER reject_business_report BEFORE INSERT ON task_reports BEGIN SELECT RAISE(ABORT,'injected'); END;")
        with self.assertRaises(sqlite3.IntegrityError):a.receive(self.event(wire))
        self.assertNotIn('latest_result',store.meta('business_job:'+identity))
        store.db.execute('DROP TRIGGER reject_business_report');store.db.commit();a.receive(self.event(wire))
        self.assertEqual(store.meta('business_job:'+identity)['remote_state'],'ready')
        self.assertEqual(store.db.execute('SELECT count(*) FROM task_reports').fetchone()[0],1)

    def test_enabling_business_reports_does_not_flood_saved_old_progress(self):
        identity,path=self.completed_local_progress();b=self.bridges['B'];b.reporter.enabled=False
        b.tick();self.assertEqual(self.stores['B'].db.execute('SELECT count(*) FROM task_reports').fetchone()[0],0)
        b.reporter.enabled=True;b.last_poll=0;b.tick()
        self.assertEqual(self.stores['B'].db.execute('SELECT count(*) FROM task_reports').fetchone()[0],0)
        path.write_text(json.dumps({'handoff_id':identity,'status':'paused','summary':'已到任务指定的截止时间，正常停止并保存检查点。'}),encoding='utf-8')
        b.last_poll=0;b.tick()
        self.assertEqual(self.stores['B'].db.execute('SELECT count(*) FROM task_reports').fetchone()[0],1)

    def test_business_report_is_authenticated_and_cannot_interrupt_ordinary_task(self):
        identity,path=self.completed_local_progress();b=self.bridges['B'];a=self.bridges['A']
        wire=b.reporter.business_record(self.stores['B'].meta('business_job:'+identity),'paused','当前已暂停。','version')[3]
        event=self.event(wire)
        self.assertIsNotNone(peer.trusted_report(self.config,event))
        self.assertIsNone(peer.trusted_report(self.config,dict(event,sender_open_dingtalk_id='forged')))
        self.assertIsNone(peer.trusted_report(self.config,dict(event,conversation_id='wrong-room')))
        self.stores['A'].db.execute('INSERT INTO tasks VALUES (?,?,?,?,?,?)',(identity,'ordinary task',1,'active',time.time(),'[]'));self.stores['A'].db.commit()
        a.reporter.observe(event)
        self.assertEqual(self.stores['A'].db.execute('SELECT count(*) FROM task_reports').fetchone()[0],0)

    def test_business_summary_unknown_send_waits_for_trusted_history_without_retry(self):
        identity,path=self.completed_local_progress();b=self.bridges['B'];b.tick()
        transport=self.transports['B'];transport.errors.append(DeliveryUnknown('unknown send'))
        b.reporter.tick();wire=next(text for text in reversed(transport.sent) if text.startswith('【AI联调协作总结】'))
        self.assertEqual(self.stores['B'].db.execute('SELECT status FROM task_reports').fetchone()[0],'unknown')
        b.reporter.last_send=0;b.reporter.tick()
        self.assertEqual(len([text for text in transport.sent if text.startswith('【AI联调协作总结】')]),1)
        event=self.event(wire)
        b.reporter.observe(dict(event,sender_open_dingtalk_id='forged'))
        self.assertEqual(self.stores['B'].db.execute('SELECT status FROM task_reports').fetchone()[0],'unknown')
        b.reporter.observe(event)
        self.assertEqual(self.stores['B'].db.execute('SELECT status FROM task_reports').fetchone()[0],'sent')


class LocalQueueTests(unittest.TestCase):
    def test_queue_capability_uses_existing_fallback_without_install_or_upgrade(self):
        with tempfile.TemporaryDirectory() as directory:
            exe=Path(directory)/'existing Codex.exe';exe.write_bytes(b'existing')
            bad=types.SimpleNamespace(returncode=2,stdout='',stderr='unsupported')
            good=types.SimpleNamespace(returncode=0,stdout='Usage: codex queue --thread --message',stderr='')
            setup_helper.business_queue_entry.cache_clear()
            with patch.object(setup_helper.Path,'home',return_value=Path(directory)),patch.object(setup_helper.subprocess,'run',side_effect=[bad,good]) as run:
                entry=setup_helper.business_queue_entry(str(exe))
            self.assertIn(str(exe),entry)
            self.assertEqual(run.call_count,2)
            self.assertTrue(all('queue --help' in c.args[0][-1] for c in run.call_args_list))
            setup_helper.business_queue_entry.cache_clear()

    def test_message_is_read_from_file_and_never_interpreted_as_shell_text(self):
        with tempfile.TemporaryDirectory(prefix="queue quote ' ") as directory:
            path=Path(directory)/'message.txt';attack='$(Get-Secret) `whoami`';path.write_text(attack,encoding='utf-8')
            result=types.SimpleNamespace(returncode=0,stdout=f'Queued message {OTHER} for thread {THREAD}.',stderr='')
            with patch.object(setup_helper,'business_queue_entry',return_value='codex'),patch.object(setup_helper.subprocess,'run',return_value=result) as run:
                answer=setup_helper.queue_business_message({'codex_path':'existing'},path,THREAD)
            command=run.call_args.args[0][-1]
            self.assertNotIn(attack,command)
            self.assertIn("''",command)
            self.assertEqual(answer['state'],'queued')

    def test_local_queue_timeout_is_unknown(self):
        import subprocess
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'message.txt';path.write_text('real message',encoding='utf-8')
            with patch.object(setup_helper,'business_queue_entry',return_value='codex'),patch.object(setup_helper.subprocess,'run',side_effect=subprocess.TimeoutExpired('queue',45)):
                self.assertEqual(setup_helper.queue_business_message({'codex_path':'existing'},path,THREAD)['state'],'unknown')

    @unittest.skipUnless(__import__('os').name=='nt','Windows PowerShell native boundary')
    def test_native_queue_reference_preserves_quoted_multiline_body_and_path(self):
        import hashlib
        import subprocess
        import sys
        original_run=subprocess.run
        with tempfile.TemporaryDirectory(prefix="queue 中文 ' $() ") as directory:
            directory=Path(directory)
            probe=directory/'argv.py'
            probe.write_text('import json,sys;print(json.dumps(sys.argv[1:],ensure_ascii=False))',encoding='utf-8')
            path=directory/'message with spaces.txt'
            observed=[]
            def native_queue(command,**options):
                prefix=('function codex { & '+setup_helper._powershell_literal(sys.executable)+' '
                        +setup_helper._powershell_literal(probe)+' @args }\n')
                actual=original_run([*command[:-1],prefix+command[-1]],**options)
                self.assertEqual(actual.returncode,0,actual.stderr)
                observed.append(json.loads(actual.stdout))
                return types.SimpleNamespace(returncode=0,stdout=f'Queued message {OTHER} for thread {THREAD}.',stderr='')
            bodies=['error="point has no Led binding"',
                    '第一行\nquoted "multi word"\n中文路径 D:\\a b\\',
                    'literal $(Get-Secret) `whoami` & | ; with \'apostrophe\'']
            with patch.object(setup_helper,'business_queue_entry',return_value='codex'),patch.object(setup_helper.subprocess,'run',side_effect=native_queue):
                for body in bodies:
                    path.write_text(body,encoding='utf-8')
                    outcome=setup_helper.queue_business_message({'codex_path':'existing'},path,THREAD)
                    self.assertEqual(outcome['state'],'queued')
                    self.assertEqual(observed[-1][:4],['queue','--thread',THREAD,'--message'])
                    self.assertEqual(len(observed[-1]),5)
                    self.assertIn(str(path.resolve()),observed[-1][-1])
                    self.assertIn(hashlib.sha256(path.read_bytes()).hexdigest(),observed[-1][-1])
                    self.assertEqual(path.read_text(encoding='utf-8'),body)

    def test_cli_argument_rejection_is_known_not_queued(self):
        result=types.SimpleNamespace(returncode=2,stdout='',stderr="error: unexpected argument 'has' found")
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'message.txt';path.write_text('text',encoding='utf-8')
            with patch.object(setup_helper,'business_queue_entry',return_value='codex'),patch.object(setup_helper.subprocess,'run',return_value=result):
                self.assertEqual(setup_helper.queue_business_message({'codex_path':'existing'},path,THREAD)['state'],'rejected')

    def test_deadline_is_checked_after_capability_probe_before_queue(self):
        with patch.object(setup_helper,'business_queue_entry',return_value='codex'),patch.object(setup_helper.subprocess,'run') as run:
            self.assertEqual(setup_helper.queue_business_message({'codex_path':'existing'},Path('message'),THREAD,expires_at=time.time()-1)['state'],'expired')
            run.assert_not_called()


if __name__=='__main__':unittest.main()
