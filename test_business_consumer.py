import json
import datetime
from pathlib import Path
import queue
import tempfile
import time
import types
import unittest
from unittest.mock import patch

import business_consumer as consumer
import setup_helper
import test_business_bridge as fixtures
from reliability import acquire_instance_lock

THREAD=fixtures.THREAD
MESSAGE=fixtures.OTHER


class DetectionTests(unittest.TestCase):
    def test_cli_has_priority_when_both_are_installed(self):
        with patch.object(consumer,'cli_command',return_value=['installed.exe','app-server','--stdio']), \
                patch.object(setup_helper,'codex_desktop_registered',return_value=True) as app:
            self.assertEqual(setup_helper.business_wake_adapter({}),'cli')
            app.assert_not_called()

    def test_app_only_and_no_frontend_are_distinguished(self):
        for registered,expected in [(True,'app'),(False,'unavailable')]:
            with patch.object(consumer,'cli_command',side_effect=consumer.PeerError('missing')), \
                    patch.object(setup_helper,'codex_desktop_registered',return_value=registered):
                self.assertEqual(setup_helper.business_wake_adapter({}),expected)

    def test_native_path_with_spaces_is_not_a_shell_command(self):
        with tempfile.TemporaryDirectory(prefix='工具 中文 ') as directory:
            path=Path(directory)/'installed codex.exe';path.touch()
            self.assertEqual(consumer.cli_command({'codex_path':str(path)}),[str(path.resolve()),'app-server','--stdio'])

    def test_locked_consumer_is_reused_without_launch_or_pid_guessing(self):
        f=fixtures.BusinessBridgeTests(methodName='test_local_self_registration_is_automatic_idempotent_and_reaches_other_peer')
        f.setUp();self.addCleanup(f.doCleanups)
        lock,_=consumer.paths(f.root,f.config,'B',THREAD)
        with acquire_instance_lock(lock,dict(thread=THREAD)), \
                patch.object(consumer,'cli_command',return_value=['installed.exe']), \
                patch.object(consumer.subprocess,'Popen') as spawn:
            result=consumer.launch(f.root,f.config,'B',THREAD,time.time()+60)
        self.assertEqual(result['state'],'requested');spawn.assert_not_called()

    def test_new_background_host_uses_its_own_signal_group(self):
        f=fixtures.BusinessBridgeTests(methodName='test_local_self_registration_is_automatic_idempotent_and_reaches_other_peer')
        f.setUp();self.addCleanup(f.doCleanups)
        with patch.object(consumer,'cli_command',return_value=['native.exe']), \
                patch.object(consumer.subprocess,'CREATE_NO_WINDOW',0x8000000,create=True), \
                patch.object(consumer.subprocess,'CREATE_NEW_PROCESS_GROUP',0x200,create=True), \
                patch.object(consumer.subprocess,'Popen') as spawn:
            consumer.launch(f.root,f.config,'B',THREAD,time.time()+60)
        self.assertEqual(spawn.call_args.kwargs['creationflags'],0x8000200)
        self.assertTrue(spawn.call_args.kwargs['stderr'].name.endswith(THREAD+'-stderr.log'))

    def test_server_approval_request_is_never_answered_automatically(self):
        rpc=object.__new__(consumer.StdioClient);rpc.messages=queue.Queue()
        rpc.messages.put(dict(id=5,method='item/commandExecution/requestApproval',params={}))
        with self.assertRaises(consumer.RpcError):rpc.receive(1)


class FakeClient:
    def __init__(self, thread=THREAD, queued=None, prior='completed', timeout_start=False, older=False):
        self.thread=thread;self.pending=list(queued or [MESSAGE]);self.prior=prior
        self.timeout_start=timeout_start;self.calls=[];self.events=[];self.closed=False
        self.older=older

    def send(self, message):
        self.calls.append((message.get('method'),message.get('params')))

    def call(self, method, params, timeout=20):
        self.calls.append((method,params))
        if method=='initialize':return {}
        if method=='thread/turns/list':
            if self.older:raise consumer.RpcError('method missing',code=self.older if type(self.older) is int else -32601)
            return dict(data=[dict(id='prior',status=self.prior)])
        if method=='thread/read':return dict(thread=dict(id=self.thread,turns=[dict(id='prior',status=self.prior)]))
        if method=='thread/resume':return dict(thread=dict(id=self.thread,status=dict(type='idle')))
        if method=='thread/queue/list':return dict(data=[dict(id=p) for p in self.pending])
        if method=='thread/queue/start':
            self.pending.remove(params['queuedSubmissionId'])
            self.events.append(dict(method='turn/completed',params=dict(threadId=self.thread,turn=dict(id='owned-turn',status='completed'))))
            if self.timeout_start:raise TimeoutError
            return dict(turn=dict(id='owned-turn',status='inProgress'))
        if method=='turn/interrupt':return {}
        raise AssertionError(method)

    def close(self):self.closed=True

    def receive(self,timeout):raise TimeoutError


class ExpiryClient(FakeClient):
    def __init__(self, expired, native, delete_mode='ok', **kwargs):
        super().__init__(queued=[expired,MESSAGE],**kwargs)
        self.expired=expired;self.native=native;self.delete_mode=delete_mode

    def call(self,method,params,timeout=20):
        if method=='thread/queue/delete':
            self.calls.append((method,params))
            if self.delete_mode=='unsupported':raise consumer.RpcError('unsupported',code=-32601)
            if self.delete_mode!='timeout_pending':self.pending.remove(params['queuedSubmissionId'])
            if self.delete_mode.startswith('timeout'):raise TimeoutError
            return dict(deleted=True)
        result=super().call(method,params,timeout)
        if method=='thread/queue/list':
            result['data']=[self.native if item['id']==self.expired else item for item in result['data']]
        return result


class ConsumerTests(unittest.TestCase):
    def setUp(self):
        f=fixtures.BusinessBridgeTests(methodName='test_local_self_registration_is_automatic_idempotent_and_reaches_other_peer')
        f.setUp();self.addCleanup(f.doCleanups);self.f=f
        self.identity,_=f.request();self.bridge=f.bridges['B']
        self.bridge.peer['workdir']=str(f.root)
        row=self.bridge.store.meta('business_job:'+self.identity)
        row.update(state='queued',queue_message_id=MESSAGE)
        self.bridge.store.set_meta('business_job:'+self.identity,row)
        self.request=dict(thread=THREAD,peer='B',project_id='',expires_at=time.time()+60)

    def run_consumer(self, fake):
        c=consumer.Consumer(self.f.root,self.f.config,self.request,client_factory=lambda *args:fake,keep_alive=False)
        with patch.object(consumer,'cli_command',return_value=['installed.exe','app-server','--stdio']):c.run()
        return json.loads(c.state_path.read_text(encoding='utf-8')),fake.calls

    def expired_job(self):
        import hashlib
        expired='33333333-3333-4333-8333-333333333333'
        identity='aaaaaaaaaaaa';row=self.bridge.store.meta('business_job:'+self.identity)
        row=dict(row,id=identity,queue_message_id=expired,expires_at=time.time()-60)
        self.bridge.store.set_meta('business_job:'+identity,row)
        message=self.bridge.runtime/(identity+'-message.txt');message.write_text('仅属于已过期项目的旧任务',encoding='utf-8')
        ref=setup_helper.business_message_reference(message,hashlib.sha256(message.read_bytes()).hexdigest())
        return expired,dict(id=expired,input=[dict(type='text',text=ref,text_elements=[])]),message

    def test_exact_expired_connector_message_is_retired_before_today_resumes(self):
        expired,native,message=self.expired_job();original=message.read_bytes()
        fake=ExpiryClient(expired,native);state,calls=self.run_consumer(fake)
        self.assertEqual(state['state'],'idle');self.assertEqual(message.read_bytes(),original)
        self.assertEqual([p['queuedSubmissionId'] for m,p in calls if m=='thread/queue/delete'],[expired])
        self.assertEqual([p['queuedSubmissionId'] for m,p in calls if m=='thread/queue/start'],[MESSAGE])
        methods=[m for m,_ in calls];self.assertLess(methods.index('thread/queue/delete'),methods.index('thread/resume'))
        _,path=consumer.paths(self.f.root,self.f.config,'B',THREAD)
        record=json.loads((path.parent/('cancel-expired-'+expired+'.json')).read_text(encoding='utf-8'))
        self.assertTrue(record['native_absence_verified']);self.assertTrue(record['delete_acknowledged'])

    def test_expired_message_with_changed_native_content_is_preserved(self):
        for change in ('text','attachment'):
            with self.subTest(change=change):
                expired,native,_=self.expired_job()
                if change=='text':native['input'][0]['text']+=' additional unowned instruction'
                else:native['input'].append(dict(type='image',url='private'))
                state,calls=self.run_consumer(ExpiryClient(expired,native))
                self.assertEqual(state['state'],'blocked')
                self.assertFalse(any(m in ('thread/queue/delete','thread/resume','thread/queue/start') for m,_ in calls))

    def test_other_coordinator_or_missing_original_file_cannot_be_retired(self):
        for change in ('source','file'):
            with self.subTest(change=change):
                expired,native,message=self.expired_job()
                if change=='source':
                    row=self.bridge.store.meta('business_job:aaaaaaaaaaaa');row['source']='someone-else'
                    self.bridge.store.set_meta('business_job:aaaaaaaaaaaa',row)
                else:message.unlink()
                state,calls=self.run_consumer(ExpiryClient(expired,native))
                self.assertEqual(state['state'],'blocked')
                self.assertFalse(any(m in ('thread/queue/delete','thread/resume') for m,_ in calls))

    def test_unknown_cancellation_ack_is_verified_by_reading_without_retry(self):
        expired,native,_=self.expired_job();fake=ExpiryClient(expired,native,delete_mode='timeout_removed')
        state,calls=self.run_consumer(fake)
        self.assertEqual(state['state'],'idle')
        self.assertEqual(len([m for m,_ in calls if m=='thread/queue/delete']),1)
        self.assertEqual([p['queuedSubmissionId'] for m,p in calls if m=='thread/queue/start'],[MESSAGE])

    def test_unresolved_cancellation_is_not_retried_after_restart(self):
        expired,native,_=self.expired_job()
        state,calls=self.run_consumer(ExpiryClient(expired,native,delete_mode='timeout_pending'))
        self.assertEqual(state['state'],'blocked')
        self.assertEqual(len([m for m,_ in calls if m=='thread/queue/delete']),1)
        state,calls=self.run_consumer(ExpiryClient(expired,native))
        self.assertEqual(state['state'],'blocked')
        self.assertFalse(any(m in ('thread/queue/delete','thread/resume') for m,_ in calls))

    def test_foreign_message_prevents_even_owned_expired_queue_mutation(self):
        expired,native,_=self.expired_job();fake=ExpiryClient(expired,native);fake.pending.append(THREAD)
        state,calls=self.run_consumer(fake)
        self.assertEqual(state['state'],'blocked')
        self.assertFalse(any(m in ('thread/queue/delete','thread/resume') for m,_ in calls))

    def test_rejected_native_cancellation_has_no_fallback_queue_write(self):
        expired,native,_=self.expired_job();state,calls=self.run_consumer(ExpiryClient(expired,native,delete_mode='unsupported'))
        self.assertEqual(state['state'],'blocked')
        self.assertEqual(len([m for m,_ in calls if m=='thread/queue/delete']),1)
        self.assertFalse(any(m in ('thread/resume','thread/queue/start') for m,_ in calls))

    def test_progress_metadata_change_does_not_change_expired_input_ownership(self):
        expired,native,_=self.expired_job()
        class ProgressClient(ExpiryClient):
            def call(inner,method,params,timeout=20):
                if method=='thread/queue/list':
                    row=self.bridge.store.meta('business_job:aaaaaaaaaaaa')
                    row['reason']=str(time.time())
                    self.bridge.store.set_meta('business_job:aaaaaaaaaaaa',row)
                return super().call(method,params,timeout)
        state,calls=self.run_consumer(ProgressClient(expired,native))
        self.assertEqual(state['state'],'idle')
        self.assertEqual(len([m for m,_ in calls if m=='thread/queue/delete']),1)

    def test_delete_ack_does_not_replace_native_absence_verification(self):
        expired,native,_=self.expired_job()
        class UnchangedClient(ExpiryClient):
            def call(inner,method,params,timeout=20):
                if method=='thread/queue/delete':
                    inner.calls.append((method,params));return dict(deleted=True)
                return super().call(method,params,timeout)
        state,calls=self.run_consumer(UnchangedClient(expired,native))
        self.assertEqual(state['state'],'blocked')
        self.assertFalse(any(m in ('thread/resume','thread/queue/start') for m,_ in calls))

    def test_previous_failure_survives_a_new_host_and_idle_status(self):
        _,state=consumer.paths(self.f.root,self.f.config,'B',THREAD)
        previous=dict(thread=THREAD,queue_message_id=MESSAGE,state='blocked',reason='后台接口已断开',checked_at=time.time())
        state.write_text(json.dumps(previous),encoding='utf-8')
        c=consumer.Consumer(self.f.root,self.f.config,self.request)
        c.status('starting','新宿主开始');c.status('idle','保留宿主')
        failure=json.loads(state.with_name(THREAD+'-last-failure.json').read_text(encoding='utf-8'))
        self.assertEqual(failure,previous)

    def test_consumes_only_exact_original_message_without_prompt_or_permission_overrides(self):
        fake=FakeClient();state,calls=self.run_consumer(fake)
        starts=[p for m,p in calls if m=='thread/queue/start']
        self.assertEqual(starts,[dict(threadId=THREAD,queuedSubmissionId=MESSAGE)])
        self.assertEqual([p for m,p in calls if m=='thread/resume'],[dict(threadId=THREAD)])
        self.assertFalse(any(m in ('turn/start','thread/start','thread/queue/add') for m,_ in calls))
        self.assertEqual(state['state'],'idle');self.assertTrue(fake.closed)

    def test_unfinished_previous_turn_blocks_parallel_execution(self):
        state,calls=self.run_consumer(FakeClient(prior='inProgress'))
        self.assertEqual(state['state'],'blocked')
        self.assertFalse(any(m in ('thread/resume','thread/queue/start') for m,_ in calls))

    def test_foreign_queue_head_is_not_skipped_or_consumed(self):
        state,calls=self.run_consumer(FakeClient(queued=[THREAD,MESSAGE]))
        self.assertEqual(state['state'],'blocked')
        self.assertFalse(any(m in ('thread/resume','thread/queue/start') for m,_ in calls))

    def test_native_restore_cannot_auto_start_an_expired_handoff(self):
        expired='33333333-3333-4333-8333-333333333333'
        row=self.bridge.store.meta('business_job:'+self.identity)
        self.bridge.store.set_meta('business_job:yesterday',dict(row,id='yesterday',
                                  queue_message_id=expired,expires_at=time.time()-1))
        class AutoClient(FakeClient):
            executed=[]
            def call(inner,method,params,timeout=20):
                if method=='thread/resume':inner.executed.extend(inner.pending)
                return super().call(method,params,timeout)
        fake=AutoClient(queued=[expired,MESSAGE]);state,calls=self.run_consumer(fake)
        self.assertEqual(fake.executed,[])
        self.assertEqual(state['state'],'blocked')
        self.assertFalse(any(m in ('thread/resume','thread/queue/delete','thread/queue/start') for m,_ in calls))

    def test_foreign_queue_tail_on_next_page_blocks_restore(self):
        class PagedClient(FakeClient):
            def call(inner,method,params,timeout=20):
                if method=='thread/queue/list':
                    inner.calls.append((method,params))
                    if params.get('cursor')=='second':return dict(data=[dict(id=THREAD)],nextCursor=None)
                    return dict(data=[dict(id=MESSAGE)],nextCursor='second')
                return super().call(method,params,timeout)
        state,calls=self.run_consumer(PagedClient())
        self.assertEqual(state['state'],'blocked')
        self.assertTrue(any(p.get('cursor')=='second' for m,p in calls if m=='thread/queue/list'))
        self.assertFalse(any(m in ('thread/resume','thread/queue/start') for m,_ in calls))

    def test_changed_binding_during_restore_precheck_never_restores(self):
        class ChangedClient(FakeClient):
            def call(inner,method,params,timeout=20):
                result=super().call(method,params,timeout)
                if method=='thread/queue/list':
                    policy=json.loads(self.bridge.policy_path.read_text(encoding='utf-8'))
                    policy['thread']=MESSAGE
                    self.bridge.policy_path.write_text(json.dumps(policy),encoding='utf-8')
                return result
        state,calls=self.run_consumer(ChangedClient())
        self.assertEqual(state['state'],'blocked')
        self.assertFalse(any(m in ('thread/resume','thread/queue/start') for m,_ in calls))

    def test_unreadable_or_incomplete_native_queue_never_restores(self):
        for mode in ('unavailable','timeout','shape','cursor_loop'):
            with self.subTest(mode=mode):
                class UnreadableClient(FakeClient):
                    def call(inner,method,params,timeout=20):
                        if method=='thread/queue/list':
                            inner.calls.append((method,params))
                            if mode=='unavailable':raise consumer.RpcError('unavailable',code=-32601)
                            if mode=='timeout':raise TimeoutError
                            if mode=='shape':return dict(data='not-a-queue')
                            return dict(data=[dict(id=MESSAGE)],nextCursor='same-page')
                        return super().call(method,params,timeout)
                state,calls=self.run_consumer(UnreadableClient())
                self.assertEqual(state['state'],'blocked')
                self.assertFalse(any(m in ('thread/resume','thread/queue/start') for m,_ in calls))

    def test_native_restore_cannot_repeat_an_unknown_original_start(self):
        _,state=consumer.paths(self.f.root,self.f.config,'B',THREAD)
        attempt=state.parent/('attempt-'+MESSAGE+'.json')
        attempt.write_text(json.dumps(dict(thread=THREAD,queue_message_id=MESSAGE,
                                          started_at=time.time(),outcome='unknown')),encoding='utf-8')
        class AutoClient(FakeClient):
            executed=False
            def call(inner,method,params,timeout=20):
                if method=='thread/resume':inner.executed=True
                return super().call(method,params,timeout)
        fake=AutoClient();result,calls=self.run_consumer(fake)
        self.assertFalse(fake.executed)
        self.assertEqual(result['state'],'blocked')
        self.assertFalse(any(m in ('thread/resume','thread/queue/start') for m,_ in calls))

    def test_expiry_during_restore_precheck_never_restores(self):
        class ExpiredClient(FakeClient):
            def call(inner,method,params,timeout=20):
                result=super().call(method,params,timeout)
                if method=='thread/queue/list':self.request['expires_at']=time.time()-1
                return result
        state,calls=self.run_consumer(ExpiredClient())
        self.assertEqual(state['state'],'blocked')
        self.assertFalse(any(m in ('thread/resume','thread/queue/start') for m,_ in calls))

    def test_queue_pagination_has_a_finite_read_limit(self):
        class EndlessClient(FakeClient):
            pages=0
            def call(inner,method,params,timeout=20):
                if method=='thread/queue/list':
                    inner.calls.append((method,params));inner.pages+=1
                    return dict(data=[],nextCursor=str(inner.pages))
                return super().call(method,params,timeout)
        fake=EndlessClient();state,calls=self.run_consumer(fake)
        self.assertEqual(state['state'],'blocked');self.assertEqual(fake.pages,8)
        self.assertFalse(any(m in ('thread/resume','thread/queue/start') for m,_ in calls))

    def test_fully_authorized_paged_queue_is_consumed_once_in_order(self):
        second='33333333-3333-4333-8333-333333333333'
        row=self.bridge.store.meta('business_job:'+self.identity)
        self.bridge.store.set_meta('business_job:second',dict(row,id='second',queue_message_id=second))
        class PagedClient(FakeClient):
            def call(inner,method,params,timeout=20):
                if method=='thread/queue/list':
                    inner.calls.append((method,params))
                    if params.get('cursor')=='tail':return dict(data=[dict(id=p) for p in inner.pending[1:]],nextCursor=None)
                    return dict(data=[dict(id=p) for p in inner.pending[:1]],nextCursor='tail' if len(inner.pending)>1 else None)
                return super().call(method,params,timeout)
        state,calls=self.run_consumer(PagedClient(queued=[MESSAGE,second]))
        self.assertEqual(state['state'],'idle')
        self.assertEqual([p['queuedSubmissionId'] for m,p in calls if m=='thread/queue/start'],[MESSAGE,second])

    def test_changed_binding_or_expiry_never_starts_native_client(self):
        for change in ('binding','expiry'):
            with self.subTest(change=change):
                if change=='binding':
                    policy=json.loads(self.bridge.policy_path.read_text(encoding='utf-8'))
                    policy['thread']=MESSAGE
                    self.bridge.policy_path.write_text(json.dumps(policy),encoding='utf-8')
                else:self.request['expires_at']=time.time()-1
                fake=FakeClient();state,calls=self.run_consumer(fake)
                self.assertEqual(state['state'],'idle');self.assertEqual(calls,[])

    def test_unknown_start_ack_does_not_retry_and_waits_for_original_completion(self):
        state,calls=self.run_consumer(FakeClient(timeout_start=True))
        self.assertEqual(len([m for m,_ in calls if m=='thread/queue/start']),1)
        self.assertEqual(state['state'],'turn_completed')

    def test_all_queue_control_calls_use_exact_bound_thread(self):
        state,calls=self.run_consumer(FakeClient())
        self.assertEqual(state['state'],'idle')
        self.assertTrue(all(p.get('threadId')==THREAD for m,p in calls if m.startswith('thread/')))

    def test_older_read_only_history_api_is_detected_without_prompt_fallback(self):
        for code in [-32601,-32600]:
            with self.subTest(code=code):
                attempt=consumer.paths(self.f.root,self.f.config,'B',THREAD)[1].parent/('attempt-'+MESSAGE+'.json')
                if attempt.exists():attempt.unlink()
                state,calls=self.run_consumer(FakeClient(older=code))
                self.assertEqual(state['state'],'idle')
                self.assertEqual(len([m for m,_ in calls if m=='thread/read']),1)
                self.assertEqual(len([m for m,_ in calls if m=='thread/queue/start']),1)

    def test_two_authorized_queued_messages_are_each_consumed_once_in_order(self):
        second='33333333-3333-4333-8333-333333333333'
        row=self.bridge.store.meta('business_job:'+self.identity)
        self.bridge.store.set_meta('business_job:second',dict(row,id='second',queue_message_id=second))
        state,calls=self.run_consumer(FakeClient(queued=[MESSAGE,second]))
        self.assertEqual([p['queuedSubmissionId'] for m,p in calls if m=='thread/queue/start'],[MESSAGE,second])
        self.assertEqual(state['state'],'idle')

    def test_daily_deadline_interrupts_only_owned_turn_and_starts_nothing_new(self):
        c=consumer.Consumer(self.f.root,self.f.config,self.request)
        fake=FakeClient();c.client=fake;c.turn_id='owned-turn'
        c.request['expires_at']=time.time()-1
        with self.assertRaises(consumer.RpcError):c.wait_turn()
        self.assertEqual(fake.calls,[('turn/interrupt',dict(threadId=THREAD,turnId='owned-turn'))])

    def test_existing_start_attempt_survives_restart_and_is_never_reissued(self):
        self.run_consumer(FakeClient())
        with patch.object(consumer,'queue_start_schema',return_value=dict(available=True,fields=['threadId','itemId'])):
            state,calls=self.run_consumer(FakeClient())
        self.assertEqual(state['state'],'blocked')
        self.assertFalse(any(m=='thread/queue/start' for m,_ in calls))

    def test_only_explicit_legacy_invalid_request_with_pending_id_can_recover_once(self):
        _,state=consumer.paths(self.f.root,self.f.config,'B',THREAD)
        started=time.time()
        old=dict(thread=THREAD,queue_message_id=MESSAGE,started_at=started)
        attempt=state.parent/('attempt-'+MESSAGE+'.json')
        attempt.write_text(json.dumps(old),encoding='utf-8')
        log=self.f.root/'runtime/B/connector.log'
        log.write_text(json.dumps(dict(time=datetime.datetime.fromtimestamp(started).strftime('%Y-%m-%d %H:%M:%S'),
                                      event='business_consumer_status',state='blocked',
                                      summary='本机后台接口拒绝请求，错误码-32600'),ensure_ascii=False)+'\n',encoding='utf-8')
        result,calls=self.run_consumer(FakeClient())
        self.assertEqual(result['state'],'idle')
        self.assertEqual(len([m for m,_ in calls if m=='thread/queue/start']),1)
        ledger=json.loads(attempt.read_text(encoding='utf-8'))
        self.assertEqual(ledger['retry_count'],1)
        self.assertEqual(ledger['previous_attempt']['outcome'],'rejected')
        with patch.object(consumer,'queue_start_schema',return_value=dict(available=True)):
            result,calls=self.run_consumer(FakeClient())
        self.assertEqual(result['state'],'blocked')
        self.assertFalse(any(m=='thread/queue/start' for m,_ in calls))

    def test_timeout_or_distant_log_is_never_treated_as_explicit_rejection(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'connector.log'
            for delta,summary in [(0,'后台回执超时'),(60,'本机后台接口拒绝请求，错误码-32600')]:
                started=time.time()
                path.write_text(json.dumps(dict(time=datetime.datetime.fromtimestamp(started+delta).strftime('%Y-%m-%d %H:%M:%S'),
                                                event='business_consumer_status',state='blocked',summary=summary),ensure_ascii=False),encoding='utf-8')
                self.assertIsNone(consumer.rejected_start_from_legacy_log(dict(thread=THREAD,queue_message_id=MESSAGE,started_at=started),path))

    def test_explicit_resume_required_rejection_recovers_with_native_resume_and_same_id(self):
        _,state=consumer.paths(self.f.root,self.f.config,'B',THREAD)
        reason='本机后台接口thread/queue/start拒绝请求，错误码-32600：resume the thread before starting a queued message'
        attempt=state.parent/('attempt-'+MESSAGE+'.json')
        attempt.write_text(json.dumps(dict(thread=THREAD,queue_message_id=MESSAGE,started_at=time.time(),outcome='rejected',error_code=-32600,retry_count=1)),encoding='utf-8')
        state.write_text(json.dumps(dict(thread=THREAD,queue_message_id=MESSAGE,state='blocked',reason=reason)),encoding='utf-8')
        result,calls=self.run_consumer(FakeClient())
        self.assertEqual(result['state'],'idle')
        self.assertEqual([m for m,_ in calls if m in ('thread/resume','thread/queue/start')],['thread/resume','thread/queue/start'])
        self.assertEqual(json.loads(attempt.read_text(encoding='utf-8'))['retry_count'],2)

    def test_one_started_turn_does_not_claim_other_queued_handoffs_have_started(self):
        second='33333333-3333-4333-8333-333333333333'
        row=self.bridge.store.meta('business_job:'+self.identity)
        self.bridge.store.set_meta('business_job:second',dict(row,id='second',queue_message_id=second))
        _,state=consumer.paths(self.f.root,self.f.config,'B',THREAD)
        state.write_text(json.dumps(dict(thread=THREAD,queue_message_id=MESSAGE,state='turn_started',reason='原会话本轮开始')),encoding='utf-8')
        self.bridge.wake_tick(start=False)
        self.assertEqual(self.bridge.store.meta('business_job:'+self.identity)['reason'],'原会话本轮开始')
        self.assertNotEqual(self.bridge.store.meta('business_job:second').get('reason'),'原会话本轮开始')

    def test_native_auto_drain_is_awaited_without_starting_the_same_message_again(self):
        class AutoClient(FakeClient):
            def call(inner,method,params,timeout=20):
                result=super().call(method,params,timeout)
                if method=='thread/resume':
                    inner.pending=[]
                    inner.events.extend([dict(method='turn/started',params=dict(threadId=THREAD,turn=dict(id='native-auto'))),
                                         dict(method='turn/completed',params=dict(threadId=THREAD,turn=dict(id='native-auto',status='completed')))])
                    result['thread']['status']=dict(type='active')
                return result
        fake=AutoClient();state,calls=self.run_consumer(fake)
        self.assertEqual(state['state'],'idle')
        self.assertFalse(any(m=='thread/queue/start' for m,_ in calls))
        self.assertEqual(fake.events,[])

    def test_idle_host_stays_alive_between_turns_until_its_deadline(self):
        c=consumer.Consumer(self.f.root,self.f.config,self.request)
        class IdleClient(FakeClient):
            def __init__(inner):super().__init__();inner.pending=[];inner.polls=0
            def receive(inner,timeout):
                self.assertFalse(inner.closed)
                inner.polls+=1
                if inner.polls>=4:c.request['expires_at']=time.time()-1
                raise TimeoutError
        fake=IdleClient();c.client_factory=lambda *a:fake
        with patch.object(consumer,'cli_command',return_value=['native.exe']):c.run()
        self.assertGreaterEqual(fake.polls,4)
        self.assertTrue(fake.closed)
        self.assertEqual(json.loads(c.state_path.read_text(encoding='utf-8'))['state'],'deadline')


class ExecutionHealthTests(unittest.TestCase):
    def setUp(self):
        f=fixtures.BusinessBridgeTests(methodName='test_local_self_registration_is_automatic_idempotent_and_reaches_other_peer')
        f.setUp();self.addCleanup(f.doCleanups);self.f=f
        f.config['peers'][1]['workdir']=str(f.root)
        (f.root/'.local').mkdir(exist_ok=True)
        (f.root/'.local/machine.json').write_text(json.dumps(dict(peer='B')),encoding='utf-8')
        _,state=consumer.paths(f.root,f.config,'B',THREAD);self.folder=state.parent
        self.turn='33333333-3333-4333-8333-333333333333'

    def test_error_categories_never_include_error_message_or_credentials(self):
        d=consumer.public_turn_metadata(dict(id=self.turn,status='failed',error=dict(codexErrorInfo='unauthorized',message='401 api_key=private-value token=another-value'),items=[]))
        self.assertEqual(d['error_category'],'模型登录或鉴权')
        self.assertEqual(d['error_info'],'unauthorized')
        self.assertNotIn('private-value',json.dumps(d));self.assertNotIn('another-value',json.dumps(d))

    def test_host_failure_and_python_crash_are_available_without_loading_a_thread(self):
        state=self.folder/(THREAD+'-state.json')
        state.write_text(json.dumps(dict(thread=THREAD,state='blocked',reason='token=hidden-value https://private.invalid/ sk-private-value',checked_at=time.time(),wire='not public')),encoding='utf-8')
        (self.folder/(THREAD+'-stderr.log')).write_text('Traceback (most recent call last):\n  File "C:/private/business_consumer.py", line 421, in run\nTypeError: token=another-secret\n',encoding='utf-8')
        called=[]
        d=consumer.recent_execution_health(self.f.root,self.f.config,THREAD,[MESSAGE],client_factory=lambda *a:called.append(a))
        self.assertEqual(called,[])
        self.assertEqual(d['consumer_state']['state'],'blocked')
        self.assertEqual(d['last_python_error']['type'],'TypeError')
        self.assertEqual(d['last_python_error']['source_lines'],[421])
        for value in ['hidden-value','private.invalid','private-value','not public','another-secret','C:/private']:
            self.assertNotIn(value,json.dumps(d))

    def test_host_state_from_another_thread_or_unrequested_queue_is_not_returned(self):
        state=self.folder/(THREAD+'-state.json')
        state.write_text(json.dumps(dict(thread=MESSAGE,state='blocked',reason='unrelated state')),encoding='utf-8')
        (self.folder/(THREAD+'-last-failure.json')).write_text(json.dumps(dict(thread=THREAD,queue_message_id=self.turn,state='blocked',reason='unrequested message')),encoding='utf-8')
        d=consumer.recent_execution_health(self.f.root,self.f.config,THREAD,[MESSAGE],client_factory=lambda *a:None)
        self.assertNotIn('consumer_state',d);self.assertNotIn('last_consumer_failure',d)

    def test_no_start_receipt_never_loads_or_starts_any_native_thread(self):
        called=[]
        d=consumer.recent_execution_health(self.f.root,self.f.config,THREAD,[MESSAGE],client_factory=lambda *a:called.append(a))
        self.assertFalse(d['available']);self.assertEqual(called,[])

    def test_read_only_turn_diagnostics_are_scoped_to_exact_start_receipts(self):
        (self.folder/('attempt-'+MESSAGE+'.json')).write_text(json.dumps(dict(thread=THREAD,queue_message_id=MESSAGE,turn_id=self.turn,outcome='started')),encoding='utf-8')
        class Client:
            def __init__(self):self.calls=[];self.closed=False
            def call(inner,method,params,timeout=5):
                inner.calls.append((method,params))
                if method=='initialize':return {}
                return dict(data=[dict(id=self.turn,status='failed',error=dict(codexErrorInfo='rateLimitExceeded',message='rate limit token=secret'),items=[dict(type='agentMessage',text='本轮未执行。token=secret sk-private-key https://private.invalid/?access_token=secret')]),dict(id=THREAD,status='completed',items=[dict(type='agentMessage',text='unrelated private conversation')])])
            def send(inner,data):inner.calls.append((data['method'],data.get('params')))
            def close(inner):inner.closed=True
        client=Client()
        with patch.object(consumer,'cli_command',return_value=['native.exe']):
            d=consumer.recent_execution_health(self.f.root,self.f.config,THREAD,[MESSAGE],client_factory=lambda *a:client)
        self.assertTrue(d['available']);self.assertEqual(len(d['records']),1)
        self.assertEqual(d['records'][0]['turn_id'],self.turn)
        self.assertEqual(d['records'][0]['error_info'],'rateLimitExceeded')
        self.assertNotIn('secret',json.dumps(d));self.assertNotIn('private conversation',json.dumps(d))
        self.assertNotIn('private-key',json.dumps(d));self.assertNotIn('private.invalid',json.dumps(d))
        self.assertTrue(d['records'][0]['agent_reply_excerpt'].startswith('本轮未执行'))
        self.assertTrue(client.closed)
        self.assertEqual([m for m,_ in client.calls],['initialize','initialized','thread/turns/list'])

    def test_external_host_turn_is_matched_by_our_exact_file_digest_and_response_format_is_reported(self):
        identity,_=self.f.request();b=self.f.bridges['B']
        row=b.store.meta('business_job:'+identity);row.update(state='queued',queue_message_id=MESSAGE)
        b.store.set_meta('business_job:'+identity,row)
        message=b.runtime/(identity+'-message.txt');message.write_text('本项目已授权的唯一委派',encoding='utf-8')
        import hashlib
        digest=hashlib.sha256(message.read_bytes()).hexdigest()
        response=b.runtime/(identity+'-response.json')
        response.write_text(json.dumps(dict(handoff_id=identity,status='ready',summary='格式核对')),encoding='utf-16')
        from peer import business_frame
        frame=b.outgoing('result','A',identity,thread=THREAD,state='ready',summary='private result body secret',result_seq=1)
        b.store.set_meta('business_outbox:diagnostic',dict(wire=business_frame(frame),state='unknown',attempts=1,reason='DeliveryUnknown'))
        class Client:
            def call(inner,method,params,timeout=5):
                if method=='initialize':return {}
                items=[dict(type='agentMessage',text='已给本任务阶段反馈')]
                if params.get('itemsView')=='full':items.insert(0,dict(type='userMessage',content=[dict(type='text',text='SHA-256：'+digest)]))
                return dict(data=[dict(id=self.turn,status='completed',items=items)])
            def send(inner,data):pass
            def close(inner):pass
        with patch.object(consumer,'cli_command',return_value=['native.exe']):
            result=consumer.recent_execution_health(self.f.root,self.f.config,THREAD,[MESSAGE],client_factory=lambda *a:Client())
        self.assertTrue(result['available']);self.assertEqual(len(result['records']),1)
        record=result['records'][0]
        self.assertEqual(record['queue_message_id'],MESSAGE)
        self.assertEqual(record['response_encoding'],'utf-16')
        self.assertTrue(record['response_schema_valid'])
        self.assertEqual(record['result_outbox']['state'],'unknown')
        self.assertEqual(record['result_outbox']['reason_class'],'DeliveryUnknown')
        self.assertNotIn('private result body',json.dumps(result))
        self.assertNotIn(digest,json.dumps(result))


if __name__=='__main__':unittest.main()
