"""Consume connector-owned submissions in an existing local Codex thread.

No prompt replay, global daemon restart, version changes or approval overrides.
The small owned stdio host survives a connector update and holds a project lock.
"""
import argparse
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import queue
import re
import sqlite3
import subprocess
import threading
import time

from connector_update import safe_path, save_json
from reliability import acquire_instance_lock, instance_lock_owner, PeerAlreadyRunning, PeerError

UUID = r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}'


def background_flags():
    return getattr(subprocess,'CREATE_NO_WINDOW',0)|getattr(subprocess,'CREATE_NEW_PROCESS_GROUP',0)


def cli_command(config):
    """Use the selected installed entry, without shell interpolation or upgrades."""
    path = Path(config['codex_path'])
    if path.suffix.lower() == '.ps1' and path.with_suffix('.cmd').is_file():
        path = path.with_suffix('.cmd')
    if not path.is_file() or path.suffix.lower() not in ('.exe', '.cmd', '.bat'):
        raise PeerError('未找到可直接运行的现有Codex CLI入口。')
    return [str(path.resolve()), 'app-server', '--stdio']


def queue_start_schema(config, folder):
    """Read this installed version's own contract, never infer it from another version."""
    schema_folder=Path(folder)/'native-schema'
    schema_folder.mkdir(parents=True,exist_ok=True)
    command=cli_command(config)[:1]+['app-server','generate-json-schema','--out',str(schema_folder)]
    try:
        result=subprocess.run(command+['--experimental'],stdin=subprocess.DEVNULL,capture_output=True,timeout=20)
        if result.returncode and b'--experimental' in result.stderr and b'unexpected argument' in result.stderr:
            result=subprocess.run(command,stdin=subprocess.DEVNULL,capture_output=True,timeout=20)
        if result.returncode:
            return dict(available=False)
        files=list(schema_folder.glob('**/ThreadQueueStartParams.json'))
        if not files:
            return dict(available=False,generated=True)
        data=json.loads(files[0].read_text(encoding='utf-8'))
        properties=data.get('properties',{})
        names=[k for k in properties if re.fullmatch(r'[A-Za-z0-9_]{1,80}',k)]
        return dict(available=True,fields=names,required=[k for k in data.get('required',[]) if k in names])
    except (OSError,ValueError,subprocess.TimeoutExpired):
        return dict(available=False)


def paths(root, config, peer_id, thread):
    from peer import project_runtime
    if not re.fullmatch(UUID, thread):
        raise PeerError('后台消费会话编号无效。')
    folder = safe_path(project_runtime(root, config, peer_id), 'business-consumers')
    folder.mkdir(parents=True, exist_ok=True)
    return folder / (thread + '.lock'), folder / (thread + '-state.json')


def launch(root, config, peer_id, thread, expires_at):
    """Launch once; the OS lock, not PID text, identifies an existing consumer."""
    command = cli_command(config)
    lock, state = paths(root, config, peer_id, thread)
    if instance_lock_owner(lock) is not None:
        return dict(state='requested', reason='原会话已有连接器后台消费者，等待处理原队列和业务回报。')
    if time.time() >= expires_at:
        return dict(state='expired', reason='已到截止时间，未启动后台消费者。')
    request = state.with_name(thread + '-request.json')
    save_json(request, dict(thread=thread, peer=peer_id, expires_at=expires_at,
                           project_id=config.get('project_id', ''), created_at=time.time()))
    args = [os.sys.executable, str(Path(root) / 'business_consumer.py'), '--request', str(request)]
    # Popen returns only a launch receipt. A turn or business response proves execution.
    # Keep unexpected Python failures locally. Health shares only their type and
    # our own source line numbers, never the raw traceback or task contents.
    with state.with_name(thread + '-stderr.log').open('ab') as errors:
        subprocess.Popen(args, cwd=str(root), stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                         stderr=errors, creationflags=background_flags())
    return dict(state='requested', reason='已启动CLI原会话后台消费者，等待确认原队列处理和业务回报；没有重复派单。')


class RpcError(Exception):
    def __init__(self, message, code=None):
        super().__init__(message)
        self.code=code


def rejected_start_from_legacy_log(attempt, log_path):
    """Recover only an old explicit InvalidRequest rejection, never an unknown ACK."""
    if set(attempt)-{'thread','queue_message_id','started_at'} or type(attempt.get('started_at')) not in (int,float):
        return None
    try:
        with Path(log_path).open('rb') as handle:
            handle.seek(0,2);size=handle.tell();handle.seek(max(0,size-262144))
            lines=handle.read().decode('utf-8',errors='replace').splitlines()
        for line in reversed(lines):
            try:
                event=json.loads(line)
                stamp=datetime.datetime.strptime(event['time'],'%Y-%m-%d %H:%M:%S').timestamp()
            except (ValueError,KeyError,TypeError):
                continue
            if (event.get('event')=='business_consumer_status' and event.get('state')=='blocked'
                    and event.get('summary')=='本机后台接口拒绝请求，错误码-32600'
                    and -1<=stamp-attempt['started_at']<=15):
                return dict(code=-32600,evidence_time=event['time'],outcome='rejected')
    except OSError:
        pass
    return None


class StdioClient:
    def __init__(self, command, cwd):
        self.process = subprocess.Popen(command, cwd=cwd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.DEVNULL, creationflags=background_flags())
        self.messages = queue.Queue()
        self.events = []
        self.sequence = 0
        self.reader = threading.Thread(target=self.read, daemon=True)
        self.reader.start()

    def read(self):
        try:
            while True:
                raw = self.process.stdout.readline(8 * 1024 * 1024)
                if not raw or not raw.endswith(b'\n'):
                    break
                try:
                    data = json.loads(raw)
                except (ValueError, UnicodeError):
                    continue
                # Discard model, tool and command content. Keep only RPC control messages.
                if 'id' in data or data.get('method') in ('turn/started', 'turn/completed'):
                    self.messages.put(data)
        finally:
            self.messages.put(None)

    def send(self, data):
        self.process.stdin.write((json.dumps(data, ensure_ascii=False) + '\n').encode('utf-8'))
        self.process.stdin.flush()

    def receive(self, timeout):
        try:
            data = self.messages.get(timeout=max(.01, timeout))
        except queue.Empty:
            raise TimeoutError from None
        if data is None:
            raise RpcError('后台入口已退出。')
        if 'id' in data and data.get('method'):
            # Never auto-accept a permission request, tool call or interactive answer.
            raise RpcError('原会话要求人工确认或交互，未自动代答。')
        return data

    def call(self, method, params, timeout=20):
        self.sequence += 1
        identity = self.sequence
        self.send(dict(id=identity, method=method, params=params))
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            data = self.receive(deadline - time.monotonic())
            if data.get('id') == identity:
                if 'error' in data:
                    code=data['error'].get('code', 'unknown')
                    detail=''
                    if code==-32600:
                        from peer import clean_business_text
                        detail='：'+re.sub(r'sk-[A-Za-z0-9_-]+','<凭据已隐藏>',clean_business_text(str(data['error'].get('message',''))))[:220]
                    raise RpcError('本机后台接口'+method+'拒绝请求，错误码' + str(code)+detail, code=code)
                return data.get('result', {})
            if data.get('method'):
                self.events.append(data)
        raise TimeoutError

    def close(self):
        # Only this connector-owned stdio child is stopped. No global Codex process is touched.
        try:
            self.process.stdin.close()
            self.process.wait(timeout=8)
        except (OSError, subprocess.TimeoutExpired):
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.process.stdout.close()
        self.reader.join(timeout=2)


def public_turn_metadata(turn):
    """Return status/error categories only; model output and credentials stay local."""
    state=turn.get('status')
    if state not in ('completed','failed','interrupted','inProgress'):
        state='unknown'
    result=dict(turn_id=turn.get('id'),status=state)
    error=turn.get('error') or {}
    info=error.get('codexErrorInfo')
    allowed={'contextWindowExceeded','sessionBudgetExceeded','usageLimitExceeded','rateLimitExceeded',
             'flexUnavailable','serverOverloaded','modelNotFound','cyberPolicy','misalignmentPolicyViolation',
             'tooManyDenials','internalServerError','unauthorized','badRequest','threadRollbackFailed','sandboxError','other'}
    if isinstance(info,str):
        result['error_info']=info if info in allowed else 'other'
    elif isinstance(info,dict):
        kinds={'httpConnectionFailed','responseStreamConnectionFailed','responseStreamDisconnected','responseTooManyFailedAttempts','activeTurnNotSteerable'}
        result['error_info']=next((k for k in info if k in kinds),'other')
    if error:
        message=str(error.get('message','')).lower()
        categories=[(r'quota|usage.limit|rate.limit|capacity|overload','模型额度或容量'),
                    (r'unauthori|authenticat|api.key|401|not.logged|login','模型登录或鉴权'),
                    (r'model.*not.*(?:found|available)|unknown.model','模型不可用'),
                    (r'permission|sandbox|approval|access.denied','权限或交互'),
                    (r'network|connect|timed.out|deadline|stream|502|503','模型连接'),
                    (r'context.window|context.length','上下文限制')]
        result['error_category']=next((label for pattern,label in categories if re.search(pattern,message)),'会话执行失败')
    items=turn.get('items',[])
    result['agent_messages']=sum(item.get('type')=='agentMessage' for item in items if isinstance(item,dict))
    result['commands']=sum(item.get('type')=='commandExecution' for item in items if isinstance(item,dict))
    return result


def preserve_consumer_failure(state_path, data, thread):
    if isinstance(data,dict) and data.get('thread') == thread and data.get('state') in ('blocked', 'unknown'):
        safe = {key: data[key] for key in ('state', 'reason', 'checked_at', 'thread', 'queue_message_id', 'turn_id') if key in data}
        save_json(state_path.with_name(thread + '-last-failure.json'), safe)


def public_consumer_state(data, thread, queue_ids):
    if not isinstance(data,dict) or data.get('thread') != thread or (data.get('queue_message_id') and data['queue_message_id'] not in queue_ids):
        return None
    from peer import clean_business_text
    reason = clean_business_text(str(data.get('reason', '')))
    reason = re.sub(r'sk-[A-Za-z0-9_-]+', '<凭据已隐藏>', reason)
    reason = re.sub(r'https?://\S+', '<链接已隐藏>', reason)
    reason = re.sub(r'(?i)(--(?:api[-_]key|token|password|app[-_]secret)\s+)\S+', r'\1<凭据已隐藏>', reason)
    state=data.get('state')
    if state not in ('starting','idle','blocked','unknown','start_attempting','turn_started','turn_completed','deadline'): state='unknown'
    result = dict(state=state, reason=reason[:80])
    if type(data.get('checked_at')) in (int, float): result['checked_at'] = data['checked_at']
    return result


def recent_execution_health(root, config, thread, queue_ids, client_factory=StdioClient):
    """Read only turns named by this connector's exact start receipts, never other chats."""
    result=dict(available=False,records=[])
    client=None
    try:
        machine=json.loads((Path(root)/'.local/machine.json').read_text(encoding='utf-8-sig'))
        peer=next(p for p in config['peers'] if p['id']==machine['peer'])
        lock,state=paths(root,config,peer['id'],thread)
        owner=instance_lock_owner(lock)
        result['host_alive']=owner is not None
        if owner and type(owner.get('pid')) is int:result['host_pid']=owner['pid']
        for name, path in (('consumer_state', state), ('last_consumer_failure', state.with_name(thread+'-last-failure.json'))):
            try:
                public = public_consumer_state(json.loads(path.read_text(encoding='utf-8')), thread, queue_ids)
                if public: result[name] = public
            except (OSError, ValueError, TypeError): pass
        errors = state.with_name(thread+'-stderr.log')
        if errors.is_file():
            with errors.open('rb') as error_stream:
                error_stream.seek(0, 2); size = error_stream.tell(); error_stream.seek(max(0, size-16384))
                error_text = error_stream.read().decode('utf-8', 'replace')
            kinds = re.findall(r'(?m)^(TypeError|ValueError|AttributeError|KeyError|IndexError|RuntimeError|PermissionError|BrokenPipeError|TimeoutError|OSError|MemoryError|JSONDecodeError)(?::|$)', error_text)
            if kinds:
                result['last_python_error'] = dict(type=kinds[-1], captured_at=errors.stat().st_mtime,
                    source_lines=[int(n) for n in re.findall(r'File "[^"\r\n]*business_consumer\.py", line (\d+)', error_text)[-4:]])
        turns={}
        jobs={};digests={};outboxes={}
        database=(state.parent.parent/'state.sqlite3').resolve()
        if database.is_file():
            db=sqlite3.connect(database.as_uri()+'?mode=ro',uri=True,timeout=1)
            try:
                rows=db.execute("SELECT value FROM metadata WHERE key LIKE 'business_job:%'").fetchall()
                outgoing=db.execute("SELECT value FROM metadata WHERE key LIKE 'business_outbox:%'").fetchall()
            finally:
                db.close()
            for row in rows:
                job=json.loads(row[0]);identity=job.get('queue_message_id')
                if (identity in queue_ids and job.get('thread')==thread and job.get('direction')=='incoming'
                        and re.fullmatch(r'[0-9a-f]{12}',str(job.get('id','')))):
                    jobs[identity]=job
                    message=state.parent.parent/'business-jobs'/(job['id']+'-message.txt')
                    if message.is_file():digests[hashlib.sha256(message.read_bytes()).hexdigest()]=identity
            from peer import BUSINESS_PREFIX
            job_ids={job['id']:job for job in jobs.values()}
            for row in outgoing:
                entry=json.loads(row[0]);wire=entry.get('wire','')
                if not wire.startswith(BUSINESS_PREFIX):continue
                frame=json.loads(wire[len(BUSINESS_PREFIX):]);job=job_ids.get(frame.get('id'))
                if (not job or frame.get('kind')!='result' or frame.get('thread')!=thread
                        or frame.get('source')!=peer['id'] or frame.get('target')!=job.get('source')
                        or frame.get('project_id','')!=config.get('project_id','')):continue
                sequence=frame.get('result_seq',0)
                previous=outboxes.get(job['id'],{})
                if type(sequence) is not int or sequence<previous.get('result_seq',0):continue
                reason=str(entry.get('reason',''))
                category=reason if reason in ('PeerError','DeliveryUnknown','SendRejected','OSError') else 'other' if reason else ''
                if 'Webhook explicitly rejected' in reason:category='webhook_rejected'
                outboxes[job['id']]=dict(state=entry.get('state'),attempts=entry.get('attempts'),reason_class=category,
                                         result_seq=sequence,wire_chars=len(wire),wire_bytes=len(wire.encode('utf-8')))
        for identity in queue_ids:
            if not re.fullmatch(UUID,identity):
                return result
            attempt=state.parent/('attempt-'+identity+'.json')
            if attempt.is_file():
                data=json.loads(attempt.read_text(encoding='utf-8'))
                if data.get('thread')==thread and data.get('queue_message_id')==identity and re.fullmatch(UUID,str(data.get('turn_id',''))):
                    turns[data['turn_id']]=identity
        if not turns and not digests:
            return result
        client=client_factory(cli_command(config),peer['workdir'])
        client.call('initialize',dict(clientInfo=dict(name='dingtalk_peer_execution_health',version='1'),capabilities=dict(experimentalApi=True)),timeout=5)
        client.send(dict(method='initialized',params={}))
        data=client.call('thread/turns/list',dict(threadId=thread,limit=8,sortDirection='desc',itemsView='full'),timeout=5)
        try:
            version=subprocess.run(cli_command(config)[:1]+['--version'],stdin=subprocess.DEVNULL,capture_output=True,timeout=3)
            match=re.search(rb'codex-cli\s+([0-9][0-9A-Za-z.+_-]{1,60})',version.stdout)
            if match:result['backend_version']=match[1].decode('ascii')
        except (OSError,subprocess.TimeoutExpired):pass
        result['available']=True
        excerpts={}
        for turn in data.get('data',[]):
            matches={turns[turn['id']]} if turn.get('id') in turns else set()
            for item in turn.get('items',[]):
                if isinstance(item,dict) and item.get('type')=='userMessage':
                    value=json.dumps(item,ensure_ascii=False)
                    matches.update(identity for digest,identity in digests.items() if digest in value)
            for identity in reversed(queue_ids):
                if identity not in matches:continue
                record=dict(public_turn_metadata(turn),queue_message_id=identity)
                job=jobs.get(identity)
                if job:
                    record['connector_job_state']=str(job.get('state','unknown'))[:32]
                    record['result_saved']=bool(job.get('result_version'))
                    if job['id'] in outboxes:record['result_outbox']=outboxes[job['id']]
                    response=state.parent.parent/'business-jobs'/(job['id']+'-response.json')
                    record['response_file_exists']=response.is_file()
                    if response.is_file() and response.stat().st_size<=32768:
                        raw=response.read_bytes()
                        encoding='utf-16' if raw.startswith((b'\xff\xfe',b'\xfe\xff')) else 'utf-8-sig'
                        record['response_encoding']=encoding
                        try:
                            reply=json.loads(raw.decode(encoding))
                            record['response_schema_valid']=(isinstance(reply,dict) and reply.get('handoff_id')==job['id']
                                                      and reply.get('status') in ('in_progress','ready','done','blocked','paused')
                                                      and isinstance(reply.get('summary'),str))
                            if record['response_schema_valid']:record['response_status']=reply['status']
                        except (ValueError,UnicodeError):record['response_schema_valid']=False
                result['records'].append(record)
            if matches:
                messages=[item.get('text','') for item in turn.get('items',[]) if isinstance(item,dict) and item.get('type')=='agentMessage']
                if messages:excerpts[turn['id']]=str(messages[-1])
        result['truncated']=len(result['records'])>4
        result['records']=result['records'][:4]
        if result['records']:
            from peer import clean_business_text,Transport
            try:
                token=Transport(config,peer).token
            except (KeyError,PeerError,OSError,ValueError):
                token=''
            budget=350//len(result['records'])
            for record in result['records']:
                text=clean_business_text(excerpts.get(record['turn_id'],''),token)
                text=re.sub(r'sk-[A-Za-z0-9_-]+','<凭据已隐藏>',text)
                text=re.sub(r'https?://\S+','<链接已隐藏>',text)
                text=re.sub(r'(?i)(--(?:api[-_]key|token|password|app[-_]secret)\s+)\S+',r'\1<凭据已隐藏>',text)
                if text:
                    record['agent_reply_excerpt']=text[:budget]
            limit=max(400,2400-110*len(queue_ids))
            while len(json.dumps(result))>limit:
                texts=[record for record in result['records'] if record.get('agent_reply_excerpt')]
                if texts:
                    for record in texts:record['agent_reply_excerpt']=record['agent_reply_excerpt'][:len(record['agent_reply_excerpt'])//2]
                else:
                    result['records']=result['records'][:max(0,len(result['records'])-1)];result['truncated']=True
                    if not result['records']:break
    except (OSError,ValueError,KeyError,StopIteration,RpcError,PeerError,TimeoutError,sqlite3.Error):
        pass
    finally:
        if client:
            client.close()
    return result


class Consumer:
    def __init__(self, root, config, request, client_factory=StdioClient, keep_alive=True):
        from peer import project_runtime, project_local
        self.root, self.config, self.request = Path(root), config, request
        self.thread, self.peer_id = request['thread'], request['peer']
        self.peer = next(p for p in config['peers'] if p['id'] == self.peer_id)
        self.runtime = project_runtime(root, config, self.peer_id)
        self.policy_path = project_local(root, config) / ('business-agent-' + self.peer_id + '.json')
        self.lock, self.state_path = paths(root, config, self.peer_id, self.thread)
        try:
            self.previous_state=json.loads(self.state_path.read_text(encoding='utf-8'))
        except (OSError,ValueError):
            self.previous_state={}
        preserve_consumer_failure(self.state_path, self.previous_state, self.thread)
        self.client_factory = client_factory
        self.client = None
        self.turn_id = None
        self.current_queue = None
        self.keep_alive=keep_alive
        self.last_status_key=None
        self.resumed = False

    def status(self, state, reason, **fields):
        key=(state,reason,self.current_queue,self.turn_id,json.dumps(fields,sort_keys=True))
        if key==self.last_status_key:return
        self.last_status_key=key
        save_json(self.state_path, dict(state=state, reason=reason, checked_at=time.time(),
                                       thread=self.thread, queue_message_id=self.current_queue,
                                       turn_id=self.turn_id, **fields))
        if state in ('blocked', 'unknown'):
            preserve_consumer_failure(self.state_path, json.loads(self.state_path.read_text(encoding='utf-8')), self.thread)

    def owned_jobs(self):
        if self.config.get('project_id', '') != self.request['project_id']:
            return []
        if not self.resumed and time.time() >= self.request['expires_at']:
            return []
        policy = json.loads(self.policy_path.read_text(encoding='utf-8'))
        if policy.get('thread') != self.thread or policy.get('workdir') not in (None, self.peer.get('workdir')):
            return []
        database = (self.runtime / 'state.sqlite3').resolve()
        db = sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=2)
        try:
            rows = db.execute("SELECT value FROM metadata WHERE key LIKE 'business_job:%' ORDER BY rowid").fetchall()
        finally:
            db.close()
        jobs = [json.loads(row[0]) for row in rows]
        return [j for j in jobs if j.get('direction') == 'incoming' and j.get('state') in ('queued', 'expired')
                and j.get('thread') == self.thread and j.get('source') == policy.get('coordinator')
                and j.get('project_id', '') == self.config.get('project_id', '')
                and re.fullmatch(UUID, str(j.get('queue_message_id', ''))) ]

    def authorized_jobs(self):
        now = time.time()
        return [job for job in self.owned_jobs() if job.get('state') == 'queued'
                and type(job.get('expires_at')) in (int, float) and math.isfinite(job['expires_at'])
                and job['expires_at'] > now and not job.get('result_version')]

    def rejected_start_proof(self, old, selected):
        """Only the existing, explicitly rejected native start may be recovered."""
        proof = rejected_start_from_legacy_log(old, self.root/'runtime'/self.peer_id/'connector.log')
        previous_reason = old.get('error_reason') or self.previous_state.get('reason', '')
        if (old.get('outcome') == 'rejected' and old.get('error_code') == -32600
                and old.get('retry_count', 0) < 3
                and self.previous_state.get('thread') == self.thread
                and self.previous_state.get('queue_message_id') == selected
                and 'resume the thread before starting a queued message' in previous_reason):
            proof = dict(code=-32600, outcome='rejected', reason='resume_required')
        other_attempts = []
        for other in self.state_path.parent.glob('attempt-*.json'):
            data = json.loads(other.read_text(encoding='utf-8'))
            if abs(data.get('started_at', 0)-old.get('started_at', 0)) <= 15:
                other_attempts.append(other)
        if (proof and len(other_attempts) == 1 and old.get('thread') == self.thread
                and old.get('queue_message_id') == selected):
            return proof
        return None

    def native_queue_snapshot(self):
        """Read a complete bounded queue; keep input only in private memory."""
        items, identities, cursors = [], set(), set()
        cursor = None
        for _ in range(8):
            params = dict(threadId=self.thread, limit=100)
            if cursor is not None:
                params['cursor'] = cursor
            result = self.client.call('thread/queue/list', params, timeout=5)
            if not isinstance(result, dict) or not isinstance(result.get('data'), list):
                raise RpcError('原会话队列结构无法核实，未恢复或启动任务。')
            for item in result['data']:
                identity = item.get('id') if isinstance(item, dict) else None
                if not isinstance(identity, str) or not re.fullmatch(UUID, identity) or identity in identities:
                    raise RpcError('原会话队列标识不完整或已变化，未恢复或启动任务。')
                identities.add(identity)
                items.append(item)
            cursor = result.get('nextCursor')
            if cursor is None:
                break
            if not isinstance(cursor, str) or not cursor or cursor in cursors:
                raise RpcError('原会话队列分页未完整读取，未恢复或启动任务。')
            cursors.add(cursor)
        else:
            raise RpcError('原会话队列超出本轮有界核查范围，未恢复或启动任务。')
        return items

    def expired_native_job(self, item):
        """Match an expired connector job to its exact original native input."""
        from setup_helper import business_message_reference
        for job in self.owned_jobs():
            expiry = job.get('expires_at')
            if (job['queue_message_id'] != item['id'] or type(expiry) not in (int, float)
                    or not math.isfinite(expiry) or not 0 < expiry <= time.time()
                    or not re.fullmatch(r'[0-9a-f]{12}', str(job.get('id', '')))):
                continue
            try:
                path = safe_path(self.runtime, 'business-jobs/' + job['id'] + '-message.txt')
            except PeerError:
                return None
            if not path.is_file():
                return None
            raw = path.read_bytes()
            if len(raw) > 131072:
                return None
            digest = hashlib.sha256(raw).hexdigest()
            content = item.get('input')
            if (not isinstance(content, list) or len(content) != 1 or not isinstance(content[0], dict)
                    or set(content[0])-{'type', 'text', 'text_elements'}
                    or content[0].get('type') != 'text' or content[0].get('text_elements', []) != []
                    or content[0].get('text') != business_message_reference(path, digest)):
                return None
            return dict(id=job['id'], queue_message_id=job['queue_message_id'], source=job['source'],
                        thread=job['thread'], project_id=job.get('project_id', ''),
                        expires_at=expiry, message_sha256=digest)
        return None

    def retire_expired_native_job(self, item, expected):
        """Cancel only verified expired queued input once; preserve the original job."""
        current = next((entry for entry in self.native_queue_snapshot() if entry['id'] == item['id']), None)
        verified = self.expired_native_job(current) if current else None
        if not verified or verified != expected or current != item or not self.authorized_jobs():
            raise RpcError('过期委派的当前归属或原文已变化，未撤销原队列消息。')
        path = self.state_path.parent / ('cancel-expired-' + item['id'] + '.json')
        record = dict(thread=self.thread, project_id=self.request['project_id'],
                      job_id=verified['id'], queue_message_id=item['id'],
                      source=verified['source'], expires_at=verified['expires_at'],
                      message_sha256=verified['message_sha256'], attempted_at=time.time(),
                      state='attempting', delete_acknowledged=False, native_absence_verified=False)
        try:
            with path.open('x', encoding='utf-8') as stream:
                json.dump(record, stream, ensure_ascii=False)
                stream.flush()
                os.fsync(stream.fileno())
        except FileExistsError:
            raise RpcError('原队列撤销已有尝试记录，未重复撤销；保留原委派待核实。') from None
        # Binding and deadline are checked again after durable intent creation.
        if self.expired_native_job(current) != verified or not self.authorized_jobs():
            record['state'] = 'not_started_binding_changed';save_json(path, record)
            raise RpcError('委派绑定或截止变化，未发起原生队列撤销。')
        try:
            result = self.client.call('thread/queue/delete',
                     dict(threadId=self.thread, queuedSubmissionId=item['id']), timeout=5)
            record['delete_acknowledged'] = isinstance(result, dict) and result.get('deleted') is True
            record['state'] = 'acknowledged' if record['delete_acknowledged'] else 'not_confirmed'
        except (RpcError, TimeoutError) as error:
            record.update(state='unknown', error_type=type(error).__name__)
        save_json(path, record)
        return path

    def authorized_native_queue(self, allow_retirement=True):
        """Verify the whole queue before resume can automatically consume input."""
        items = self.native_queue_snapshot()
        identities = [item['id'] for item in items]
        # Expiry, binding and results can change while pages are being read.
        allowed = {job['queue_message_id'] for job in self.authorized_jobs()}
        if not allowed:
            raise RpcError('原会话队列含过期或未获本项目授权的消息，保留原队列，未恢复或启动任务。')
        stale = [item for item in items if item['id'] not in allowed]
        if stale:
            expired = [(item, self.expired_native_job(item)) for item in stale]
            if not allow_retirement or len(expired) > 8 or any(job is None for _, job in expired):
                raise RpcError('原会话队列含过期或未获本项目授权的消息，保留原队列，未恢复或启动任务。')
            records = [self.retire_expired_native_job(item, job) for item, job in expired]
            # An unknown delete ACK is never repeated. Only a fresh complete read
            # proving absence permits this consumer to proceed with today's work.
            remaining = self.authorized_native_queue(allow_retirement=False)
            for path in records:
                record = json.loads(path.read_text(encoding='utf-8'))
                record.update(native_absence_verified=True, verified_at=time.time(), state='absent_verified')
                save_json(path, record)
            self.status('expired_retired', '已核销本项目原文一致的过期排队委派，保留原文和撤销记录，继续本轮授权任务。',
                        expired_queue_count=len(records))
            return remaining
        for identity in identities:
            attempt = self.state_path.parent / ('attempt-' + identity + '.json')
            if attempt.exists():
                old = json.loads(attempt.read_text(encoding='utf-8'))
                if not self.rejected_start_proof(old, identity):
                    raise RpcError('原队列消息已有开始尝试，保留原记录，未通过恢复会话重复启动。')
        return identities

    def native_turn_job(self, items):
        """Correlate an automatic turn to the exact connector input before using its deadline."""
        from setup_helper import business_message_reference

        messages = [item['content'] for item in items
                    if isinstance(item, dict) and item.get('type') == 'userMessage' and 'content' in item]
        if not messages:
            return None
        matches = []
        for job in self.owned_jobs():
            expiry = job.get('expires_at')
            if (type(expiry) not in (int, float) or not math.isfinite(expiry)
                    or not re.fullmatch(r'[0-9a-f]{12}', str(job.get('id', '')))):
                continue
            path = safe_path(self.runtime, 'business-jobs/' + job['id'] + '-message.txt')
            if not path.is_file() or path.stat().st_size > 131072:
                continue
            reference = business_message_reference(path, hashlib.sha256(path.read_bytes()).hexdigest())
            if any(content == [{'type': 'text', 'text': reference, 'text_elements': []}]
                   or content == [{'type': 'text', 'text': reference}] for content in messages):
                matches.append(job)
        return matches[0] if len(matches) == 1 else None

    def wait_turn(self, expires_at=None, native=False):
        if expires_at is None:
            expires_at = self.request['expires_at']
        while time.time() < expires_at:
            try:
                data = self.client.events.pop(0) if self.client.events else self.client.receive(max(0, min(5, expires_at - time.time())))
            except TimeoutError:
                continue
            params = data.get('params', {})
            if params.get('threadId') != self.thread:
                continue
            turn = params.get('turn', {})
            if data.get('method') == 'turn/started' and not self.turn_id:
                self.turn_id = turn.get('id')
            if (native and not self.current_queue and self.turn_id
                    and params.get('turnId') == self.turn_id and data.get('method') in ('item/started', 'item/completed')):
                job = self.native_turn_job([params.get('item')])
                if job:
                    self.current_queue, expires_at = job['queue_message_id'], job['expires_at']
                    self.status('turn_started', '已核实自动接续回合属于本项目原委派，采用该委派截止时间。')
            if data.get('method') == 'turn/completed' and (not self.turn_id or turn.get('id') == self.turn_id):
                self.turn_id = turn.get('id')
                if self.current_queue:
                    save_json(self.state_path.parent/('turn-'+self.current_queue+'.json'),
                              dict(public_turn_metadata(turn),thread=self.thread,queue_message_id=self.current_queue,checked_at=time.time()))
                label={'completed':'已完成','failed':'失败','interrupted':'已中断'}.get(turn.get('status'),'已结束')
                self.status('turn_completed', '原会话本轮'+label+'，业务结果以其响应文件为准。', turn_status=turn.get('status'))
                return
        if self.turn_id:
            self.client.call('turn/interrupt', dict(threadId=self.thread, turnId=self.turn_id), timeout=5)
        raise RpcError('已到截止时间，后台不再发起新工作。')

    def native_events(self, timeout=.01):
        """Observe turns which Codex itself starts when restoring a queued session."""
        try:
            data=self.client.events.pop(0) if self.client.events else self.client.receive(timeout)
        except TimeoutError:
            return False
        params=data.get('params',{})
        if params.get('threadId')==self.thread and data.get('method')=='turn/started':
            turn = params.get('turn', {})
            self.turn_id, self.current_queue = turn.get('id'), None
            job = self.native_turn_job(turn.get('items', []))
            if not job and self.turn_id:
                try:
                    recent = self.client.call('thread/turns/list', dict(threadId=self.thread, limit=1,
                                              sortDirection='desc', itemsView='full'), timeout=2)
                    exact = next((row for row in recent.get('data', []) if row.get('id') == self.turn_id), {})
                    job = self.native_turn_job(exact.get('items', []))
                except (RpcError, TimeoutError):
                    pass
            if job:
                self.current_queue = job['queue_message_id']
            self.status('turn_started','原生会话已自动接续回合，保持宿主并等待结果。')
            self.wait_turn(job['expires_at'] if job else None, native=True)
            return True
        return False

    def idle(self, reason):
        self.status('idle',reason+' 宿主保持在线，不重复投递。')
        if not self.keep_alive:return False
        if time.time() >= self.request['expires_at']:
            self.status('deadline','已到本轮截止，结束本轮会话宿主。');return False
        self.native_events(timeout=1)
        return True

    def run(self):
        with acquire_instance_lock(self.lock, dict(thread=self.thread)):
            try:
                if not self.authorized_jobs():
                    self.status('idle', '没有本项目获授权的待处理原队列。');return
                self.status('starting', '正在连接已安装的CLI后台接口。')
                self.client = self.client_factory(cli_command(self.config), self.peer['workdir'])
                self.client.call('initialize', dict(clientInfo=dict(name='dingtalk_peer_consumer', version='1'),
                                                     capabilities=dict(experimentalApi=True)))
                self.client.send(dict(method='initialized', params={}))
                # Read only first. An unfinished turn must be reconciled, never run concurrently.
                try:
                    recent = self.client.call('thread/turns/list', dict(threadId=self.thread, limit=1, sortDirection='desc'))
                    turns = recent.get('data', [])
                except RpcError as error:
                    if error.code not in (-32601,-32600):
                        raise
                    # Older installed versions can expose history through thread/read.
                    recent=self.client.call('thread/read',dict(threadId=self.thread,includeTurns=True))
                    turns=list(reversed(recent.get('thread',{}).get('turns',[])))[:1]
                if turns and turns[0].get('status') not in ('completed', 'failed', 'interrupted'):
                    self.status('blocked', '原会话上一轮尚未结束，未并发启动；请先核实原会话状态。');return
                # Some installed backends auto-drain pending input on resume.
                # A post-resume head check cannot prevent that execution.
                self.authorized_native_queue()
                if not self.authorized_jobs():
                    raise RpcError('委派有效期或绑定已变化，未恢复原会话。')
                resumed=self.client.call('thread/resume',dict(threadId=self.thread),timeout=60)
                if resumed.get('thread',{}).get('id')!=self.thread:
                    raise RpcError('后台返回的会话与原绑定不符。')
                self.resumed = True
                while True:
                    if self.native_events():continue
                    allowed = {j['queue_message_id']: j for j in self.authorized_jobs()}
                    if not allowed:
                        if self.idle('本项目原队列已处理或已有回报。'):continue
                        return
                    items = self.authorized_native_queue()
                    if not items:
                        if self.idle('原消息已离开队列，等待业务回报。'):continue
                        return
                    selected = items[0]
                    if selected not in allowed:
                        self.status('blocked', '队首含其他来源任务，未跳过或消费未授权消息。');return
                    self.current_queue, self.turn_id = selected, None
                    # Recheck expiry and binding immediately before the one atomic queue start.
                    job = next((row for row in self.authorized_jobs() if row['queue_message_id'] == selected), None)
                    if not job:
                        self.status('idle', '委派有效期或绑定已变化，未启动。');return
                    attempt=self.state_path.parent / ('attempt-' + selected + '.json')
                    attempt_data=dict(thread=self.thread,queue_message_id=selected,started_at=time.time(),outcome='starting',retry_count=0)
                    try:
                        with attempt.open('x',encoding='utf-8') as handle:
                            json.dump(attempt_data,handle)
                    except FileExistsError:
                        old=json.loads(attempt.read_text(encoding='utf-8'))
                        proof=self.rejected_start_proof(old, selected)
                        if not proof:
                            contract=queue_start_schema(self.config,self.state_path.parent)
                            self.status('blocked','原开始尝试保留，未重复启动；本机CLI原队列开始契约：'+json.dumps(contract,ensure_ascii=False));return
                        # The native queue still contains this exact ID and the old host
                        # explicitly rejected validation. Preserve both attempts in one ledger.
                        attempt_data.update(retry_count=old.get('retry_count',0)+1,previous_attempt=dict(old,**proof),recovery='explicit_rejected_start_reconciled')
                        save_json(attempt,attempt_data)
                    self.status('start_attempting', '正在请求原队列原消息开始，本次请求不会重复。')
                    try:
                        result = self.client.call('thread/queue/start', dict(threadId=self.thread, queuedSubmissionId=selected), timeout=30)
                        self.turn_id = result.get('turn', {}).get('id')
                        attempt_data.update(outcome='started',turn_id=self.turn_id)
                        save_json(attempt,attempt_data)
                        self.status('turn_started', 'CLI原会话已开始处理原队列，等待业务回报。')
                    except TimeoutError:
                        attempt_data['outcome']='unknown';save_json(attempt,attempt_data)
                        self.status('unknown', '开始回执不明，等待原回合结束，不重发或重试开始。')
                        self.wait_turn(job['expires_at']);return
                    except RpcError as error:
                        attempt_data.update(outcome='rejected' if error.code in (-32600,-32601,-32602) else 'unknown',error_code=error.code,error_reason=str(error))
                        save_json(attempt,attempt_data)
                        raise
                    self.wait_turn(job['expires_at'])
            except (RpcError, OSError, ValueError, sqlite3.Error, TimeoutError) as error:
                reason = str(error) if isinstance(error, RpcError) else '后台消费未完成：' + type(error).__name__
                self.status('blocked', reason)
            finally:
                if self.client:
                    self.client.close()


def main():
    from peer import load_config
    root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser()
    parser.add_argument('--request', required=True)
    args = parser.parse_args()
    request_path = Path(args.request).resolve()
    request_path.relative_to(root / 'runtime')
    request = json.loads(request_path.read_text(encoding='utf-8'))
    config = load_config(root / '.local/config.json')
    consumer = Consumer(root, config, request)
    expected = consumer.state_path.with_name(request['thread'] + '-request.json')
    if expected.resolve() != request_path or not re.fullmatch(UUID, request['thread']):
        raise PeerError('后台消费请求路径无效。')
    try:
        consumer.run()
    except PeerAlreadyRunning:
        pass


if __name__ == '__main__':
    main()
