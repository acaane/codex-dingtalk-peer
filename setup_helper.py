"""Setup, account checks and friendly launcher; no credentials in command lines."""
import argparse
import copy
import functools
import getpass
import hashlib
import json
import os
from pathlib import Path
import queue
import re
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import warnings
from urllib.parse import parse_qs, urlparse, urlencode

from peer import (ROOT, PeerError, Transport, bind_store_context, codex_command, configured_project,
                  parse_agent_response, load_config, project_local, project_runtime, workbook_robots)
from peer import operator_command, webhook_receipt
from connector_update import safe_path, save_json
from reliability import instance_lock_owner
from reliability import SendRejected, acquire_instance_lock
from windows_secret import protect, unprotect


SETUP_ENTRY = operator_command("scripts/setup/setup.cmd")
CHECK_ENTRY = operator_command("scripts/diagnostics/check.cmd")


def say(message):
    print(message, flush=True)


def read_participants(path):
    """Read an explicit public invitation; never load credentials from it."""
    data = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not isinstance(data, dict) or set(data) - {"group_id", "project_id", "peers"} or not isinstance(data.get("group_id"), str) or not data["group_id"].strip():
        raise PeerError("参与者配置只允许群、项目标识和公开身份字段，不接受凭据。")
    configured_project(data)
    peers = data.get("peers")
    allowed = {"id", "name", "owner_name", "bot_id", "owner_member_id", "sender_id", "owner_id", "workdir", "role"}
    required = {"id", "name", "owner_name", "bot_id", "owner_member_id"}
    if not isinstance(peers, list) or len(peers) < 2:
        raise PeerError("参与者配置至少需要两端。")
    for participant in peers:
        if (not isinstance(participant, dict) or set(participant) - allowed
                or any(not isinstance(participant.get(k), str) or not participant[k].strip() for k in required)
                or any(not isinstance(v, str) for v in participant.values())
                or not re.fullmatch(r"[A-Z][A-Z0-9_-]*", participant["id"]) or participant["id"] == "ALL"):
            raise PeerError("参与者配置字段无效，必须提供本人姓名和稳定机器人/主人ID。")
    for key in ("id", "bot_id", "owner_member_id"):
        if len({p[key] for p in peers}) != len(peers):
            raise PeerError("参与者身份不能重复。")
    return data


def configuration_template():
    config = json.loads((ROOT / "config.example.json").read_text(encoding="utf-8-sig"))
    invitation = ROOT / ".local/participants.json"
    if not invitation.exists():
        invitation = ROOT / "participants.json"
    if invitation.exists():
        config.update(read_participants(invitation))
    for participant in config["peers"]:
        participant.setdefault("sender_id", "pending-bot-" + participant["id"])
        participant.setdefault("owner_id", "pending-owner-" + participant["id"])
        participant.setdefault("workdir", "由本机配置")
        participant.setdefault("role", "负责本端项目，按当前任务与同伴协作。")
    return config


def select_item(title, items, label, input_fn=input):
    if not items:
        raise PeerError(title + "：没有可用项。")
    if len(items) == 1:
        return items[0]
    say(title)
    for index, item in enumerate(items, 1):
        say(f"  {index}. {label(item)}")
    while True:
        value = input_fn("选择序号：").strip()
        if value.isdigit() and 1 <= int(value) <= len(items):
            return items[int(value) - 1]


def invited_project(history, group_id, own_id, partner_id, sender_id):
    """Use only unambiguous, current invitations from the actual partner robot."""
    from peer import BUSINESS_PREFIX
    if (not isinstance(history, dict) or history.get('complete') is not True
            or history.get('hasMore') or history.get('failures') or history.get('partial')
            or history.get('failedCount', 0) or not isinstance(history.get('messages', []), list)):
        return None
    candidates = set()
    for message in history.get('messages', []):
        if not isinstance(message, dict):
            continue
        text = message.get('text')
        if (message.get('conversationId') != group_id or message.get('senderId') != sender_id
                or not isinstance(text, str) or not text.startswith(BUSINESS_PREFIX)):
            continue
        try:
            data = json.loads(text[len(BUSINESS_PREFIX):])
            if (not isinstance(data, dict) or data.get('version') != 1 or data.get('source') != partner_id
                    or data.get('target') != own_id or data.get('kind') not in ('bound', 'request')):
                continue
            stamp = data.get('authorized_at') if data['kind'] == 'bound' else data.get('expires_at')
            if type(stamp) not in (int, float):
                continue
            if not (time.time() - 86400 <= stamp <= time.time() + 300 if data['kind'] == 'bound'
                    else time.time() < stamp <= time.time() + 86400):
                continue
            identity = configured_project(data)
            if identity:
                candidates.add(identity)
        except (ValueError, TypeError, PeerError):
            continue
    return next(iter(candidates)) if len(candidates) == 1 else None


def detect_project_directory():
    """Inspect nearby repository markers, never recursively scan disks or private data."""
    parent = ROOT.resolve().parent
    if parent == Path(parent.anchor):
        return None
    markers = ('.git', 'Cargo.toml', 'CMakeLists.txt', 'package.json', 'pyproject.toml', 'go.mod')
    def repository(path):
        return path.is_dir() and any((path / name).exists() for name in markers)
    if repository(parent):
        return str(parent)
    try:
        siblings = list(parent.iterdir())
        if len(siblings) > 100:
            return None
        candidates = [p.resolve() for p in siblings if p.resolve() != ROOT.resolve()
                      and not p.is_symlink() and repository(p)]
    except OSError:
        return None
    return str(candidates[0]) if len(candidates) == 1 else None


def discover_participants(tools, profile, membership, self_data, search_data, template, partner=None, project_id=None, input_fn=input, history=None):
    """Match authenticated contact IDs to actual group identities; names only find candidates."""
    if (membership.get("complete") is not True or membership.get("hasMore") or membership.get("partial") or membership.get("failures") or membership.get('failedCount', 0)):
        raise PeerError("群身份数据不完整，不能自动生成参与者配置。")
    if membership.get('conversationId') != template['group_id']:
        raise PeerError("群身份查询返回了其他群，原配置未修改。")
    records = self_data.get("result", [])
    if isinstance(records, dict):
        records = [records]
    people = [r['orgEmployeeModel'] for r in records if isinstance(r, dict) and isinstance(r.get('orgEmployeeModel'), dict)]
    if len(people) != 1 or not people[0].get('userId'):
        raise PeerError("未能唯一检测本人账号ID，请检查本人登录后重新设置。")
    person = people[0]
    self_id = str(person['userId'])
    matches = [r for r in search_data.get('result', []) if str(r.get('userId', '')) == self_id and r.get('openDingTalkId')]
    own_ids = {r['openDingTalkId'] for r in matches}
    owners = [u for u in membership.get('users', []) if u.get('openDingtalkId') in own_ids]
    if len(owners) != 1:
        raise PeerError("本人通讯录ID未能唯一对应群成员，不能只凭姓名自动授权。请核对群和登录身份。")
    owner = owners[0]
    bots = [b for b in membership.get('bots', []) if b.get('openBotId') and b.get('openDingtalkId') and b.get('robotCode') != 'intelligent']
    known = {p['bot_id']: p for p in template['peers'] if p.get('bot_id')}
    own_candidates = [b for b in bots if known.get(b['openBotId'], {}).get('owner_member_id') == owner['memberDingtalkId']]
    if not own_candidates:
        expected = '联调-AI-' + person.get('orgUserName', profile.get('userName', ''))
        own_candidates = [b for b in bots if b.get('name') == expected]
    own_bot = select_item('选择本人的现有机器人', own_candidates or bots, lambda b: b.get('name', b['openBotId']), input_fn)
    others = [b for b in bots if b['openBotId'] != own_bot['openBotId']]
    if partner:
        candidates = [b for b in others if partner in (b.get('name'), b['openBotId'])]
        if len(candidates) != 1:
            raise PeerError("指定对端机器人未唯一匹配，请检查群内现有机器人。")
        other_bot = candidates[0]
    else:
        coordinator = min(template['peers'], key=lambda p: p['id'])
        preferred = [b for b in others if b['openBotId'] == coordinator.get('bot_id')]
        other_bot = preferred[0] if len(preferred) == 1 else select_item('选择本次联调的对端机器人', others, lambda b: b.get('name', b['openBotId']), input_fn)
    other_known = known.get(other_bot['openBotId'], {})
    peer_owners = [u for u in membership.get('users', []) if u.get('memberDingtalkId') == other_known.get('owner_member_id')]
    if len(peer_owners) != 1:
        peer_owners = membership.get('users', [])
        other_owner = select_item('选择对端机器人的负责人（无法从稳定ID自动确定）', peer_owners, lambda u: u.get('name', u.get('memberDingtalkId', '')), input_fn)
    else:
        other_owner = peer_owners[0]
    if other_owner['memberDingtalkId'] == owner['memberDingtalkId']:
        raise PeerError("真实双端联调需要不同的本人账号，请核对对端负责人。")
    if not project_id and history is not None:
        own_id = known.get(own_bot['openBotId'], {}).get('id') or ('P' + hashlib.sha256(own_bot['openBotId'].encode()).hexdigest()[:8].upper())
        other_id = other_known.get('id') or ('P' + hashlib.sha256(other_bot['openBotId'].encode()).hexdigest()[:8].upper())
        project_id = invited_project(history, template['group_id'], own_id, other_id, other_bot['openDingtalkId'])
        if project_id:
            say('已从对端真实机器人接入通知识别项目：' + project_id)
    while True:
        identity = project_id or input_fn('双方约定的项目标识（小写字母/数字/_/-）：').strip()
        try:
            if not identity:
                raise PeerError('项目标识不能为空。')
            configured_project({'project_id': identity})
            break
        except PeerError:
            if project_id:
                raise
            say('项目标识格式无效，请重新填写。')
    roster = []
    for bot, member in ((own_bot, owner), (other_bot, other_owner)):
        previous = known.get(bot['openBotId'], {})
        peer_id = previous.get('id') or ('P' + hashlib.sha256(bot['openBotId'].encode()).hexdigest()[:8].upper())
        roster.append(dict(id=peer_id, name=bot['name'], owner_name=member.get('name', ''), bot_id=bot['openBotId'],
                           owner_member_id=member['memberDingtalkId'], sender_id=bot['openDingtalkId'], owner_id=member['openDingtalkId'],
                           workdir='由本机配置', role='负责本端项目，按本次任务与同伴协作。'))
    return {'group_id': membership['conversationId'], 'project_id': identity, 'peers': sorted(roster, key=lambda p: p['id'])}, roster[0]['id']


def discover_local(tools, partner=None, project_id=None, ask_project=False):
    profile = ensure_dws_login(tools)
    template = configuration_template()
    membership = run_json([tools['dws_path'], 'chat', '+chat-members-list', '--group', template['group_id'], '--profile', profile['profile'], '--format', 'json'])
    self_data = run_json([tools['dws_path'], 'contact', 'user', 'get-self', '--profile', profile['profile'], '--format', 'json'])
    search = run_json([tools['dws_path'], 'aisearch', 'person', '--query', profile['userName'], '--dimension', 'name', '--profile', profile['profile'], '--format', 'json'])
    selected_project = project_id or (None if ask_project else configured_project(template))
    history = None
    if not selected_project and not ask_project:
        try:
            history = run_json([tools['dws_path'], 'chat', '+chat-messages', '--group', template['group_id'],
                                '--profile', profile['profile'], '--order', 'desc', '--no-reactions',
                                '--page-all', '--max-items', '300', '--page-limit', '20', '--format', 'json'])
        except (PeerError, subprocess.TimeoutExpired):
            history = None
    roster, own_peer = discover_participants(tools, profile, membership, self_data, search, template, partner,
                                            selected_project, history=history)
    local = ROOT / '.local'
    local.mkdir(exist_ok=True)
    save_json(safe_path(local, 'participants.json'), roster)
    save_json(safe_path(local, 'discovery.json'), {'peer': own_peer, 'project_id': roster['project_id'], 'profile': profile['profile']})
    say(f"已识别本人 {profile['userName']}、机器人端ID {own_peer} 和项目 {roster['project_id']}。")
    return roster


def webhook_rejection_message(record):
    receipt = record.get('send_receipt', {})
    code = receipt.get('webhook_errcode') if isinstance(receipt, dict) else None
    suffix = '（错误码' + str(code) + '）' if code is not None and re.fullmatch(r'-?\d{1,12}', str(code)) else ''
    if str(code) == '300005':
        return f'机器人平台已明确拒绝当前Webhook：token不存在（300005）。请从本人的机器人设置重新复制完整Webhook，在{SETUP_ENTRY}中选择“重新粘贴”。'
    return f'机器人平台已明确拒绝接入校验消息{suffix}；请核对本人Webhook及机器人安全设置，修正后运行{SETUP_ENTRY}重试。本次不会自动重发。'


def recover_wrapped_rejection(record, path):
    """Recover only the exact recorded MCP error, never infer from a generic timeout."""
    receipt = record.get('send_receipt', {})
    if record.get('state') == 'unknown' and isinstance(receipt, dict):
        embedded = webhook_receipt({'error': {'message': receipt.get('error_message')}})
        if embedded:
            record.update(state='rejected', error_type='SendRejected')
            record['send_receipt']['webhook_errcode'] = embedded['errcode']
            save_json(path, record)
    return record


def verify_webhook_identity(config, peer_id, retry_rejected=False):
    """One saved probe proves the credential's actual sender; unknown delivery is not resent."""
    participant = next(p for p in config['peers'] if p['id'] == peer_id)
    transport = Transport(config, participant)
    fingerprint = hashlib.sha256((config['profile'] + config['group_id'] + participant['sender_id'] + transport.token).encode()).hexdigest()
    path = safe_path(ROOT, f'.local/identity-check-{peer_id}.json')
    diagnostic_path = safe_path(ROOT, f'.local/webhook-diagnostic-{peer_id}.json')
    diagnostic = json.loads(diagnostic_path.read_text(encoding='utf-8')) if diagnostic_path.exists() else {}
    if diagnostic.get('fingerprint') == fingerprint:
        diagnostic = recover_wrapped_rejection(diagnostic, diagnostic_path)
    if (diagnostic.get('fingerprint') == fingerprint and diagnostic.get('state') == 'verified'
            and isinstance(diagnostic.get('message_id'), str) and diagnostic['message_id']):
        say('本人Webhook已由独立诊断消息通过真实群发送者ID校验；原未知消息记录保留。')
        return
    previous = json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
    if (previous.get('fingerprint') == fingerprint and previous.get('state') in ('unknown', 'sending')
            and diagnostic.get('fingerprint') == fingerprint and diagnostic.get('state') == 'rejected'):
        raise PeerError(webhook_rejection_message(diagnostic))
    if previous.get('fingerprint') == fingerprint:
        record = recover_wrapped_rejection(previous, path)
        if record['state'] == 'verified':
            return
        if record['state'] == 'mismatch':
            raise PeerError('该Webhook的实际机器人与所选本人不符，请在setup中修正本人的Webhook。')
        # Older installers recorded an explicit rejection as unknown. Only this
        # exact error class proves that a manual setup may safely try again.
        if record.get('error_type') == 'SendRejected' and record['state'] == 'unknown':
            record['state'] = 'rejected'
            save_json(path, record)
        if record['state'] == 'rejected' and not retry_rejected:
            raise PeerError(webhook_rejection_message(record))
    if previous.get('fingerprint') != fingerprint or record['state'] == 'rejected':
        if previous:
            save_json(path.with_name(path.stem + '-' + str(time.time_ns()) + '.json'), previous)
        record = {'fingerprint': fingerprint, 'state': 'sending', 'started_at': time.time(),
                  'text': '【AI联调接入校验】' + peer_id + '端 ' + os.urandom(12).hex() + '；仅核验本人机器人身份，不代表业务联调通过。'}
        save_json(path, record)
        try:
            transport.send(record['text'])
            record['state'] = 'accepted'
        except SendRejected:
            record.update(state='rejected', error_type='SendRejected', send_receipt=getattr(transport, 'last_send_diagnostic', {}))
            save_json(path, record)
            raise PeerError(webhook_rejection_message(record)) from None
        except PeerError as error:
            record['state'] = 'unknown'
            record['error_type'] = type(error).__name__
        record['send_receipt'] = getattr(transport, 'last_send_diagnostic', {})
        save_json(path, record)
    reconcile_identity_probe(config, participant, record, path)


def reconcile_identity_probe(config, participant, record, path):
    start = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(record['started_at'] - 3))
    for delay in (0, 2, 4):
        if delay:
            say('正在等待钉钉群历史同步，继续核实原接入消息；不会重复发送。')
            time.sleep(delay)
        data = run_json([config['dws_path'], 'chat', '+chat-messages', '--group', config['group_id'],
                         '--start', start, '--order', 'asc', '--page-all', '--max-items', '100', '--page-limit', '5',
                         '--no-reactions', '--profile', config['profile'], '--format', 'json'])
        if not isinstance(data, dict) or not isinstance(data.get('messages'), list):
            raise PeerError('接入校验历史结构不可用，已保留待核实状态。')
        if (data.get('complete') is not True or data.get('hasMore') or data.get('partial')
                or data.get('failures') or data.get('failedCount', 0)):
            raise PeerError(f'接入校验历史不完整，已保留状态；请稍后运行{SETUP_ENTRY}核实原消息，不会重复发送。')
        matches = [m for m in data['messages'] if isinstance(m, dict) and m.get('conversationId') == config['group_id'] and m.get('text') == record['text']]
        if matches:
            if any(m.get('senderId') != participant['sender_id'] for m in matches):
                record['state'] = 'mismatch'
                save_json(path, record)
                raise PeerError('Webhook实际发送者与本人机器人不符，配置未启用。')
            if not all(isinstance(m.get('messageId'), str) and m['messageId'] for m in matches):
                raise PeerError('接入校验消息缺少有效ID，已保留待核实状态。')
            record.update(state='verified', message_id=matches[0]['messageId'])
            save_json(path, record)
            say('本人Webhook已通过真实群消息发送者ID校验。')
            return
    raise PeerError(f'接入校验消息尚未核实，已保留状态且不会重复发送；请稍后运行{SETUP_ENTRY}核实，勿删除identity-check记录或重复粘贴凭据。')


def diagnose_webhook_send():
    """One distinct diagnostic message; reconcile its own unknown result on rerun."""
    config = json.loads((ROOT / '.local/config.json').read_text(encoding='utf-8-sig'))
    machine = json.loads((ROOT / '.local/machine.json').read_text(encoding='utf-8-sig'))
    peer_id = machine['peer']
    membership = run_json([config['dws_path'], 'chat', '+chat-members-list', '--group', config['group_id'],
                           '--profile', config['profile'], '--format', 'json'])
    config = resolve_group_identities(config, membership, configuration_template())
    participant = next(p for p in config['peers'] if p['id'] == peer_id)
    transport = Transport(config, participant)
    fingerprint = hashlib.sha256((config['profile'] + config['group_id'] + participant['sender_id'] + transport.token).encode()).hexdigest()
    original_path = safe_path(ROOT, f'.local/identity-check-{peer_id}.json')
    original = json.loads(original_path.read_text(encoding='utf-8')) if original_path.exists() else {}
    if original.get('fingerprint') != fingerprint:
        raise PeerError(f'当前凭据或身份与原校验记录不符，请先运行{SETUP_ENTRY}核对；未发送诊断消息。')
    if original.get('state') == 'verified':
        raise PeerError('本人Webhook接入校验已经通过，无需发送新的诊断消息。')
    path = safe_path(ROOT, f'.local/webhook-diagnostic-{peer_id}.json')
    with acquire_instance_lock(safe_path(ROOT, f'.local/webhook-diagnostic-{peer_id}.lock')):
        record = json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
        if record.get('fingerprint') == fingerprint:
            record = recover_wrapped_rejection(record, path)
        if record.get('fingerprint') != fingerprint or record.get('state') == 'rejected':
            previous = record or None
            record = dict(fingerprint=fingerprint, state='sending', started_at=time.time(),
                          text='【AI联调发送诊断】' + peer_id + '端 ' + os.urandom(12).hex() + '；仅检查连接器发送，不涉及业务或设备操作。')
            if previous:
                save_json(path.with_name(path.stem + '-' + str(time.time_ns()) + '.json'), previous)
            save_json(path, record)
            say('开始发送一条独立的连接器诊断消息；原接入消息及状态保留。')
            try:
                transport.send(record['text'])
                record['state'] = 'accepted'
            except SendRejected:
                record.update(state='rejected', error_type='SendRejected')
            except PeerError as error:
                record.update(state='unknown', error_type=type(error).__name__)
            record['send_receipt'] = getattr(transport, 'last_send_diagnostic', {})
            save_json(path, record)
        if record['state'] not in ('verified', 'rejected', 'mismatch'):
            try:
                reconcile_identity_probe(config, participant, record, path)
            except PeerError as error:
                record['history_error_type'] = type(error).__name__
                save_json(path, record)
        report = dict(peer=peer_id, project_id=configured_project(config), groupId=config['group_id'],
                      original_state=original.get('state'), original_error_type=original.get('error_type'),
                      state=record['state'], expected_sender_id=participant['sender_id'],
                      message_id=record.get('message_id'), send_receipt=record.get('send_receipt', {}),
                      history_error_type=record.get('history_error_type'), configuration_changed=False)
        output = ROOT / 'webhook-diagnostics.json'
        save_json(output, report)
    say('已保存 webhook-diagnostics.json；状态：' + record['state'] + '。未修改原校验记录或发送授权。')
    return output


def collect_onboarding_diagnostics():
    """Read failed setup as-is; never send, authorize, log in, or export CLI output."""
    config = json.loads((ROOT / '.local/config.json').read_text(encoding='utf-8-sig'))
    tools = json.loads((ROOT / '.tools/toolchain.json').read_text(encoding='utf-8-sig'))
    machine = json.loads((ROOT / '.local/machine.json').read_text(encoding='utf-8-sig'))
    peer_id = machine['peer']
    probe_path = safe_path(ROOT, f'.local/identity-check-{peer_id}.json')
    record = json.loads(probe_path.read_text(encoding='utf-8-sig')) if probe_path.exists() else {}
    report = dict(groupId=config['group_id'], project_id=configured_project(config), profile=config['profile'],
                  peer=peer_id, send_authorized=config.get('send_authorized', False),
                  expectedPeers=[{k: p.get(k) for k in ('id', 'name', 'sender_id', 'owner_id')} for p in config['peers']],
                  probe={k: record.get(k) for k in ('state', 'error_type', 'started_at', 'message_id')})

    def read(*args, version=False):
        command = [tools['dws_path'], *args]
        if not version:
            command += ['--profile', config['profile'], '--format', 'json']
        try:
            result = subprocess.run(command, capture_output=True, encoding='utf-8', errors='replace', timeout=20)
        except (OSError, subprocess.TimeoutExpired) as error:
            return {'error_type': type(error).__name__}
        if version:
            match = re.search(r'\bdws version (v[0-9.]+)\b', result.stdout)
            return {'exit_code': result.returncode, 'version': match.group(1) if match else None}
        try:
            data = json.loads(result.stdout or result.stderr)
        except ValueError:
            return {'exit_code': result.returncode, 'unparseable': True}
        if not isinstance(data, dict):
            return {'exit_code': result.returncode, 'unexpected_shape': True}
        if result.returncode or data.get('error'):
            error = data.get('error') if isinstance(data.get('error'), dict) else {}
            code = error.get('code')
            return {'exit_code': result.returncode, 'error_code': code if isinstance(code, str) and re.fullmatch(r'[A-Za-z0-9_.-]{1,80}', code) else None,
                    'retryable': data.get('retryable') is True or error.get('retryable') is True}
        return data

    report['dws'] = read('--version', version=True)
    schema = read('schema', '--cli-path', 'chat +messages-send', '--compact')
    parameters = schema.get('parameters') or (schema.get('data') or {}).get('parameters') or []
    report['send_capabilities'] = {'identity': 'identity' in parameters, 'as': 'as' in parameters,
                                   'body_stdin': 'body-stdin' in parameters, 'webhook_token': 'webhook-token' in parameters,
                                   **{k: schema[k] for k in ('exit_code', 'error_code', 'unparseable', 'error_type') if k in schema}}
    membership = read('chat', '+chat-members-list', '--group', config['group_id'])
    expected = {p.get('sender_id') for p in config['peers']}
    report['membership'] = {k: membership[k] for k in ('conversationId', 'complete', 'hasMore', 'exit_code', 'error_code', 'unparseable', 'error_type') if k in membership}
    report['membership']['bots'] = [{k: b.get(k) for k in ('name', 'openBotId', 'openDingtalkId', 'status')}
                                    for b in membership.get('bots', []) if isinstance(b, dict) and b.get('openDingtalkId') in expected]
    try:
        Transport(config, next(p for p in config['peers'] if p['id'] == peer_id))
        report['credential_readable'] = True
    except (PeerError, OSError, ValueError, StopIteration):
        report['credential_readable'] = False
    if record.get('text') and isinstance(record.get('started_at'), (int, float)):
        start = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(record['started_at'] - 3))
        history = read('chat', '+chat-messages', '--group', config['group_id'], '--start', start,
                       '--order', 'asc', '--page-all', '--max-items', '100', '--page-limit', '5', '--no-reactions')
        matches = [m for m in history.get('messages', []) if isinstance(m, dict)
                   and m.get('conversationId') == config['group_id'] and m.get('text') == record['text']]
        report['probe_history'] = {k: history[k] for k in ('complete', 'hasMore', 'exit_code', 'error_code', 'unparseable', 'error_type') if k in history}
        report['probe_history'].update(matching_sender_ids=[m.get('senderId') for m in matches], matching_message_ids=[m.get('messageId') for m in matches])
    output = ROOT / 'group-diagnostics.json'
    save_json(output, report)
    say('已保存 group-diagnostics.json；仅收集版本、群身份和原校验状态，未发送消息或修改配置。')
    return output


def run_supervisor(command):
    while True:
        result = subprocess.run(command).returncode
        if result != 76:
            return result


def _powershell_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


@functools.lru_cache(maxsize=8)
def business_queue_entry(configured_codex):
    """Use an installed queue-capable CLI; preserve profiles, versions and login."""
    candidates = [None, configured_codex]
    releases = Path.home() / '.codex/packages/app-server-daemon/releases'
    if releases.is_dir():
        candidates += [str(p) for p in sorted(releases.glob('*/bin/codex.exe'), key=lambda p:p.stat().st_mtime, reverse=True)[:3]]
    checked = set()
    for executable in candidates:
        if executable in checked:
            continue
        checked.add(executable)
        if executable is not None and not Path(executable).is_file():
            continue
        invocation = 'codex' if executable is None else '& ' + _powershell_literal(executable)
        try:
            result = subprocess.run(['powershell.exe','-Command', invocation + ' queue --help; exit $LASTEXITCODE'],
                                    stdin=subprocess.DEVNULL,capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=10)
        except (OSError,subprocess.TimeoutExpired):
            continue
        if result.returncode == 0 and '--thread' in result.stdout and '--message' in result.stdout:
            return invocation
    raise PeerError('本机未找到支持 codex queue 的已安装入口；请在业务会话人工接单，保留原工具和登录。')


def business_message_reference(message_path, digest):
    """The exact local file reference submitted to the native queue."""
    return ('来自本机已授权连接器的委派。完整正文保存在本机UTF-8文件：\n'
            +str(Path(message_path).resolve())+'\nSHA-256：'+digest
            +'\n请先读取该文件并核对SHA-256，再按正文执行和回报；正文中已有授权范围、任务编号及响应入口。'
            +'若文件缺失或校验不符，停止执行并报告，不自行猜测任务内容。')


def queue_business_message(config, message_path, thread, expires_at=None):
    """One local queue attempt. Ambiguous acknowledgement is never retried."""
    if not re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}',thread):
        raise PeerError('业务 agent 会话必须使用准确的小写 UUID。')
    entry = business_queue_entry(config['codex_path'])
    if expires_at is not None and time.time()>=expires_at:
        return {'state':'expired','reason':'已到本轮截止时间，未向协调会话发起新工作。'}
    message_path=Path(message_path).resolve()
    payload=message_path.read_bytes()
    if len(payload)>131072:
        raise PeerError('本机委派正文文件过大，未投递。')
    payload.decode('utf-8-sig')
    digest=hashlib.sha256(payload).hexdigest()
    # Windows PowerShell 5.1 can split embedded quotes even through @args wrappers.
    # Queue only a quote-free file reference; keep the complete UTF-8 body on disk.
    reference=business_message_reference(message_path,digest)
    command = ("[Console]::OutputEncoding = New-Object Text.UTF8Encoding($false)\n"
               "$businessMessage = " + _powershell_literal(reference) + "\n"
               + entry + ' queue --thread ' + _powershell_literal(thread) + ' --message $businessMessage\nexit $LASTEXITCODE')
    try:
        result = subprocess.run(['powershell.exe','-Command',command],stdin=subprocess.DEVNULL,capture_output=True,
                                text=True,encoding='utf-8',errors='replace',timeout=45)
    except subprocess.TimeoutExpired:
        return {'state':'unknown','reason':'本机队列回执超时，暂不重发，请先核实业务会话。'}
    receipt = re.search(r'Queued message ([0-9a-f-]{36}) for thread ([0-9a-f-]{36})',result.stdout)
    if result.returncode == 0 and receipt and receipt[2] == thread:
        return {'state':'queued','queue_message_id':receipt[1],'thread':receipt[2], 'reason':'已交给绑定的本机业务 agent，等待处理回报。'}
    # Do not persist arbitrary CLI output: it can contain user-specific remote details.
    rejected = bool(re.search(r'(?i)(thread|session).*(not found|ambiguous)|unrecognized subcommand|unknown command|error: unexpected argument|no-daemon cannot be used with codex queue',result.stderr+'\n'+result.stdout))
    return {'state':'rejected' if rejected else 'unknown','reason':'业务会话投递未得到有效回执，请检查会话ID和本机入口；不会盲目重发。','exit_code':result.returncode}


def codex_desktop_registered():
    """Detect the installed desktop handler, without installing or changing registration."""
    if os.name != 'nt':
        return False
    try:
        import winreg
        with winreg.OpenKey(winreg.HKEY_CLASSES_ROOT, 'codex') as key:
            winreg.QueryValueEx(key, 'URL Protocol')
        return True
    except (ImportError, OSError):
        return False


def business_wake_adapter(config):
    """CLI wins when both installed frontends are present."""
    from business_consumer import cli_command
    try:
        cli_command(config)
        return 'cli'
    except (KeyError, PeerError, OSError):
        return 'app' if codex_desktop_registered() else 'unavailable'


def wake_queued_business(config, thread, queue_ids, expires_at):
    """Resume only the bound queue; never submit another prompt or accept approvals."""
    if time.time() >= expires_at:
        return dict(state='expired', reason='已到截止时间，未唤醒业务会话。')
    health = business_queue_health(config, thread, queue_ids)
    if time.time() >= expires_at:
        return dict(state='expired', reason='已到截止时间，未唤醒业务会话。')
    if not health['queue_available']:
        return dict(state='unavailable', reason='无法核实原队列，未重复投递或启动新任务。')
    adapter = business_wake_adapter(config)
    if adapter == 'cli':
        from business_consumer import launch
        machine = json.loads((ROOT / '.local/machine.json').read_text(encoding='utf-8'))
        from peer import project_local
        policy = json.loads((project_local(ROOT,config) / ('business-agent-' + machine['peer'] + '.json')).read_text(encoding='utf-8'))
        if policy.get('thread') != thread:
            return dict(state='unavailable', reason='原绑定已变化，未唤醒旧会话。')
        return launch(ROOT, config, machine['peer'], thread, expires_at)
    if not any(item['pending'] for item in health['queued_items']):
        return dict(state='consumed', reason='原消息已离开队列，继续等待业务回报，不重复唤醒。')
    if adapter != 'app' or not health['desktop_wakeup_available']:
        return dict(state='unavailable', reason='原消息仍在队列，本机没有可用的CLI或桌面唤醒入口。')
    uri = 'codex://threads/' + thread
    try:
        result = subprocess.run(['powershell.exe', '-NoProfile', '-Command',
                                 "Start-Process -FilePath " + _powershell_literal(uri)
                                 + " -WindowStyle Hidden -ErrorAction Stop"],
                                stdin=subprocess.DEVNULL, capture_output=True, timeout=15)
    except (OSError, subprocess.TimeoutExpired):
        return dict(state='unknown', reason='业务会话唤醒回执不明，保留原任务，不自动重复唤醒。')
    if result.returncode != 0:
        return dict(state='unavailable', reason='已注册的桌面会话入口启动失败，原消息和业务状态均保留。')
    return dict(state='requested', reason='已自动请求打开原业务会话，等待原队列被处理及业务回报；没有重复派单。')


def business_queue_health(config, thread, queue_ids):
    """Read only the bound session's queue metadata; no prompt, credentials or repair."""
    if not re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', thread):
        raise PeerError('诊断会话无效。')
    queue_ids = list(dict.fromkeys(queue_ids))[:8]
    if any(not isinstance(item, str) or not re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', item) for item in queue_ids):
        raise PeerError('诊断队列编号无效。')
    snapshot = dict(schema_version=1, checked_at=time.time(), thread=thread,
                    runtime_status='unavailable', auto_start=None, queue_available=False, queued_items=[],
                    desktop_wakeup_available=codex_desktop_registered())
    sqlite_root = Path(os.environ.get('CODEX_HOME') or (Path.home() / '.codex'))
    try:
        entry = business_queue_entry(config['codex_path'])
        result = subprocess.run(['powershell.exe', '-Command', entry + ' doctor --json'],
                                stdin=subprocess.DEVNULL, capture_output=True, text=True,
                                encoding='utf-8', errors='replace', timeout=15)
        if len(result.stdout) <= 262144 and result.returncode in (0, 1):
            report = json.loads(result.stdout)
            checks = report.get('checks', {})
            runtime = checks.get('app_server.status', {})
            status = runtime.get('status')
            snapshot['runtime_status'] = status if status in ('ok', 'warning', 'error') else 'unavailable'
            mode = runtime.get('details', {}).get('mode')
            if mode in ('ephemeral', 'managed', 'dedicated'):
                snapshot['runtime_mode'] = mode
            version = str(report.get('codexVersion', ''))
            if re.fullmatch(r'[0-9A-Za-z._+-]{1,80}', version):
                snapshot['codex_version'] = version
            details = checks.get('config.load', {}).get('details', {})
            configured_root = details.get('sqlite home') or details.get('CODEX_HOME')
            if isinstance(configured_root, str) and configured_root:
                sqlite_root = Path(configured_root)
            flags = str(details.get('feature flag overrides', ''))
            if re.search(r'\bdaemon_auto_start\s*=\s*false\b', flags):
                snapshot['auto_start'] = False
            elif re.search(r'\bdaemon_auto_start\s*=\s*true\b', flags):
                snapshot['auto_start'] = True
    except (OSError, ValueError, TypeError, AttributeError, subprocess.TimeoutExpired, PeerError):
        pass
    database = sqlite_root / 'queue_1.sqlite'
    if database.is_file():
        try:
            db = sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True, timeout=1)
            try:
                columns = {row[1] for row in db.execute('PRAGMA table_info(queued_items)')}
                if {'id', 'thread_id', 'created_at_ms'} <= columns:
                    snapshot['queue_available'] = True
                    for identity in queue_ids:
                        row = db.execute('SELECT created_at_ms FROM queued_items WHERE id=? AND thread_id=?',
                                         (identity, thread)).fetchone()
                        item = dict(queue_message_id=identity, pending=row is not None)
                        if row is not None and type(row[0]) is int:
                            item['created_at_ms'] = row[0]
                        snapshot['queued_items'].append(item)
            finally:
                db.close()
        except (OSError, sqlite3.Error):
            pass
    # Absence from the queue alone is not evidence that business execution began.
    if snapshot['queue_available'] and queue_ids and not any(item['pending'] for item in snapshot['queued_items']):
        from business_consumer import recent_execution_health
        snapshot['native_execution']=recent_execution_health(ROOT,config,thread,queue_ids)
    return snapshot


def existing_connector(root, peer_id):
    from connector_update import safe_path
    runtime = safe_path(root, f"runtime/{peer_id}")
    for name in ("supervisor.lock", "run.lock"):
        owner = instance_lock_owner(runtime / name)
        if owner is not None:
            return owner
    return None


def connector_log(root, peer_id):
    """Only read logs within this peer's runtime, including the previous managed launcher."""
    from connector_update import safe_path
    runtime = safe_path(root, f"runtime/{peer_id}")
    journal = safe_path(root, f"runtime/{peer_id}/connector.log")
    if journal.is_file():
        return journal
    try:
        managed = json.loads((runtime / "managed-launcher.json").read_text(encoding="utf-8"))
        legacy = Path(managed["stdout"])
        legacy.resolve().relative_to(runtime.resolve())
        legacy = safe_path(root, legacy.relative_to(Path(root).resolve()))
        if legacy.is_file():
            return legacy
    except (OSError, ValueError, KeyError, TypeError, PeerError):
        pass
    return None


def interactive_console():
    if not sys.stdin.isatty():
        return False
    if os.name != "nt":
        return True
    # Windows isatty() also accepts NUL; only a real console should enter a log viewer.
    import ctypes
    from ctypes import wintypes
    import msvcrt
    try:
        handle = msvcrt.get_osfhandle(sys.stdin.fileno())
        mode = wintypes.DWORD()
        return bool(ctypes.windll.kernel32.GetConsoleMode(wintypes.HANDLE(handle), ctypes.byref(mode)))
    except (OSError, ValueError):
        return False


def follow_existing_connector(root, peer_id, interactive=None):
    owner = existing_connector(root, peer_id)
    if owner is None:
        return None
    suffix = f"（进程 {owner['pid']}）" if type(owner.get("pid")) is int else ""
    say(f"【连接器】{peer_id}端已有实例在运行{suffix}，本次不会重复启动。这不是启动失败。")
    journal = connector_log(root, peer_id)
    if journal:
        say(f"日志文件：{journal}")
    if interactive is None:
        interactive = interactive_console()
    if not interactive:
        return 0
    say("当前窗口用于查看日志；按 Ctrl+C 关闭查看，原连接器继续运行。")
    position, current, missing_since = 0, None, None
    try:
        while True:
            path = connector_log(root, peer_id)
            if path is not None:
                with path.open("rb") as stream:
                    size = stream.seek(0, 2)
                    changed = path != current or size < position
                    if changed:
                        start = max(0, size - 65536)
                        stream.seek(start)
                        tail = stream.read().decode("utf-8", errors="replace").splitlines()
                        for line in tail[-20:]:
                            say(line)
                    else:
                        stream.seek(position)
                        for line in stream.read().decode("utf-8", errors="replace").splitlines():
                            say(line)
                    position = stream.tell()
                    current = path
            if existing_connector(root, peer_id) is None:
                missing_since = missing_since or time.monotonic()
                if time.monotonic() - missing_since >= 5:
                    say("原连接器已退出。需要重新启动时，请再次双击 start.cmd。")
                    return 0
            else:
                missing_since = None
            time.sleep(0.5)
    except KeyboardInterrupt:
        say("已关闭日志查看，原连接器未被本窗口停止。")
        return 0


def ensure_business_agent(config, peer_id, client_factory=None):
    """Create one project coordinator, preserving existing self-registered sessions."""
    if not config.get('auto_coordinate', False):
        return {'state': 'manual'}
    if config.get('send_authorized') is not True or not configured_project(config):
        raise PeerError('接入身份和项目尚未核实，未创建协调会话。')
    from business_consumer import StdioClient, RpcError, cli_command
    from peer import BusinessAgentBridge, THREAD_ID
    from reliability import Store
    participant = next(p for p in config['peers'] if p['id'] == peer_id)
    workdir = Path(participant['workdir']).resolve()
    if not workdir.is_dir() or workdir == ROOT.resolve() or ROOT.resolve() in workdir.parents:
        raise PeerError('请先选择真实业务目录；连接器目录不能作为业务工程自动登记。')
    local = project_local(ROOT, config)
    local.mkdir(parents=True, exist_ok=True)
    binding = local / ('business-agent-' + peer_id + '.json')
    attempt = local / ('business-bootstrap-' + peer_id + '.json')
    with acquire_instance_lock(local / ('business-bootstrap-' + peer_id + '.lock')):
        if binding.exists():
            saved = json.loads(binding.read_text(encoding='utf-8-sig'))
            if (not re.fullmatch(THREAD_ID, str(saved.get('thread', '')))
                    or saved.get('workdir') not in (None, str(workdir))):
                raise PeerError('已有协调会话与业务目录不符，保留原登记；请核对项目选择。')
            return {'state': 'reused', 'thread': saved['thread']}
        previous = json.loads(attempt.read_text(encoding='utf-8')) if attempt.exists() else {}
        context = {'project_id': configured_project(config), 'peer': peer_id, 'workdir': str(workdir)}
        if previous and previous.get('context') != context:
            raise PeerError('原自动建会话记录属于其他业务目录，未覆盖或新建。')
        thread = previous.get('thread')
        if not re.fullmatch(THREAD_ID, str(thread or '')):
            if previous:
                raise PeerError('上次创建协调会话的回执尚未核实，记录已保留，不重复创建；可让原业务agent自行登记。')
            client = None
            try:
                client = (client_factory or StdioClient)(cli_command(config), str(workdir))
                client.call('initialize', {'clientInfo': {'name': 'dingtalk_peer_setup', 'version': '1'}})
                client.send({'method': 'initialized', 'params': {}})
                instructions = (
                    '你是本机自动创建的项目协调agent，使用中文。项目标识：' + configured_project(config)
                    + '；本端：' + peer_id + '；业务目录：' + str(workdir)
                    + '；连接器目录：' + str(ROOT.resolve()) + '。先读业务目录AGENTS.md及README。'
                    '联调职责和响应方式见连接器docs/coordinator-start.txt及每次已校验的委派文件。'
                    '默认只读；业务修改、程序启动及设备动作只按当前明确授权执行。'
                    '区分模拟输入、真实程序通信和现场验收；接口有明显错误倾向再向主协调确认。'
                    '只处理本项目真实身份核验后的任务，保存进度、证据和响应；不得输出任何凭据。'
                    '阶段完成且下一步等待新派单时，保存回报后结束当前AI回合，后台业务进程和截止守护继续运行；'
                    '不要用反复sleep或等待子agent占住回合，阻塞原生队列接续。'
                    '创建会话本身不授权修改文件或启动业务程序。')
                save_json(attempt, {'state': 'creating', 'context': context, 'created_at': time.time()})
                result = client.call('thread/start', {'cwd': str(workdir), 'ephemeral': False,
                                                       'developerInstructions': instructions}, timeout=45)
                thread = result.get('thread', {}).get('id')
                if not re.fullmatch(THREAD_ID, str(thread or '')):
                    raise PeerError('后台未返回有效协调会话编号；已保留创建记录，未再次创建。')
                save_json(attempt, {'state': 'created', 'context': context, 'thread': thread,
                                    'created_at': time.time()})
            except (RpcError, TimeoutError, OSError) as error:
                stage = '创建回执待核实，不会重复创建' if attempt.exists() else '尚未创建会话'
                raise PeerError('自动协调入口暂不可用（' + type(error).__name__ + '）：' + stage
                                + '。请检查Codex登录、网络/代理及本机入口；原环境和进度保留。') from None
            finally:
                if client is not None:
                    client.close()
        coordinator = min(p['id'] for p in config['peers'] if p['id'] != peer_id)
        runtime = project_runtime(ROOT, config, peer_id)
        runtime.mkdir(parents=True, exist_ok=True)
        store = Store(runtime / 'state.sqlite3')
        try:
            bind_store_context(store, config, participant)
            bridge = BusinessAgentBridge(config, participant, store, Transport(config, participant), ROOT)
            bridge.register_local(thread, coordinator)
            # The normal listener flushes the saved announcement; no second send here.
        finally:
            store.db.close()
        save_json(attempt, {'state': 'registered', 'context': context, 'thread': thread,
                            'registered_at': time.time()})
        return {'state': 'created', 'thread': thread}


def enable_default_updates(config):
    if config.get('send_authorized') is True and not safe_path(ROOT, '.local/update-policy.json').exists():
        from connector_update import enable
        enable(ROOT, min(peer['id'] for peer in config['peers']))
        say('已自动启用连接器专用更新，后续直接运行 start.cmd 即可。')


def start_connector(path, peer_id):
    result = follow_existing_connector(ROOT, peer_id)
    if result is not None:
        return result
    # Refresh identity only when starting an instance, not when viewing a running one.
    config = check_config(path)
    prepare_local_codex(config, peer_id)
    enable_default_updates(config)
    try:
        coordinator = ensure_business_agent(config, peer_id)
        if coordinator['state'] in ('created', 'reused'):
            say('【自动接入】' + ('已自动建立' if coordinator['state'] == 'created' else '已复用')
                + '本项目协调会话，无需另开窗口或手动关联；业务执行仍以接单回报为准。')
    except PeerError as error:
        say('【自动接入待处理】' + str(error))
    adapter=business_wake_adapter(config)
    say({'cli':'业务会话唤醒入口：CLI（两种入口都有时优先CLI）。',
         'app':'业务会话唤醒入口：App（未找到可直接运行的CLI）。',
         'unavailable':'业务会话唤醒入口暂不可用；原队列会保留并报告原因。'}[adapter])
    say("连接器启动中；出现 ready 后即可接任务。停止连接器请在此窗口按 Ctrl+C。")
    result = run_supervisor([sys.executable, str(ROOT / "supervisor.py"), "--config", str(path), "--peer", peer_id])
    if result == 20:
        # Another launcher may have won the race after the preflight check.
        viewed = follow_existing_connector(ROOT, peer_id)
        if viewed is None:
            say("另一实例刚刚退出；需要启动时请再次双击 start.cmd。")
        return viewed or 0
    return result


def run_json(command):
    result = subprocess.run(command, capture_output=True, encoding="utf-8", errors="replace", timeout=60)
    try:
        data = json.loads(result.stdout or result.stderr)
    except ValueError:
        raise PeerError(f"工具没有返回预期结果，请运行 {CHECK_ENTRY} 检查环境；原始输出未展示以保护凭据。") from None
    if result.returncode or "error" in data:
        err = data.get("error", {})
        raise PeerError(f"工具调用未成功（code={err.get('code', result.returncode)}, reason={err.get('reason', 'unknown')}），请检查账号和群权限。")
    return data


def choose_profile(data, input_fn=input):
    profiles = data.get("profiles", [])
    current = [p for p in profiles if p.get("isOrgCurrent") is True]
    if len(current) == 1:
        return current[0]
    if not profiles:
        raise PeerError(f"没有可用的钉钉登录身份，请重新运行 {SETUP_ENTRY} 登录。")
    say("请选择本机使用的钉钉身份：")
    for index, profile in enumerate(profiles, 1):
        say(f"  {index}. {profile.get('corpName', '')} / {profile.get('userName', '')}")
    while True:
        answer = input_fn("输入序号：").strip()
        if answer.isdigit() and 1 <= int(answer) <= len(profiles):
            return profiles[int(answer) - 1]


def dws_login_command(executable):
    result = subprocess.run([executable, "auth", "login", "--help"], capture_output=True, encoding="utf-8", errors="replace", timeout=30)
    command = [executable, "auth", "login"]
    if "--recommend" in result.stdout:
        command.append("--recommend")
    return command


def ensure_dws_login(tools, finish_initial_login=False):
    try:
        status = run_json([tools["dws_path"], "auth", "status", "--format", "json"])
    except PeerError:
        status = {"authenticated": False}
    if not status.get("authenticated") or finish_initial_login:
        say("请在浏览器完成本人钉钉授权，再回到此窗口等待。此阶段不需要粘贴 Webhook。")
        command = dws_login_command(tools["dws_path"])
        if "--recommend" in command:
            say("将使用 DWS 推荐授权流程，避免停在终端的权限多选界面。")
        else:
            say("这个 DWS 版本需要手动完成终端选择：按空格选择所需项，回车确认。")
        if subprocess.run(command).returncode:
            raise PeerError("钉钉登录未完成，稍后重新双击 start.cmd 即可。")
    profile = choose_profile(run_json([tools["dws_path"], "profile", "list", "--format", "json"]))
    say(f"钉钉身份：{profile.get('corpName', '')} / {profile.get('userName', '')}")
    return profile


def _contains_thread(home, thread):
    for database in home.glob('state_*.sqlite'):
        try:
            connection = sqlite3.connect(database.as_uri() + '?mode=ro', uri=True, timeout=1)
            try:
                if connection.execute('SELECT 1 FROM threads WHERE id=?', (thread,)).fetchone():
                    return True
            finally:
                connection.close()
        except sqlite3.Error:
            continue
    return False


def _select_home(saved, thread, setup):
    explicit = os.environ.get('CODEX_HOME')
    if explicit:
        return Path(explicit).expanduser().resolve()
    default = (Path.home() / '.codex').resolve()
    cli_home = (Path.home() / '.codex-cli').resolve()
    candidates = tuple(dict.fromkeys(home for home in (saved, default, cli_home)
                                    if home is not None and home.is_dir()))
    if thread:
        matches = [home for home in candidates if _contains_thread(home, thread)]
        if len(matches) == 1:
            return matches[0]
    if saved in candidates:
        return saved
    has_cli_config = (cli_home / 'config.toml').is_file()
    has_default_config = (default / 'config.toml').is_file()
    if has_cli_config and not has_default_config:
        return cli_home
    if has_cli_config and has_default_config and setup and sys.stdin.isatty():
        choice = input('Codex 目录：回车选择 .codex，输入 2 选择 .codex-cli：').strip()
        if choice == '2':
            return cli_home
        if choice not in ('', '1'):
            raise PeerError('Codex 目录选项无效，请重新运行设置。')
    return default


def _provider(root, codex):
    from business_consumer import RpcError, StdioClient, cli_command

    client = None
    try:
        client = StdioClient(cli_command({'codex_path': codex}), str(root))
        client.call('initialize', {'clientInfo': {'name': 'dingtalk_peer_setup', 'version': '1'}}, timeout=8)
        client.send({'method': 'initialized', 'params': {}})
        result = client.call('config/read', {'includeLayers': False}, timeout=8)
        config = result.get('config', {})
        provider = config.get('model_provider')
        return provider, (config.get('model_providers') or {}).get(provider, {})
    except (OSError, TimeoutError, RpcError, PeerError):
        return None, {}
    finally:
        if client is not None:
            client.close()


def _has_dotenv_key(home, key):
    # Codex reads its own .env. Inspect the named assignment only, leaving the
    # credential in its existing source and letting Codex handle its syntax.
    dotenv = home / '.env'
    if not dotenv.is_file():
        return False
    with dotenv.open(encoding='utf-8-sig', errors='replace') as source:
        for line in source:
            name, separator, value = line.strip().partition('=')
            if separator and name.removeprefix('export ').strip() == key:
                value = value.strip()
                if value and not value.startswith('#') and value not in ('""', "''"):
                    return True
    return False


def _save_token(path, token):
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=path.name + '.', delete=False) as staged:
        temporary = Path(staged.name)
    try:
        protect(token, temporary)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _hidden_token():
    if not sys.stdin.isatty():
        raise PeerError('本机缺少模型网关凭据。请在连接器目录的 Windows 终端运行 start.cmd，首次隐藏输入。')
    with warnings.catch_warnings():
        warnings.simplefilter('error', getpass.GetPassWarning)
        try:
            token = getpass.getpass('首次配置：粘贴本人模型网关 Token（输入隐藏），然后回车：').strip()
        except (EOFError, getpass.GetPassWarning):
            raise PeerError('无法隐藏读取模型网关凭据。请在 Windows 终端运行 start.cmd。') from None
    if not token:
        raise PeerError('模型网关 Token 为空，请重新运行 start.cmd。')
    return token


def prepare_codex_startup(root, codex, thread=None, setup=False):
    """Load credentials into this launcher's process so its Codex children inherit them."""
    selection = safe_path(root, '.local/codex-home.json')
    saved = None
    if selection.is_file():
        value = json.loads(selection.read_text(encoding='utf-8-sig')).get('home')
        if isinstance(value, str) and value:
            saved = Path(value).resolve()
    home = _select_home(saved, thread, setup)
    if not home.is_dir():
        return
    os.environ['CODEX_HOME'] = str(home)
    if saved != home and (setup or saved is not None or home != (Path.home() / '.codex').resolve()):
        selection.parent.mkdir(parents=True, exist_ok=True)
        save_json(selection, {'home': str(home)})

    if not (home / 'config.toml').is_file():
        return
    provider, settings = _provider(root, codex)
    key = settings.get('env_key')
    if not isinstance(key, str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', key):
        return
    token = os.environ.get(key, '').strip()
    if not token and _has_dotenv_key(home, key):
        return

    scope = [os.path.normcase(str(home)), provider, settings.get('base_url'), key]
    identity = hashlib.sha256(json.dumps(scope, ensure_ascii=False).encode('utf-8')).hexdigest()
    path = safe_path(root, f'.local/codex-provider-{identity}.protected')
    path.parent.mkdir(parents=True, exist_ok=True)
    with acquire_instance_lock(path.with_suffix('.lock')):
        try:
            previous = unprotect(path).strip() if path.is_file() else ''
        except (OSError, ValueError):
            previous = ''
        if not token and previous and setup and sys.stdin.isatty():
            choice = input('模型网关凭据：回车复用，输入 2 重新粘贴：').strip()
            if choice == '2':
                token = _hidden_token()
            elif choice not in ('', '1'):
                raise PeerError('模型网关凭据选项无效，请重新运行设置。')
        token = token or previous or _hidden_token()
        if token != previous:
            _save_token(path, token)
        os.environ[key] = token


def prepare_local_codex(config, peer_id, setup=False):
    binding = safe_path(project_local(ROOT, config), f'business-agent-{peer_id}.json')
    thread = None
    if binding.is_file():
        data = json.loads(binding.read_text(encoding='utf-8-sig'))
        thread = data.get('thread')
    prepare_codex_startup(ROOT, config['codex_path'], thread=thread, setup=setup)


def codex_requires_openai_auth(codex):
    """Ask the selected CLI about its effective provider without reading its credentials."""
    from business_consumer import RpcError, StdioClient, cli_command
    client = None
    try:
        client = StdioClient(cli_command({'codex_path': codex}), str(ROOT))
        client.call('initialize', {'clientInfo': {'name': 'dingtalk_peer_setup', 'version': '1'}}, timeout=8)
        client.send({'method': 'initialized', 'params': {}})
        account = client.call('account/read', {'refreshToken': False}, timeout=8)
        return account.get('requiresOpenaiAuth') if isinstance(account, dict) else None
    except (OSError, TimeoutError, RpcError, PeerError):
        # Older CLIs may not expose this field. Preserve their existing login flow.
        return None
    finally:
        if client is not None:
            client.close()


def ensure_codex_auth(codex):
    """Return whether the configured provider handles its own authentication."""
    if codex_requires_openai_auth(codex) is False:
        say('Codex 使用本机配置的模型提供方，将通过一次实际调用核对连接和凭据。')
        return True
    check = subprocess.run([codex, "login", "status"], capture_output=True)
    if check.returncode:
        say("请在打开的浏览器中登录本人的 Codex / ChatGPT 账号。")
        if subprocess.run([codex, "login"]).returncode:
            raise PeerError("Codex 登录未完成，稍后重新双击 start.cmd 即可。")
    return False


def available_models(codex):
    process = subprocess.Popen([codex, "app-server"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    inbox = queue.Queue()
    def read():
        for line in process.stdout:
            try: inbox.put(json.loads(line))
            except ValueError: pass
        inbox.put(None)
    def drain():
        for _ in process.stderr: pass
    threading.Thread(target=read, daemon=True).start()
    threading.Thread(target=drain, daemon=True).start()
    def send(data):
        process.stdin.write(json.dumps(data).encode() + b"\n")
        process.stdin.flush()
    def receive(request_id):
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            try: message = inbox.get(timeout=max(0.1, deadline-time.monotonic()))
            except queue.Empty: break
            if message is None: break
            if message.get("id") == request_id:
                if "error" in message: raise PeerError("Codex 模型列表不可用，请确认登录账号可使用 Codex。")
                return message["result"]
        raise PeerError("读取 Codex 模型列表超时，请检查网络后重试。")
    try:
        send({"id": 0, "method": "initialize", "params": {"clientInfo": {"name": "dingtalk_peer_setup", "version": "0.2.0"}}})
        receive(0)
        send({"method": "initialized", "params": {}})
        send({"id": 1, "method": "model/list", "params": {"limit": 100, "includeHidden": False}})
        return receive(1)["data"]
    finally:
        process.stdin.close()
        try: process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.terminate()  # Owned short-lived stdio RPC server; not a DWS subscription.
            process.wait(timeout=10)


def select_model(models):
    visible = [m for m in models if not m.get("hidden") and "text" in m.get("inputModalities", ["text"])]
    if not visible:
        raise PeerError("这个 Codex 账号没有可用的文本模型。")
    return next((m.get("model", m["id"]) for m in visible if m.get("isDefault")), visible[0].get("model", visible[0]["id"]))


def prepare_credential(peer_id, workbook=None, webhook=None):
    template = configuration_template()
    local = ROOT / ".local"
    local.mkdir(exist_ok=True)
    credential = local / f"webhook-{peer_id}.protected"
    peer = next(p for p in template["peers"] if p["id"] == peer_id)
    if workbook:
        robots = workbook_robots(workbook)
        if peer["name"] not in robots:
            raise PeerError("表格中没有找到所选本人的机器人，请核对 D 列名称。")
        webhook = "https://oapi.dingtalk.com/robot/send?" + urlencode({"access_token": robots[peer["name"]]})
    if webhook:
        parsed = urlparse(webhook.strip())
        values = parse_qs(parsed.query)
        if parsed.scheme != "https" or parsed.hostname != "oapi.dingtalk.com" or parsed.path != "/robot/send" or set(values) != {"access_token"} or len(values["access_token"]) != 1:
            raise PeerError("Webhook 格式不正确；本版使用关键词校验，不支持加签 URL。")
        protect(webhook.strip(), credential)
    elif not credential.exists():
        raise PeerError("首次部署需要本人的 Webhook 或含机器人配置的 Excel 文件。")
    else:
        unprotect(credential)  # Fail now if copied from another Windows account.
    return credential


def save_config(tools, peer_id, project, workbook=None, webhook=None, profile=None, model=None, destination=None, project_id=None):
    local = ROOT / ".local"
    path = Path(destination) if destination else local / "config.json"
    previous = json.loads(path.read_text(encoding="utf-8-sig")) if path.exists() else None
    if existing_connector(ROOT, peer_id) is not None:
        raise PeerError("请先正常停止本机连接器，再配置或切换项目。")
    project = Path(project).resolve()
    if not project.is_dir():
        raise PeerError("项目目录不存在，请选择真实目录。")
    old_peer = next((p for p in previous['peers'] if p['id'] == peer_id), None) if previous else None
    if project_id is None:
        if old_peer and Path(old_peer['workdir']).resolve() == project:
            project_id = configured_project(previous)
        else:
            project_id = configured_project(configuration_template())
            if not project_id:
                raise PeerError("新项目需要明确的项目标识；双方使用相同标识，不能沿用旧任务和agent绑定。")
    configured_project({"project_id": project_id})
    context_file = safe_path(project_local(ROOT, {"project_id": project_id}), "config.json")
    if context_file.exists():
        config = json.loads(context_file.read_text(encoding="utf-8-sig"))
    elif previous and configured_project(previous) == project_id:
        config = copy.deepcopy(previous)
    else:
        config = configuration_template()
    config["project_id"] = project_id
    credential = prepare_credential(peer_id, workbook, webhook)
    peer = next(p for p in config["peers"] if p["id"] == peer_id)
    config.pop("workbook", None)
    config.update({k: tools[k] for k in ("dws_path", "codex_path")})
    config.update(credential_file=str(credential), profile=profile, model=model, send_authorized=True, isolate_codex_config=False)
    config.setdefault('auto_coordinate', True)
    peer["workdir"] = str(project)
    for other in config["peers"]:
        if other["id"] != peer_id:
            other["workdir"] = "由对端电脑自行配置；本机不使用"
    if path.exists():
        (path.parent / f"{path.name}.{time.time_ns()}.bak").write_bytes(path.read_bytes())
    if previous and configured_project(previous):
        save_json(safe_path(project_local(ROOT, previous), "config.json"), previous)
    if context_file != path:
        save_json(context_file, config)
    save_json(path, config)
    (local / "machine.json").write_text(json.dumps({"peer": peer_id, "python_path": tools["python_path"], "checked": False}, indent=2), encoding="utf-8")
    return path


def resolve_group_identities(config, membership, template):
    """Pin real group identities; translate account-scoped IDs for this profile."""
    if membership.get("conversationId") != config["group_id"]:
        raise PeerError("群成员查询返回了其他群，原有身份配置未修改。")
    if (membership.get("complete") is not True or membership.get("partial")
            or membership.get("hasMore") or membership.get("failedCount", 0)
            or membership.get("failures")):
        raise PeerError("群成员查询不完整，不能校准机器人和主人身份；请保留配置后重试。")
    bots, users = membership.get("bots"), membership.get("users")
    if not isinstance(bots, list) or not isinstance(users, list):
        raise PeerError("群成员查询缺少机器人或成员列表，原有身份配置未修改。")
    resolved = copy.deepcopy(config)
    for peer in resolved["peers"]:
        if not peer.get("bot_id") or not peer.get("owner_member_id"):
            # Upgrade only this exact known deployment. Never authorize by name.
            legacy = [p for p in template.get("peers", [])
                      if template.get("group_id") == config["group_id"]
                      and p["id"] == peer["id"]
                      and p.get("sender_id") == peer.get("sender_id")
                      and p.get("owner_id") == peer.get("owner_id")]
            if len(legacy) != 1 or not all(legacy[0].get(k) for k in ("bot_id", "owner_member_id")):
                raise PeerError(f"{peer['id']} 端缺少稳定身份标识。请同时更新 setup_helper.py 和 config.example.json；不会按名称放行。")
            for key in ("bot_id", "owner_member_id"):
                if peer.get(key) and peer[key] != legacy[0][key]:
                    raise PeerError(f"{peer['id']} 端稳定身份配置冲突，原有身份配置未修改。")
                peer[key] = legacy[0][key]
        matches = [b for b in bots if b.get("openBotId") == peer["bot_id"]]
        owners = [u for u in users if u.get("memberDingtalkId") == peer["owner_member_id"]]
        if len(matches) != 1 or not matches[0].get("openDingtalkId"):
            raise PeerError(f"群内未唯一找到 {peer['id']} 端原机器人，请确认该机器人仍在群中；原有身份配置未修改。")
        if len(owners) != 1 or not owners[0].get("openDingtalkId"):
            raise PeerError(f"群内未唯一找到 {peer['id']} 端原主人，请确认参与同事仍在群中；原有身份配置未修改。")
        peer["sender_id"] = matches[0]["openDingtalkId"]
        peer["owner_id"] = owners[0]["openDingtalkId"]
    for key in ("bot_id", "sender_id", "owner_member_id", "owner_id"):
        if len({p[key] for p in resolved["peers"]}) != len(resolved["peers"]):
            raise PeerError("多个参与端绑定了相同身份，原有身份配置未修改。")
    return resolved


def check_config(path):
    config = load_config(path)
    membership = run_json([config["dws_path"], "chat", "+chat-members-list", "--group", config["group_id"], "--profile", config["profile"], "--format", "json"])
    template = configuration_template()
    resolved = resolve_group_identities(config, membership, template)
    if resolved != config:
        path = Path(path)
        (path.parent / f"{path.name}.{time.time_ns()}.bak").write_bytes(path.read_bytes())
        path.write_text(json.dumps(resolved, ensure_ascii=False, indent=2), encoding="utf-8")
    if configured_project(resolved):
        snapshot = safe_path(project_local(ROOT, resolved), "config.json")
        if snapshot != Path(path):
            save_json(snapshot, resolved)
    say("测试群及机器人身份检查通过，已按当前登录账号校准机器人和主人 ID。")
    return resolved


def check_codex(config, provider_auth=False):
    if provider_auth:
        config = dict(config, model=None, isolate_codex_config=False)
    probe = ROOT / ".local/check-workspace"
    probe.mkdir(parents=True, exist_ok=True)
    response = probe / f"response-{time.time_ns()}.json"
    command, features = codex_command(config, {"workdir": str(probe)}, response, ROOT / "response.schema.json")
    prompt = '只验证Codex是否能生成回答，不要调用任何工具、读取文件或执行命令。只输出JSON：{"action":"finish","to":"A","text":"本机Codex检查通过"}'
    try:
        result = subprocess.run(command, input=prompt, capture_output=True, encoding="utf-8", timeout=120, cwd=probe)
    except subprocess.TimeoutExpired:
        if provider_auth:
            raise PeerError('配置的 Codex 提供方在 120 秒内未返回结果。请检查本机到网关的网络、base_url 和服务状态。') from None
        raise PeerError('Codex连接检查超过120秒，尚未取得回答。请确认网络/代理节点可访问Codex，且本人的Codex登录有效；网络恢复后重新运行start.cmd即可，配置和已有进度保留，无需重装。') from None
    if result.returncode:
        if provider_auth:
            raise PeerError('配置的 Codex 提供方未通过实际调用。请核对本机 config.toml 的 model_provider、base_url、可用模型和本人 API 凭据，修正后再运行 start.cmd。令牌仅在本机填写。')
        raise PeerError("当前Codex未能完成连接检查。请先确认网络/代理、本人Codex登录和账号可用模型；若提示Windows沙箱初始化，再按本机提示处理。已有版本、配置和进度保留，无需重装。")
    try:
        answer = parse_agent_response(result, response, features)
    except ValueError:
        raise PeerError("当前 Codex 未返回可解析的检查结果；不会自动更换其版本。") from None
    if answer.get("action") != "finish":
        raise PeerError("Codex 检查结果异常。")
    say("本机 Codex 实际调用检查通过。")


def main():
    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers(dest="command", required=True)
    npm = subs.add_parser("install-npm")
    npm.add_argument("--node", required=True)
    npm.add_argument("--prefix", required=True)
    npm.add_argument("--packages", nargs="+", required=True)
    configure = subs.add_parser("configure")
    configure.add_argument("--peer", required=True)
    configure.add_argument("--project", required=True)
    configure.add_argument("--project-id", help="双方共享的项目标识；新项目必填，已有旧部署可保持原标识")
    configure.add_argument("--workbook")
    configure.add_argument("--paste-webhook", action="store_true")
    subs.add_parser("participants")
    subs.add_parser('onboarding-diagnostics')
    subs.add_parser('webhook-diagnostics')
    subs.add_parser('detect-project')
    discover = subs.add_parser("discover")
    discover.add_argument("--partner")
    discover.add_argument("--project-id")
    discover.add_argument("--ask-project", action="store_true")
    for command in ("check", "start", "task", "stop-task", "resume", "enable-updates", "publish-update", "register-agent"):
        subs.add_parser(command)
    args = parser.parse_args()
    if args.command == 'detect-project':
        print(json.dumps({'path': detect_project_directory()}, ensure_ascii=False))
        return 0
    if args.command == 'onboarding-diagnostics':
        collect_onboarding_diagnostics()
        return 0
    if args.command == 'webhook-diagnostics':
        diagnose_webhook_send()
        return 0
    if args.command == "participants":
        template = configuration_template()
        print(json.dumps({"project_id": configured_project(template), "peers": [
            {"id": p["id"], "name": p["name"], "owner_name": p.get("owner_name", p["name"].removeprefix("联调-AI-"))}
            for p in template["peers"]]}, ensure_ascii=False))
        return 0
    tools = json.loads((ROOT / ".tools/toolchain.json").read_text(encoding="utf-8-sig")) if args.command != "install-npm" else None
    if args.command == 'discover':
        discover_local(tools, args.partner, args.project_id, args.ask_project)
        return 0
    if args.command == "install-npm":
        node = Path(args.node)
        env = dict(os.environ)
        env["PATH"] = str(node.parent) + os.pathsep + env.get("PATH", "")
        command = [str(node), str(node.parent / "node_modules/npm/bin/npm-cli.js"), "install", "--prefix", args.prefix, "--registry", "https://registry.npmjs.org", "--no-audit", "--no-fund", "--save-exact", *args.packages]
        return subprocess.run(command, env=env).returncode
    if args.command == "configure":
        participant = next((p for p in configuration_template()["peers"] if p["id"] == args.peer), None)
        if participant is None:
            raise PeerError("所选参与者不在本部署配置中。")
        # Finish all secret input before the CLI owns the terminal for OAuth.
        webhook = getpass.getpass("粘贴本人 Webhook（内容不显示），然后回车：").strip() if args.paste_webhook else None
        prepare_credential(args.peer, args.workbook, webhook)
        profile = ensure_dws_login(tools)
        expected = participant.get("owner_name", participant["name"].removeprefix("联调-AI-"))
        discovery_file = ROOT / '.local/discovery.json'
        discovery = json.loads(discovery_file.read_text(encoding='utf-8')) if discovery_file.exists() else None
        if discovery and (discovery['profile'] != profile['profile'] or discovery['peer'] != args.peer):
            raise PeerError('登录身份或本人端ID在检测后发生了变化，请重新运行setup检测。')
        if not discovery and profile.get("userName") != expected:
            raise PeerError(f"当前钉钉登录为 {profile.get('userName', '')}，所选机器人属于 {expected}。请用本人身份登录或重新选择机器人。")
        prepare_local_codex({'project_id': args.project_id, 'codex_path': tools['codex_path']}, args.peer, setup=True)
        provider_auth = ensure_codex_auth(tools['codex_path'])
        model = None
        if not provider_auth:
            try:
                model = select_model(available_models(tools["codex_path"]))
            except PeerError:
                say("当前 Codex 未提供模型列表，沿用本机原有模型配置，不更换版本。")
        path = save_config(tools, args.peer, args.project, profile=profile["profile"], model=model, project_id=args.project_id)
        config = check_config(path)
        try:
            verify_webhook_identity(config, args.peer, retry_rejected=True)
        except PeerError:
            config['send_authorized'] = False
            save_json(path, config)
            if configured_project(config):
                save_json(safe_path(project_local(ROOT, config), 'config.json'), config)
            raise
        enable_default_updates(config)
        say(f"配置完成：{expected}，模型 {model or '沿用 Codex 本机配置'}。Webhook 已按本机 Windows 用户加密保存。")
        return 0
    path = ROOT / ".local/config.json"
    if path.exists():
        saved = json.loads(path.read_text(encoding="utf-8"))
        changed = False
        for key in ("dws_path", "codex_path"):
            if saved.get(key) != tools[key]:
                saved[key] = tools[key]
                changed = True
        if changed:
            path.write_text(json.dumps(saved, ensure_ascii=False, indent=2), encoding="utf-8")
    config = load_config(path)
    machine = json.loads((ROOT / ".local/machine.json").read_text(encoding="utf-8"))
    if args.command == "check":
        ensure_dws_login(tools)
        prepare_local_codex(config, machine['peer'])
        provider_auth = ensure_codex_auth(tools['codex_path'])
        config = check_config(path)
        check_codex(config, provider_auth=provider_auth)
        if provider_auth and (config.get('model') is not None or config.get('isolate_codex_config', True)):
            config.update(model=None, isolate_codex_config=False)
            save_json(path, config)
            if configured_project(config):
                save_json(safe_path(project_local(ROOT, config), 'config.json'), config)
        machine_file = ROOT / ".local/machine.json"
        machine_data = json.loads(machine_file.read_text(encoding="utf-8"))
        machine_data["checked"] = True
        machine_file.write_text(json.dumps(machine_data, indent=2), encoding="utf-8")
        return 0
    peer = machine["peer"]
    if args.command == 'register-agent':
        from peer import BusinessAgentBridge
        from reliability import Store
        thread=os.environ.get('CODEX_THREAD_ID','')
        if not re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}',thread):
            raise PeerError('请在需要登记的协调agent自身工具环境运行；当前没有有效的CODEX_THREAD_ID。')
        local=next(p for p in config['peers'] if p['id']==peer)
        coordinator=min(p['id'] for p in config['peers'] if p['id']!=peer)
        runtime=project_runtime(ROOT,config,peer)
        runtime.mkdir(parents=True,exist_ok=True)
        store=Store(runtime/'state.sqlite3')
        try:
            bind_store_context(store,config,local)
            bridge=BusinessAgentBridge(config,local,store,Transport(config,local),ROOT)
            bridge.register_local(thread,coordinator)
            bridge.flush_one()
        finally:
            store.db.close()
        say(f'本机协调会话已自动登记：{thread}。连接器将用本人机器人回报，后续按此会话派单。')
        return 0
    if args.command == "enable-updates":
        from connector_update import enable
        publisher = min(p["id"] for p in config["peers"])
        enable(ROOT, publisher)
        say(f"已启用连接器专用更新；只接受配置中的 {publisher} 机器人身份发布。发布端会在代码稳定并校验后自动发布；私人配置、凭据和业务项目不参与更新。")
        return 0
    if args.command == "publish-update":
        return subprocess.run([sys.executable, str(ROOT / "peer.py"), "--config", str(path), "--peer", peer, "publish-update"]).returncode
    if args.command == "start":
        return start_connector(path, peer)
    if args.command == "task":
        goal = input("输入本次联调目标：").strip()
        if not goal: return 0
        others = [p["id"] for p in config["peers"] if p["id"] != peer]
        target = others[0] if len(others) == 1 else input("接收方ID：").strip()
        return subprocess.run([sys.executable, str(ROOT / "peer.py"), "--config", str(path), "--peer", peer, "start", "--to", target, "--goal", goal]).returncode
    if args.command == "resume":
        task = input("输入要继续的任务编号（直接回车继续本机最近任务）：").strip() or "latest"
        goal = input("补充本次目标（可直接回车沿用）：").strip()
        others = [p["id"] for p in config["peers"] if p["id"] != peer]
        target = others[0] if len(others) == 1 else input("接收方ID：").strip()
        command = [sys.executable, str(ROOT / "peer.py"), "--config", str(path), "--peer", peer, "resume", "--task", task, "--to", target]
        if goal: command += ["--goal", goal]
        return subprocess.run(command).returncode
    task = input("输入要停止的12位任务编号：").strip()
    return subprocess.run([sys.executable, str(ROOT / "peer.py"), "--config", str(path), "--peer", peer, "stop", "--task", task]).returncode


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    try:
        sys.exit(main())
    except (PeerError, ValueError, OSError) as error:
        say(str(error))
        sys.exit(1)
