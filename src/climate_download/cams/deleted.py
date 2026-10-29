"""One bounded replacement for a positively identified deleted ADS order."""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
import re
import time

from climate_download.cams import recovery
from climate_download.cams.catalogue import DATASET, SOURCE
from climate_download.cams.state import digest, now, safe_error, save, signature

KIND = 'ads_order_deleted'


def classify(error, request_id):
    if getattr(getattr(error, 'response', None), 'status_code', None) != 404:
        return None
    match = re.search(r'\bjob\s+'+re.escape(request_id)+r'\s+(deleted|dismissed)\b', str(error), re.I)
    return {'kind': KIND, 'http_status': 404, 'disposition': match[1].lower()} if match else None


def saved_deleted(state):
    error = state.get('error') or {}
    message = error.get('message', '')
    return bool(state.get('request_id') and re.search(r'\b404\b', message)
                and re.search(r'\bjob\s+'+re.escape(state['request_id'])+r'\s+(deleted|dismissed)\b', message, re.I))


def _states(config, folder, state):
    root = config.output_dir/SOURCE/state['date']/f"{state['cycle']:02d}z"/'.cams'
    paths = set(root.glob('*/state.json')) | set(root.glob('hourly/parts/*/state.json'))
    paths.discard(folder/'state.json')
    return [state]+[json.loads(path.read_text()) for path in paths]


def allowed(config, folder, state):
    limit = min(config.deleted_order_retry_limit, (state.get('deleted_retry_policy') or {}).get('max_retries', 0))
    used = sum(attempt.get('failure_kind') == KIND for current in _states(config, folder, state)
               for attempt in current.get('previous_attempts', []))
    return limit == 1 and used < limit and recovery._allowed(config, state, folder)


def record(config, folder, state, request, error, secret, *, reply=None):
    """Keep the original state/request bytes and a separate immutable deletion observation."""
    remote_verified = bool(reply and reply.get('status') in ('deleted','dismissed')
                           and signature(recovery._remote_request(reply)) == signature(request))
    diagnostic = ({'kind': KIND, 'http_status': None, 'disposition': reply['status'],
                   'evidence_source': 'remote_status'} if remote_verified else classify(error, state['request_id']))
    if not diagnostic:
        raise recovery.RecoveryBlocked('ADS disappearance is not positively identified as deletion')
    request_path = folder/'request.json'
    if (not request_path.is_file() or signature(json.loads(request_path.read_text())) != signature(request)
            or state.get('request_sha256') != signature(request)):
        raise recovery.RecoveryBlocked('Deleted ADS order has no verified original local request')
    directory = 'failures/'+signature(state['request_id'])[:16]
    archive = folder/directory
    previous = state.get('failed_attempt')
    if previous and previous['request_id'] == state['request_id']:
        recovery._failure_evidence(folder, state, request)
    else:
        recovery._archive_exact(archive/'state.json', (folder/'state.json').read_bytes())
        recovery._archive_exact(archive/'request.json', request_path.read_bytes())
    observation = archive/'deleted_observation.json'
    if not observation.is_file():
        recovery._archive_exact(archive/'deleted_state.json', (folder/'state.json').read_bytes())
        observed = state['last_check'] if saved_deleted(state) and state.get('last_check') else now()
        stamp = dt.datetime.fromisoformat(observed)
        if stamp.tzinfo is None or stamp > dt.datetime.fromisoformat(now()):
            raise recovery.RecoveryBlocked('Deleted ADS order has an invalid original observation time')
        value = {'captured_at': observed, 'verified_at': now(), 'request_id': state['request_id'],
                 'request_sha256': signature(request), 'account': state['account'], 'diagnostic': diagnostic,
                 'local_request_verified': True, 'remote_request_verified': remote_verified, 'deleted_response_verified': True,
                 'original_deletion_state_sha256': digest(archive/'deleted_state.json'),
                 'error': safe_error(error, secret, max_chars=None)}
        recovery._archive_exact(observation, (json.dumps(value, ensure_ascii=False, indent=2)+'\n').encode())
    value = json.loads(observation.read_text())
    if not previous or previous['request_id'] != state['request_id']:
        recovery._archive_exact(archive/'failure.json', observation.read_bytes())
        state['failed_attempt'] = {'directory': directory, 'request_id': state['request_id'],
            'state_sha256': digest(archive/'state.json'), 'request_sha256': digest(archive/'request.json'),
            'failure_sha256': digest(archive/'failure.json')}
    policy_path = archive/'deleted_policy.json'
    if not policy_path.is_file():
        policy = state.get('deleted_retry_policy') or {'max_retries': config.deleted_order_retry_limit, 'captured_at': now()}
        entry = {'request_id': state['request_id'], 'request_sha256': signature(request), 'policy': policy,
                 'observation_sha256': digest(observation), 'observed_at': value['captured_at']}
        recovery._archive_exact(policy_path, (json.dumps(entry, ensure_ascii=False, indent=2)+'\n').encode())
    entry = json.loads(policy_path.read_text())
    state.update(phase='remote_failed', error=safe_error(error, secret), last_check=now(),
                 failure_diagnostic=diagnostic, deleted_retry_policy=entry['policy'],
                 deleted_recovery={'directory': str(policy_path.relative_to(folder)), 'sha256': digest(policy_path)})
    if config.deleted_order_retry_limit and not recovery.has_raw_evidence(folder) and allowed(config, folder, state):
        # The common evidence checker pins the due time to the original failure.
        failure = json.loads((archive/'failure.json').read_text())
        due = dt.datetime.fromisoformat(failure['captured_at'])+dt.timedelta(
            seconds=recovery._delays(state['mars_retry_policy'])[recovery.used_count(state)])
        state.update(phase='retry_wait', retry_due_at=due.isoformat())
    save(folder/'state.json', state)
    return state['phase'] == 'retry_wait'


def verify(config, folder, state, request):
    if not allowed(config, folder, state):
        raise recovery.RecoveryBlocked('Deleted-order replacement budget or shared one-day window is exhausted')
    recovery._failure_evidence(folder, state, request)
    proof = state['deleted_recovery'];path = folder/proof['directory']
    if (not path.resolve().is_relative_to((folder/'failures').resolve())
            or digest(path) != proof['sha256']):
        raise recovery.RecoveryBlocked('Deleted-order policy evidence changed')
    entry = json.loads(path.read_text());observation = path.with_name('deleted_observation.json')
    if (entry['request_id'] != state['request_id'] or entry['request_sha256'] != signature(request)
            or entry['policy'] != state['deleted_retry_policy'] or entry['policy']['max_retries'] != 1
            or digest(observation) != entry['observation_sha256']):
        raise recovery.RecoveryBlocked('Deleted-order request, policy or observation changed')
    value = json.loads(observation.read_text())
    if (value['diagnostic']['kind'] != KIND or not value['deleted_response_verified']
            or value['request_id'] != state['request_id'] or value['account'] != state['account']
            or digest(path.with_name('deleted_state.json')) != value['original_deletion_state_sha256']):
        raise recovery.RecoveryBlocked('Original deletion evidence changed')
    if recovery.has_raw_evidence(folder):
        raise recovery.RecoveryBlocked('Existing raw bytes must retain their original order')


def account_ready(client):
    return not client.get_jobs(limit=100, status=['accepted', 'running']).json['jobs']


def _exclude_duplicate(client, state, request):
    """Inspect all retained jobs created since the original submission intent."""
    def utc(value):
        stamp = dt.datetime.fromisoformat(value.replace('Z','+00:00'))
        # ADS job timestamps are UTC, including responses with no offset suffix.
        return stamp.replace(tzinfo=dt.timezone.utc) if stamp.tzinfo is None else stamp.astimezone(dt.timezone.utc)
    floor = utc(state.get('submission_started_at', state['created_at']))-dt.timedelta(minutes=5)
    page = client.get_jobs(limit=100, sortby='-created')
    seen = set()
    began = time.monotonic()
    def request_identity(value):
        return signature({key: sorted(map(str, items)) if key in ('variable','leadtime_hour','time','pressure_level')
                          and isinstance(items,list) else items for key,items in value.items()})
    def receipt(pages):
        return {'checked_at': now(), 'since': floor.isoformat(), 'pages': pages,
                'inspected_order_ids': sorted(seen)}
    for number in range(10):
        if time.monotonic()-began > 60:
            raise TimeoutError('Bounded ADS duplicate inspection deferred')
        jobs = page.json['jobs']
        for job in jobs:
            if time.monotonic()-began > 60:
                raise TimeoutError('Bounded ADS duplicate inspection deferred')
            if utc(job['created']) < floor:
                return receipt(number+1)
            rid = job['jobID']
            if rid == state['request_id'] or job.get('processID', DATASET) != DATASET:
                continue
            if job.get('status') is not None and job['status'] not in ('accepted','running','successful'):
                continue
            if rid in seen:
                raise recovery.RecoveryBlocked('ADS job pagination repeated an order')
            seen.add(rid)
            try:
                reply = client.get_remote(rid).json
            except Exception as error:
                if classify(error, rid):
                    continue
                raise
            if (reply.get('status') in ('accepted', 'running', 'successful')
                    and request_identity(recovery._remote_request(reply)) == request_identity(request)):
                raise recovery.RecoveryBlocked('Another matching ADS order exists; retain its ID instead of posting a duplicate')
        page = page.next if jobs else None
        if page is None:
            return receipt(number+1)
    raise recovery.RecoveryBlocked('ADS job inspection exceeded its bounded page limit')


def retry_decision(config, folder, state, request, client, secret):
    verify(config, folder, state, request)
    if dt.datetime.fromisoformat(now()) < dt.datetime.fromisoformat(state['retry_due_at']):
        return None
    try:
        try:
            reply = client.get_remote(state['request_id']).json
        except Exception as error:
            if not classify(error, state['request_id']):
                raise
        else:
            if signature(recovery._remote_request(reply)) != signature(request):
                raise recovery.RecoveryBlocked('Original remote request parameters changed')
            if reply['status'] in ('accepted', 'running', 'successful'):
                state.update(phase='submitted', retry_cancelled_reason='original_order_reactivated')
                save(folder/'state.json', state)
                return 'original'
            if reply['status'] not in ('deleted','dismissed'):
                raise recovery.RecoveryBlocked('Original ADS order is no longer verifiably deleted')
        if not account_ready(client):
            state.update(retry_wait_reason='another_active_order', last_check=now())
            save(folder/'state.json', state)
            return None
        state['deleted_duplicate_check'] = _exclude_duplicate(client, state, request)
    except recovery.RecoveryBlocked:
        raise
    except Exception as error:
        if getattr(getattr(error, 'response', None), 'status_code', None) in (400,401,403,404,422):
            raise recovery.RecoveryBlocked('Deleted ADS order or account cannot be verified') from None
        state.update(last_check=now(), retry_wait_reason='metadata_deferred', error=safe_error(error, secret))
        save(folder/'state.json', state)
        return None
    return 'retry'
