"""Explicit original-order recovery; an unknown POST is never an automatic retry."""
from __future__ import annotations

import datetime as dt
import fcntl
import json
from pathlib import Path
import time

from climate_download.cams import recovery
from climate_download.cams.catalogue import DATASET, SOURCE, request_for, request_variables
from climate_download.cams.state import digest, now, save, signature
from climate_download.cams.client import create_client, preflight

UNCERTAIN = {'submitting', 'submission_uncertain'}


def uncertain_groups(folder: Path, key: str) -> list[dict]:
    rows = []
    for path in sorted(list((folder/'.cams').glob('*/state.json'))+
                       list((folder/'.cams/hourly/parts').glob('*/state.json'))):
        state = json.loads(path.read_text())
        if state.get('phase') not in UNCERTAIN:
            continue
        if any(state.get(k, v) != v for k, v in (('date', key[:8]), ('cycle', int(key[8:])))):
            raise ValueError('Uncertain CAMS state identity changed')
        rows.append({'init': key, 'phase': state['phase'], 'group': state.get('group'),
                     'request_id': state.get('request_id'), 'state': str(path.relative_to(folder))})
    return rows


def uncertain_accounts(config) -> set[str]:
    accounts = set()
    for path in (config.output_dir/SOURCE).glob('????????/??z'):
        key = path.parent.name+path.name[:2]
        if (path/f'{key[:8]}_{key[8:]}z_{SOURCE}.manifest.json').is_file():
            continue
        if uncertain_groups(path, key):
            accounts.add(config.account_for(int(key[8:])).name)
    return accounts


def _stamp(value):
    stamp = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
    return stamp.replace(tzinfo=dt.UTC) if stamp.tzinfo is None else stamp.astimezone(dt.UTC)


def _identity(request):
    return signature({k: sorted(map(str, v)) if k in ('variable', 'leadtime_hour', 'time', 'pressure_level')
                      and isinstance(v, list) else v for k, v in request.items()})


def lookup(client, state, request):
    """Bounded GET-only search; absence is evidence, never proof that no POST occurred."""
    init = _stamp(request['date']+'T'+request['time'][0]+':00+00:00')
    floor = min(init, _stamp(state['submission_started_at']))-dt.timedelta(minutes=5)
    page = client.get_jobs(limit=100, sortby='-created')
    previous = {a['request_id'] for a in state.get('previous_attempts', [])}
    seen, matches = set(), []
    began, last = time.monotonic(), None
    for number in range(10):
        jobs = page.json['jobs']
        stamps = [_stamp(j['created']) for j in jobs]
        if any(a < b for a, b in zip(stamps, stamps[1:])) or (last and stamps and last < stamps[0]):
            raise recovery.RecoveryBlocked('ADS submission lookup order is not verifiable')
        for job, stamp in zip(jobs, stamps):
            if time.monotonic()-began > 60:
                raise recovery.RecoveryBlocked('ADS submission lookup exceeded its time bound')
            if stamp < floor:
                break
            rid = job['jobID']
            if rid in seen:
                raise recovery.RecoveryBlocked('ADS submission lookup repeated an order')
            seen.add(rid)
            if not isinstance(job.get('processID'), str) or not job['processID']:
                raise recovery.RecoveryBlocked('ADS submission lookup lacks dataset identity')
            if job.get('processID') != DATASET:
                continue
            remote = client.get_remote(rid).json
            if remote.get('processID', DATASET) != DATASET:
                raise recovery.RecoveryBlocked('ADS submission lookup dataset changed')
            ids = recovery._remote_request(remote)
            if not ids:
                raise recovery.RecoveryBlocked('ADS submission lookup lacks request identity')
            if remote['status'] not in ('accepted', 'running', 'successful') and (
                    rid in previous or stamp < _stamp(state['submission_started_at'])-dt.timedelta(minutes=5)):
                continue
            if _identity(ids) == _identity(request) and remote['status'] not in ('deleted', 'dismissed'):
                matches.append({'request_id': rid, 'status': remote['status'],
                                'request_sha256': signature(ids), 'created': remote.get('created', job['created'])})
        reached = not jobs or stamps[-1] < floor
        if not reached:
            page = page.next
            reached = page is None
        if reached:
            return {'checked_at': now(), 'since': floor.isoformat(), 'pages': number+1,
                    'inspected_order_ids': sorted(seen), 'matches': matches,
                    'coverage_complete': True, 'absence_is_not_proof': True}
        last = stamps[-1]
    raise recovery.RecoveryBlocked('ADS submission lookup exceeded its page bound')


def read_replacement(root: Path, path: Path):
    entry = json.loads(path.read_text())
    folder = root/entry['folder']; archive = root/entry['archive']
    if (not folder.resolve().is_relative_to(root.resolve())
            or not archive.resolve().is_relative_to((folder/'uncertain').resolve())
            or path.name != entry['request_sha256']+'.json'
            or type(entry['reserved_retry_count']) is not int or entry['reserved_retry_count'] < 1):
        raise recovery.RecoveryBlocked('Submission replacement receipt identity changed')
    for name, key in (('state.json', 'state_sha256'), ('request.json', 'request_file_sha256'),
                      ('authorization.json', 'authorization_sha256')):
        if digest(archive/name) != entry[key]:
            raise recovery.RecoveryBlocked('Submission replacement original evidence changed')
    saved = json.loads((archive/'state.json').read_text())
    request = json.loads((archive/'request.json').read_text())
    if (signature(request) != entry['request_sha256'] or saved['request_sha256'] != entry['request_sha256']
            or saved['phase'] not in UNCERTAIN or _stamp(entry['window_start_at']) > _stamp(saved['submission_started_at'])
            or entry['reserved_retry_count'] != recovery.used_count(saved)+1):
        raise recovery.RecoveryBlocked('Submission replacement evidence or window changed')
    return entry


def _context(config, date, cycle, group, part):
    request_for(date, cycle, group)
    init = config.output_dir/SOURCE/date/f'{cycle:02d}z'; parent = init/'.cams'/group
    folder = parent
    if part is not None:
        from climate_download.cams.partition import validate_plan
        if group != 'hourly' or len(part) != 16 or any(c not in '0123456789abcdef' for c in part):
            raise ValueError('Invalid original CAMS part')
        plan = json.loads((parent/'partition.json').read_text()); validate_plan(plan, date, cycle)
        saved = json.loads((parent/'state.json').read_text())
        if digest(parent/'partition.json') != saved['partition_sha256']:
            raise recovery.RecoveryBlocked('Original CAMS partition changed')
        expected = next(p['request'] for p in plan['parts'] if p['id'] == part)
        folder = parent/'parts'/part
    else:
        expected = request_for(date, cycle, group)
    state = json.loads((folder/'state.json').read_text()); request = json.loads((folder/'request.json').read_text())
    if (signature(request) != signature(expected) or state['request_sha256'] != signature(expected)
            or any(state[k] != v for k, v in (('date', date), ('cycle', cycle), ('group', group),
                       ('dataset', DATASET), ('account', config.account_for(cycle).name)))):
        raise recovery.RecoveryBlocked('Original CAMS submission request/account identity changed')
    request_variables(request, date, cycle, group)
    if dt.datetime.fromisoformat(state['submission_started_at']).tzinfo is None:
        raise recovery.RecoveryBlocked('Original submission requires an aware timestamp')
    return init, parent, folder, state, request


def _reopen(parent, folder, state, proof):
    state.update(phase='submitted', submission_recovery=proof)
    state.pop('error', None)
    save(folder/'state.json', state)
    _repair_parent(parent, folder, proof)


def _repair_parent(parent, folder, proof):
    if parent != folder:
        saved = json.loads((parent/'state.json').read_text())
        if saved['phase'] not in UNCERTAIN or saved.get('failed_part', saved.get('active_part')) != folder.name:
            return
        saved.update(phase='partitioned', active_part=folder.name, submission_recovery=proof)
        for key in ('error', 'failed_part', 'request_id'):
            saved.pop(key, None)
        save(parent/'state.json', saved)


def recover_submission(config, date, cycle, group, *, part=None, authorization=None, client=None, secret=''):
    """One explicit operator action under normal locks; normal cron never grants POST authority."""
    key = f'{date}{cycle:02d}'
    if key < config.first_init:
        raise ValueError('Submission predates configured recovery floor')
    locks = config.output_dir/SOURCE/'.locks'; locks.mkdir(parents=True, exist_ok=True)
    with (locks/f'account-{config.account_for(cycle).name}.lock').open('a') as account_lock, \
            (locks/f'init-{key}.lock').open('a') as init_lock:
        try:
            fcntl.flock(account_lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
            fcntl.flock(init_lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            return {'status': 'locked', 'init': key}
        init, parent, folder, state, request = _context(config, date, cycle, group, part)
        if state['phase'] not in UNCERTAIN:
            if state.get('request_id'):
                proof = state.get('submission_recovery')
                pointer = state.get('submission_replacement')
                if not proof and pointer:
                    ledger = init/'.cams'/pointer['ledger']
                    if digest(ledger) != pointer['sha256']:
                        raise recovery.RecoveryBlocked('Submission replacement receipt changed')
                    entry = read_replacement(init/'.cams', ledger)
                    proof = {'method': 'explicit_single_replacement', 'ledger_sha256': digest(ledger),
                             'request_id': state['request_id'], 'directory': str((init/'.cams'/entry['archive']).relative_to(folder))}
                if proof:
                    _repair_parent(parent, folder, proof)
                return {'status': 'already_resumed', 'init': key, 'request_id': state['request_id']}
            raise recovery.RecoveryBlocked('CAMS submission is not an unresolved POST')
        if recovery.has_raw_evidence(folder):
            raise recovery.RecoveryBlocked('Unknown submission has raw bytes requiring their original order')
        archive = folder/'uncertain'/signature([state['submission_started_at'], signature(request)])[:16]
        for name in ('state.json', 'request.json'):
            recovery._archive_exact(archive/name, (folder/name).read_bytes())
        if parent != folder:
            for name in ('state.json', 'request.json', 'partition.json'):
                recovery._archive_exact(archive/('parent-'+name), (parent/name).read_bytes())
        ledger = init/'.cams/submission_replacements'/(signature(request)+'.json')
        spent = read_replacement(init/'.cams', ledger) if ledger.is_file() else None
        if client is None:
            client, secret = create_client(config.account_for(cycle))

        def inspect():
            result = lookup(client, state, request)
            path = archive/('lookup-'+signature(result)[:16]+'.json')
            recovery._archive_exact(path, (json.dumps(result, indent=2)+'\n').encode())
            if len(result['matches']) > 1:
                raise recovery.RecoveryBlocked('Multiple matching ADS orders need inspection; no replacement')
            return result, path

        def adopt(result, proof_path):
            match = result['matches'][0]
            state.update(request_id=match['request_id'], recovered_at=now())
            if spent:
                state['retry_count'] = max(recovery.used_count(state), spent['reserved_retry_count'])
            proof = {'method': 'original_order_found', 'directory': str(archive.relative_to(folder)),
                     'lookup_sha256': digest(proof_path), 'request_id': match['request_id']}
            _reopen(parent, folder, state, proof)
            return {'status': 'original_recovered', 'init': key, 'request_id': match['request_id'],
                    'orders_submitted': 0, 'lookup': result}

        result, proof_path = inspect()
        if result['matches']:
            return adopt(result, proof_path)
        if spent or authorization is None:
            return {'status': 'replacement_spent' if spent else 'not_found', 'init': key,
                    'orders_submitted': 0, 'lookup': result}
        if (authorization.get('schema_version') != 1
                or authorization.get('state_sha256') != digest(archive/'state.json')
                or authorization.get('request_sha256') != signature(request)
                or type(authorization.get('max_extra_submissions')) is not int
                or authorization['max_extra_submissions'] != 1 or not authorization.get('reason')
                or any(dt.datetime.fromisoformat(authorization[k]).tzinfo is None for k in ('authorized_at', 'expires_at'))
                or _stamp(authorization['authorized_at']) > _stamp(now())
                or _stamp(authorization['expires_at']) > _stamp(state['submission_started_at'])+dt.timedelta(days=1)):
            raise recovery.RecoveryBlocked('Explicit single-submission authorization differs from the original')
        from climate_download.cams.deleted import account_ready
        from climate_download.cams.worker import _submit_once, newer_live_due

        def budget():
            policy = state.get('mars_retry_policy', {})
            if policy.get('schema_version') != 3 or config.mars_count_retry_window_seconds is None:
                raise recovery.RecoveryBlocked('Submission replacement requires the shared bounded daily policy')
            recovery._delays(policy)
            starts = [_stamp(state['submission_started_at'])]
            try:
                starts.append(recovery._first_failure_at(config, folder, state))
            except recovery.RecoveryBlocked as error:
                if str(error) != 'CAMS retry window has no retained verified failure':
                    raise
            start = min(starts); at = _stamp(now())
            deadline = min(_stamp(authorization['expires_at']), start+dt.timedelta(
                seconds=min(policy['window_seconds'], config.mars_count_retry_window_seconds)))
            if (at > deadline or recovery.init_used_count(config, folder, state) >=
                    min(config.mars_count_retry_limit, policy['per_init_limit'])):
                raise recovery.RecoveryBlocked('Submission replacement window or shared budget exhausted')
            return start

        budget()
        if newer_live_due(config, key, cycle) or not account_ready(client):
            return {'status': 'waiting_for_account_or_realtime', 'init': key, 'orders_submitted': 0}
        state['preflight'] = preflight(client, request, group)
        result, proof_path = inspect()
        if result['matches']:
            return adopt(result, proof_path)
        if newer_live_due(config, key, cycle) or not account_ready(client):
            return {'status': 'waiting_for_account_or_realtime', 'init': key, 'orders_submitted': 0}
        start = budget()
        recovery._archive_exact(archive/'authorization.json', (json.dumps(authorization, indent=2)+'\n').encode())
        entry = {'folder': str(folder.relative_to(init/'.cams')), 'archive': str(archive.relative_to(init/'.cams')),
                 'request_sha256': signature(request), 'state_sha256': digest(archive/'state.json'),
                 'request_file_sha256': digest(archive/'request.json'), 'authorization_sha256': digest(archive/'authorization.json'),
                 'window_start_at': start.isoformat(), 'reserved_retry_count': recovery.used_count(state)+1,
                 'reserved_at': now(), 'lookup_sha256': digest(proof_path)}
        recovery._archive_exact(ledger, (json.dumps(entry, indent=2)+'\n').encode())
        state['retry_count'] = entry['reserved_retry_count']
        state['submission_replacement'] = {'ledger': str(ledger.relative_to(init/'.cams')), 'sha256': digest(ledger)}
        save(folder/'state.json', state)
        _submit_once(folder, state, request, client, secret)
        proof = {'method': 'explicit_single_replacement', 'directory': str(archive.relative_to(folder)),
                 'ledger_sha256': digest(ledger), 'request_id': state['request_id']}
        _reopen(parent, folder, state, proof)
        return {'status': 'replacement_submitted', 'init': key, 'request_id': state['request_id'],
                'orders_submitted': 1, 'lookup': result}
