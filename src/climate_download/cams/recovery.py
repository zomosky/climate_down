"""Narrow MARS -43 recovery with immutable failure evidence and a pinned budget."""
from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
import re

from climate_download.cams.catalogue import DATASET, SOURCE
from climate_download.cams.state import digest, now, safe_error, save, signature


class RecoveryBlocked(ValueError):
    """The failed request cannot safely be replaced automatically."""


# Local evidence may be incomplete after a crash. Isolate that init, rather
# than turning a corrupt receipt into a failure of the complete scheduled scan.
EVIDENCE_ERRORS = (ValueError, OSError, KeyError, TypeError, IndexError)


def has_raw_evidence(folder: Path) -> bool:
    return any((folder/name).exists() for name in
               ('download.grib', 'download.grib.part', 'download.grib.part.ranges'))


def classify_mars_failure(error: Exception) -> dict | None:
    message = str(error)
    if not re.search(r'\bMarsRuntimeError\b|\bmars\s*-\s*(?:ERROR|FATAL)\b', message, re.I):
        return None
    if not re.search(r'\bLast error is\s+-43\b', message, re.I):
        return None
    counts = re.search(r'\bExpected\s+(\d+),\s*got\s+(\d+)\b', message, re.I)
    if counts is None:
        return None
    expected, actual = map(int, counts.groups())
    if expected <= 0 or actual < 0 or expected == actual:
        return None
    return {'kind': 'mars_field_count_mismatch', 'mars_error_code': -43,
            'expected_fields': expected, 'actual_fields': actual}


def policy_for(config) -> dict:
    delays = getattr(config, 'mars_count_retry_delays_seconds', None)
    if delays:
        return {'schema_version': 2, 'max_retries': config.mars_count_retry_limit,
                'delays_seconds': list(delays), 'per_init_limit': config.mars_count_retry_limit,
                'captured_at': now()}
    return {'max_retries': getattr(config, 'mars_count_retry_limit', 0),
            'delay_seconds': getattr(config, 'mars_count_retry_delay_seconds', 1800), 'captured_at': now()}


def used_count(state) -> int:
    value = state.get('retry_count', 0)
    previous = state.get('previous_attempts', [])
    if type(value) is not int or value < 0 or not isinstance(previous, list):
        raise RecoveryBlocked('Invalid retained CAMS replacement count')
    return max(value, len(previous))


def init_used_count(config, folder, state) -> int:
    """Called under the account/init lock; parent and native pieces share one cap."""
    total = used_count(state)
    root = config.output_dir/SOURCE/state['date']/f"{state['cycle']:02d}z"/'.cams'
    paths = list(root.glob('*/state.json'))+list(root.glob('hourly/parts/*/state.json'))
    for path in paths:
        if path.resolve() != (folder/'state.json').resolve():
            total += used_count(json.loads(path.read_text()))
    return total


def _delays(policy) -> list[int]:
    if policy.get('schema_version', 1) == 1:
        if (type(policy.get('max_retries')) is not int or policy['max_retries'] not in (0, 1)
                or type(policy.get('delay_seconds')) is not int or not 300 <= policy['delay_seconds'] <= 21600):
            raise RecoveryBlocked('Invalid retained legacy CAMS retry policy')
        return [policy['delay_seconds']]*policy['max_retries']
    delays = policy.get('delays_seconds', [])
    limit = policy.get('max_retries')
    if (policy.get('schema_version') != 2 or type(limit) is not int or not 1 <= limit <= 3
            or not isinstance(delays, list) or len(delays) != limit
            or any(type(d) is not int or not 300 <= d <= 21600 for d in delays)
            or any(a >= b for a, b in zip(delays, delays[1:]))
            or policy.get('per_init_limit') != limit):
        raise RecoveryBlocked('Invalid retained staged CAMS retry policy')
    return delays


def _allowed(config, state, folder) -> bool:
    policy = state.get('mars_retry_policy', {})
    if not policy:
        return False
    delays = _delays(policy)
    limit = min(getattr(config, 'mars_count_retry_limit', 0), len(delays))
    if used_count(state) >= limit:
        return False
    if policy.get('schema_version') == 2:
        return (bool(getattr(config, 'mars_count_retry_delays_seconds', None))
                and init_used_count(config, folder, state) < min(limit, policy['per_init_limit']))
    return True


def _remote_request(reply) -> dict:
    return dict(reply.get('metadata', {}).get('request', {}).get('ids', {}))


def _archive_exact(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != content:
            raise RecoveryBlocked('Existing failed-request archive differs; inspect it before recovery')
        return
    with path.open('xb') as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())


def _failure_evidence(folder, state, request):
    previous = state['failed_attempt']
    directory = 'failures/'+signature(state['request_id'])[:16]
    if previous['directory'] != directory or previous['request_id'] != state['request_id']:
        raise RecoveryBlocked('Failed-request archive identity changed')
    archive = folder/directory
    for name, field in (('state.json', 'state_sha256'), ('request.json', 'request_sha256'),
                        ('failure.json', 'failure_sha256')):
        if not (archive/name).is_file() or digest(archive/name) != previous[field]:
            raise RecoveryBlocked('Failed-request archive checksum changed')
    if (folder/'request.json').read_bytes() != (archive/'request.json').read_bytes():
        raise RecoveryBlocked('Original ADS request bytes changed')
    if signature(json.loads((archive/'request.json').read_text())) != signature(request):
        raise RecoveryBlocked('Original ADS request parameters changed')
    snapshot = json.loads((archive/'state.json').read_text())
    if used_count(snapshot) != used_count(state):
        raise RecoveryBlocked('Retained CAMS retry count differs from its failure evidence')
    policy = state.get('mars_retry_policy', {})
    enrollment = state.get('staged_policy_enrollment')
    if policy.get('schema_version') == 2:
        if snapshot.get('mars_retry_policy', {}).get('schema_version') == 2:
            if snapshot['mars_retry_policy'] != policy or snapshot.get('staged_policy_enrollment') != enrollment:
                raise RecoveryBlocked('Retained staged CAMS policy changed')
        elif not enrollment:
            raise RecoveryBlocked('Legacy CAMS failure has no staged enrollment evidence')
        if enrollment:
            path = folder/enrollment['directory']
            if (not path.resolve().is_relative_to((folder/'failures').resolve())
                    or not path.is_file() or digest(path) != enrollment['sha256']
                    or json.loads(path.read_text())['policy'] != policy):
                raise RecoveryBlocked('Staged CAMS enrollment checksum or policy changed')
    failure = json.loads((archive/'failure.json').read_text())
    if state.get('phase') == 'retry_wait':
        due = dt.datetime.fromisoformat(failure['captured_at'])+dt.timedelta(seconds=_delays(policy)[used_count(state)])
        if dt.datetime.fromisoformat(state['retry_due_at']) != due:
            raise RecoveryBlocked('Retained CAMS retry deadline changed')
    for attempt in state.get('previous_attempts', []):
        proof = attempt.get('evidence')
        if proof:
            saved = folder/proof['directory']
            if saved.resolve().parent != (folder/'failures').resolve():
                raise RecoveryBlocked('Previous CAMS archive is outside the retained failure tree')
            for name, key in (('state.json','state_sha256'), ('request.json','request_sha256'),
                              ('failure.json','failure_sha256')):
                if not (saved/name).is_file() or digest(saved/name) != proof[key]:
                    raise RecoveryBlocked('Previous failed-request archive checksum changed')
    return archive, failure


def enroll_staged_policy(config, folder, state, request) -> bool:
    """One explicit v2 enrollment; retain old policy, counts, and failure bytes."""
    if (not getattr(config, 'mars_count_retry_delays_seconds', None)
            or state.get('mars_retry_policy', {}).get('schema_version') == 2
            or state.get('phase') not in ('remote_failed', 'retry_wait')
            or state.get('failure_diagnostic', {}).get('kind') != 'mars_field_count_mismatch'
            or not state.get('mars_retry_policy') or has_raw_evidence(folder)):
        return False
    date = request['date'].replace('-', '')
    cycle = int(request['time'][0].split(':')[0])
    if (state.get('date') != date or state.get('cycle') != cycle or state.get('dataset') != DATASET
            or state.get('account') != config.account_for(cycle).name
            or state.get('request_sha256') != signature(request)):
        raise RecoveryBlocked('Retained CAMS failure request/account/init identity changed')
    _delays(state['mars_retry_policy'])
    if init_used_count(config, folder, state) >= config.mars_count_retry_limit:
        return False
    archive, failure = _failure_evidence(folder, state, request)
    if not (failure.get('local_request_verified') and failure.get('remote_request_verified')
            and failure.get('diagnostic', {}).get('kind') == 'mars_field_count_mismatch'):
        raise RecoveryBlocked('Retained MARS failure is not verified for staged enrollment')
    path = archive/'staged_policy_enrollment.json'
    if path.is_file():
        entry = json.loads(path.read_text())
        if (entry['request_id'] != state['request_id'] or entry['previous_policy'] != state['mars_retry_policy']
                or entry['used_retries'] != used_count(state)
                or entry['original_state_sha256'] != state['failed_attempt']['state_sha256']):
            raise RecoveryBlocked('Staged CAMS policy enrollment evidence changed')
    else:
        entry = {'enrolled_at': now(), 'request_id': state['request_id'],
                 'previous_policy': state['mars_retry_policy'], 'used_retries': used_count(state),
                 'original_state_sha256': state['failed_attempt']['state_sha256'],
                 'policy': policy_for(config), 'reason': 'explicit_staged_recovery_configuration'}
        _archive_exact(path, (json.dumps(entry, ensure_ascii=False, indent=2)+'\n').encode())
    delays = _delays(entry['policy'])
    state.update(mars_retry_policy=entry['policy'], retry_count=entry['used_retries'], phase='retry_wait',
                 staged_policy_enrollment={'directory': str(path.relative_to(folder)),
                                           'sha256': digest(path),
                                           'previous_max_retries': entry['previous_policy']['max_retries']},
                 retry_due_at=(dt.datetime.fromisoformat(failure['captured_at'])
                               + dt.timedelta(seconds=delays[entry['used_retries']])).isoformat())
    save(folder/'state.json', state)
    return True


def record_failure(config, folder, state, request, reply, error, secret) -> bool:
    """Archive every terminal failure; enroll only a newly authorized count error."""
    diagnostic = classify_mars_failure(error)
    state.update(phase='remote_failed', error=safe_error(error, secret))
    state.pop('failure_diagnostic', None)
    if diagnostic:
        state['failure_diagnostic'] = diagnostic
    path = folder/'state.json'
    save(path, state)
    directory = 'failures/'+signature(state['request_id'])[:16]
    archive = folder/directory
    _archive_exact(archive/'state.json', path.read_bytes())
    request_file = folder/'request.json'
    request_verified = False
    if request_file.is_file():
        content = request_file.read_bytes()
        _archive_exact(archive/'request.json', content)
        request_verified = signature(json.loads(content)) == signature(request) == state['request_sha256']
    remote_verified = signature(_remote_request(reply)) == signature(request)
    failure = {'captured_at': now(), 'request_id': state['request_id'],
               'request_sha256': state['request_sha256'], 'account': state['account'],
               'diagnostic': diagnostic, 'remote_request_verified': remote_verified,
               'local_request_verified': request_verified, 'error': safe_error(error, secret, max_chars=None)}
    _archive_exact(archive/'failure.json', (json.dumps(failure, ensure_ascii=False, indent=2)+'\n').encode())
    state['failed_attempt'] = {'directory': directory, 'request_id': state['request_id'],
                               'state_sha256': digest(archive/'state.json'),
                               'request_sha256': digest(archive/'request.json') if request_file.is_file() else None,
                               'failure_sha256': digest(archive/'failure.json')}
    if diagnostic and request_verified and remote_verified:
        enroll_staged_policy(config, folder, state, request)
    eligible = (reply['status'] == 'failed' and diagnostic is not None and _allowed(config, state, folder)
                and request_verified and remote_verified
                and not has_raw_evidence(folder))
    if eligible:
        delay = _delays(state['mars_retry_policy'])[used_count(state)]
        due = dt.datetime.fromisoformat(failure['captured_at']) + dt.timedelta(seconds=delay)
        state.update(phase='retry_wait', retry_due_at=due.isoformat())
    save(path, state)
    return eligible


def retry_decision(config, folder, state, request, client, secret) -> str | None:
    """Only return 'retry' after all evidence, account and original-order checks."""
    if not _allowed(config, state, folder):
        raise RecoveryBlocked('MARS retry policy is disabled or its request/init budget is exhausted')
    _failure_evidence(folder, state, request)
    if dt.datetime.fromisoformat(now()) < dt.datetime.fromisoformat(state['retry_due_at']):
        return None
    if has_raw_evidence(folder):
        raise RecoveryBlocked('Existing raw bytes must continue using their original order')
    try:
        reply = client.get_remote(state['request_id']).json
        if signature(_remote_request(reply)) != signature(request):
            raise RecoveryBlocked('Original remote request parameters changed')
        if reply['status'] == 'successful':
            state.update(phase='submitted', retry_cancelled_reason='original_order_became_successful')
            save(folder/'state.json', state)
            return 'original'
        if reply['status'] in ('accepted', 'running'):
            state.update(phase='submitted', retry_cancelled_reason='original_order_reactivated')
            save(folder/'state.json', state)
            return 'original'
        if reply['status'] != 'failed':
            state.update(retry_wait_reason='original_order_not_failed', last_check=now())
            save(folder/'state.json', state)
            return None
        if client.get_jobs(limit=100, status=['accepted', 'running']).json['jobs']:
            state.update(retry_wait_reason='another_active_order', last_check=now())
            save(folder/'state.json', state)
            return None
    except RecoveryBlocked:
        raise
    except Exception as error:
        if getattr(getattr(error, 'response', None), 'status_code', None) in (400, 401, 403, 404, 422):
            raise RecoveryBlocked('Original ADS order cannot be verified before retry') from None
        state.update(last_check=now(), retry_wait_reason='metadata_deferred', error=safe_error(error, secret))
        save(folder/'state.json', state)
        return None
    return 'retry'
