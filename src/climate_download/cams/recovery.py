"""Narrow MARS -43 recovery with immutable failure evidence and a pinned budget."""
from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
import re

from climate_download.cams.state import digest, now, safe_error, save, signature


class RecoveryBlocked(ValueError):
    """The failed request cannot safely be replaced automatically."""


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
    return {'max_retries': getattr(config, 'mars_count_retry_limit', 0),
            'delay_seconds': getattr(config, 'mars_count_retry_delay_seconds', 1800), 'captured_at': now()}


def _allowed(config, state) -> bool:
    policy = state.get('mars_retry_policy', {})
    return (getattr(config, 'mars_count_retry_limit', 0) == 1 and policy.get('max_retries') == 1
            and 300 <= policy.get('delay_seconds', 0) <= 21600 and state.get('retry_count', 0) == 0)


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
    eligible = (reply['status'] == 'failed' and diagnostic is not None and _allowed(config, state)
                and request_verified and remote_verified
                and not has_raw_evidence(folder))
    if eligible:
        due = dt.datetime.fromisoformat(now()) + dt.timedelta(seconds=state['mars_retry_policy']['delay_seconds'])
        state.update(phase='retry_wait', retry_due_at=due.isoformat())
    save(path, state)
    return eligible


def retry_decision(config, folder, state, request, client, secret) -> str | None:
    """Only return 'retry' after all evidence, account and original-order checks."""
    if not _allowed(config, state):
        raise RecoveryBlocked('MARS retry policy is disabled or its one-attempt budget is exhausted')
    if dt.datetime.fromisoformat(now()) < dt.datetime.fromisoformat(state['retry_due_at']):
        return None
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
