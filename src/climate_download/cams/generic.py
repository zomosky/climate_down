"""Opt-in recovery of confirmed MARS failures with no detailed server diagnostic."""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
import re

from climate_download.cams import recovery
from climate_download.cams.state import digest, safe_error, save, signature

KIND = 'mars_runtime_error'
PROOF = 'generic_mars_enrollment'


def classify(error):
    message = str(error)
    status = getattr(getattr(error, 'response', None), 'status_code', None)
    detail = re.search(r'Full error message:\s*(.*)\Z', message, re.I | re.S)
    if (status not in (None, 400) or not re.search(r'\bMarsRuntimeError\b', message)
            or 'MARS has returned an error' not in message
            or detail is None or detail[1].strip()
            or re.search(r'^\s*(?:401|403|404|422)\b|\blicen[cs]e\b|\bforbidden\b|\bunauthori[sz]ed\b|'
                         r'\brestricted access\b|\binvalid (?:parameter|variable|request|selection)\b|'
                         r'\bunknown (?:parameter|variable)\b|\bLast error is\b', message, re.I)):
        return None
    return {'kind': KIND, 'detail_missing': True}


def read_enrollment(path):
    entry = json.loads(path.read_text()); archive = path.parent
    for name, key in (('state.json', 'state_sha256'), ('request.json', 'request_file_sha256'),
                      ('failure.json', 'failure_sha256')):
        if digest(archive/name) != entry[key]:
            raise recovery.RecoveryBlocked('Generic MARS enrollment original bytes changed')
    failure = json.loads((archive/'failure.json').read_text())
    if (entry['diagnostic'] != classify(RuntimeError(failure['error']['message']))
            or entry['diagnostic']['kind'] != KIND
            or any(entry[key] != failure[key] for key in ('request_id', 'request_sha256', 'account'))
            or signature(json.loads((archive/'request.json').read_text())) != entry['request_sha256']
            or entry['observed_at'] != failure['captured_at']
            or not (entry['remote_failed_verified'] and failure['local_request_verified']
                    and failure['remote_request_verified'])):
        raise recovery.RecoveryBlocked('Generic MARS enrollment identity or diagnostic changed')
    stamp = dt.datetime.fromisoformat(entry['observed_at'])
    verified = dt.datetime.fromisoformat(entry['verified_at'])
    if stamp.tzinfo is None or verified.tzinfo is None or stamp > verified:
        raise recovery.RecoveryBlocked('Generic MARS enrollment failure timestamp is invalid')
    return entry


def verify_proof(folder, proof, archive):
    pointer = proof.get(PROOF)
    if not pointer:
        return
    path = folder/pointer['directory']
    if (path.resolve().parent != archive.resolve() or path.name != 'generic_mars_enrollment.json'
            or digest(path) != pointer['sha256']):
        raise recovery.RecoveryBlocked('Generic MARS enrollment checksum or directory changed')
    entry = read_enrollment(path)
    if (entry['request_id'] != proof['request_id'] or entry['state_sha256'] != proof['state_sha256']
            or entry['failure_sha256'] != proof['failure_sha256']
            or entry['request_file_sha256'] != proof['request_sha256']):
        raise recovery.RecoveryBlocked('Generic MARS enrollment differs from the retained failure')


def enroll(config, folder, state, request, client, secret):
    """Reclassify a retained failure through a separate immutable receipt, never rewrite it."""
    if (not config.generic_mars_retry_enabled or state.get('phase') != 'remote_failed'
            or state.get('failure_diagnostic') or not state.get('failed_attempt')
            or not state.get('request_id') or recovery.has_raw_evidence(folder)
            or state.get('mars_retry_policy', {}).get('schema_version') != 3):
        return False
    recovery._delays(state['mars_retry_policy'])
    archive, failure = recovery._failure_evidence(folder, state, request)
    diagnostic = classify(RuntimeError(failure['error']['message']))
    if not diagnostic:
        return False
    if (not (failure['local_request_verified'] and failure['remote_request_verified'])
            or failure['request_id'] != state['request_id'] or failure['account'] != state['account']
            or failure['request_sha256'] != signature(request)):
        raise recovery.RecoveryBlocked('Original generic MARS failure identity is not verified')
    observed = dt.datetime.fromisoformat(failure['captured_at'])
    if observed.tzinfo is None:
        raise recovery.RecoveryBlocked('Original generic MARS failure has no UTC timestamp')
    window = min(state['mars_retry_policy']['window_seconds'], config.mars_count_retry_window_seconds)
    at = dt.datetime.fromisoformat(recovery.now())
    if at > observed+dt.timedelta(seconds=window):
        return False
    path = archive/'generic_mars_enrollment.json'
    try:
        reply = client.get_remote(state['request_id']).json
        if signature(recovery._remote_request(reply)) != signature(request):
            raise recovery.RecoveryBlocked('Original generic MARS request parameters changed')
        if reply['status'] in ('accepted', 'running', 'successful'):
            state.update(phase='submitted', retry_cancelled_reason='original_order_reactivated')
            save(folder/'state.json', state)
            return True
        if reply['status'] != 'failed':
            raise recovery.RecoveryBlocked('Original generic MARS job is not confirmed failed')
    except recovery.RecoveryBlocked:
        raise
    except Exception as error:
        state['generic_enrollment_error'] = safe_error(error, secret)
        save(folder/'state.json', state)
        return False
    if not path.is_file():
        entry = {'schema_version': 1, 'observed_at': failure['captured_at'], 'verified_at': recovery.now(),
                 'request_id': state['request_id'], 'request_sha256': signature(request),
                 'account': state['account'], 'diagnostic': diagnostic, 'remote_failed_verified': True,
                 'state_sha256': digest(archive/'state.json'), 'request_file_sha256': digest(archive/'request.json'),
                 'failure_sha256': digest(archive/'failure.json')}
        recovery._archive_exact(path, (json.dumps(entry, ensure_ascii=False, indent=2)+'\n').encode())
    read_enrollment(path)
    state['failed_attempt'][PROOF] = {'directory': str(path.relative_to(folder)), 'sha256': digest(path)}
    state['failure_diagnostic'] = diagnostic
    if recovery._allowed(config, state, folder):
        due = observed+dt.timedelta(seconds=recovery._delays(state['mars_retry_policy'])[recovery.used_count(state)])
        state.update(phase='retry_wait', retry_due_at=due.isoformat())
    save(folder/'state.json', state)
    return state['phase'] == 'retry_wait'
