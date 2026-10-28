"""One explicitly bounded ADS native cube per order, with exact union coverage.

This avoids asking ADS to execute a heterogeneous multi-MARS request. It neither
changes the scientific contract nor retries a failed bundle under a new name.
"""
from __future__ import annotations

from collections import defaultdict
import json
import os
from pathlib import Path
import shutil

from climate_download.cams.catalogue import DATASET, GROUPS, request_for, request_variables
from climate_download.cams.client import NotAvailableYet
from climate_download.cams.grib import validate
from climate_download.cams.state import digest, now, save, signature


def make_plan(date: str, cycle: int, constraints: list[dict]) -> dict:
    original = request_for(date, cycle, 'hourly')
    remaining = {v.api_name: set(v.leads) for v in GROUPS['hourly']}
    # Canonical row/value ordering makes the selected rectangles independent of
    # response ordering; overlapping advertised rows cannot duplicate a field.
    normalized = [{k: sorted(set(v)) for k, v in row.items()} for row in constraints]
    rows = sorted(normalized, key=signature)
    parts = []
    for row in rows:
        if (original['time'][0] not in row.get('time', []) or 'forecast' not in row.get('type', [])
                or row.get('pressure_level') or row.get('model_level')
                or not any(span.split('/')[0] <= original['date'] <= span.split('/')[-1]
                           for span in row.get('date', []))):
            continue
        available = {int(h) for h in row.get('leadtime_hour', [])}
        rectangles = defaultdict(list)
        for variable in original['variable']:
            if variable not in row.get('variable', []):
                continue
            leads = tuple(sorted(remaining[variable] & available))
            if leads:
                rectangles[leads].append(variable)
                remaining[variable].difference_update(leads)
        for leads, variables in rectangles.items():
            request = {**original, 'variable': variables, 'leadtime_hour': [str(h) for h in leads]}
            parts.append({'id': signature(request)[:16], 'request': request,
                          'constraint_signature': signature(row)})
    if any(remaining.values()):
        raise NotAvailableYet('Full native CAMS fields are not available for an exact request partition')
    plan = {'schema_version': 1, 'strategy': 'native_constraint_cubes', 'date': date, 'cycle': cycle,
            'group': 'hourly', 'request_sha256': signature(original),
            'parts': sorted(parts, key=lambda p: p['id'])}
    validate_plan(plan, date, cycle)
    return plan


def validate_plan(plan: dict, date: str, cycle: int) -> None:
    original = request_for(date, cycle, 'hourly')
    if (set(plan) != {'schema_version', 'strategy', 'date', 'cycle', 'group', 'request_sha256', 'parts'}
            or plan['schema_version'] != 1 or plan['strategy'] != 'native_constraint_cubes'
            or (plan['date'], plan['cycle'], plan['group']) != (date, cycle, 'hourly')
            or plan['request_sha256'] != signature(original)):
        raise ValueError('Pinned CAMS partition identity differs')
    expected = {(v.api_name, h) for v in GROUPS['hourly'] for h in v.leads}
    seen, ids = set(), set()
    for part in plan['parts']:
        request_variables(part['request'], date, cycle, 'hourly')
        if (set(part) != {'id', 'request', 'constraint_signature'} or part['id'] in ids
                or part['id'] != signature(part['request'])[:16]
                or not isinstance(part['constraint_signature'], str) or len(part['constraint_signature']) != 64):
            raise ValueError('Pinned CAMS partition part differs')
        identities = {(v, int(h)) for v in part['request']['variable'] for h in part['request']['leadtime_hour']}
        if seen & identities:
            raise ValueError('CAMS request partition overlaps native identities')
        seen.update(identities)
        ids.add(part['id'])
    if seen != expected:
        raise ValueError('CAMS request partition does not cover every native identity')


def tick(config, folder: Path, date: str, cycle: int, client, secret: str) -> bool:
    from climate_download.cams.worker import BLOCKED, BlockedRequestError, group_tick, newer_live_due
    original = request_for(date, cycle, 'hourly')
    folder.mkdir(parents=True, exist_ok=True)
    state_path, plan_path = folder/'state.json', folder/'partition.json'
    state = json.loads(state_path.read_text()) if state_path.is_file() else {
        'date': date, 'cycle': cycle, 'group': 'hourly', 'account': config.account_for(cycle).name,
        'dataset': DATASET, 'phase': 'prepared', 'created_at': now(), 'request_sha256': signature(original)}
    if (state['request_sha256'] != signature(original) or state['account'] != config.account_for(cycle).name
            or state['dataset'] != DATASET):
        raise ValueError('Pinned CAMS partition request/account differs')
    if not state.get('partitioned_request') and (state.get('request_id') or state['phase'] in BLOCKED):
        raise BlockedRequestError('An existing CAMS order cannot be adopted by a different request strategy')
    if (state['phase'] == 'remote_failed' and state.get('failed_part')
            and getattr(config, 'mars_count_retry_delays_seconds', None)):
        from climate_download.cams import recovery
        try:
            plan = json.loads(plan_path.read_text())
            validate_plan(plan, date, cycle)
            if digest(plan_path) != state['partition_sha256']:
                raise ValueError('Pinned failed CAMS partition plan changed')
            part = next(p for p in plan['parts'] if p['id'] == state['failed_part'])
            child_folder = folder/'parts'/part['id']
            child = json.loads((child_folder/'state.json').read_text())
            enrolled = recovery.enroll_staged_policy(config, child_folder, child, part['request'])
        except (ValueError, OSError, KeyError, StopIteration):
            raise BlockedRequestError('Retained CAMS piece evidence requires inspection') from None
        if enrolled:
            state.update(phase='retry_wait', retry_due_at=child['retry_due_at'])
            save(state_path, state)
    if state['phase'] in BLOCKED:
        raise BlockedRequestError('CAMS partition retains a failed or uncertain order; inspect its evidence')
    if not plan_path.is_file():
        if state.get('partition_sha256'):
            raise ValueError('Pinned CAMS partition plan is missing')
        from climate_download.cams.recovery import has_raw_evidence
        if has_raw_evidence(folder):
            raise ValueError('Existing raw bytes require their original CAMS request')
        try:
            plan = make_plan(date, cycle, client.get_collection(DATASET).constraints)
        except Exception as error:
            from climate_download.cams.state import safe_error
            status = getattr(getattr(error, 'response', None), 'status_code', None)
            rejected = status in (400, 401, 403, 404, 422) or (isinstance(error, ValueError)
                                                          and not isinstance(error, NotAvailableYet))
            state.update(phase='preflight_rejected' if rejected else 'preflight_deferred',
                         partitioned_request=True, last_check=now(), error=safe_error(error, secret))
            save(state_path, state)
            if rejected:
                raise BlockedRequestError('Native CAMS partition preflight rejected; evidence retained') from None
            return False
        save(plan_path, plan)
    plan = json.loads(plan_path.read_text())
    validate_plan(plan, date, cycle)
    if state.get('partition_sha256') and digest(plan_path) != state['partition_sha256']:
        raise ValueError('Pinned CAMS partition checksum changed')
    if state['phase'] == 'complete':
        target = folder/'download.grib'
        if not target.is_file() or digest(target) != state['sha256']:
            raise ValueError('Completed CAMS partition raw checksum mismatch')
        return True
    state.update(partitioned_request=True, partition_sha256=digest(plan_path), phase='partitioned')
    save(state_path, state)
    if not (folder/'request.json').exists():
        save(folder/'request.json', original)
    elif signature(json.loads((folder/'request.json').read_text())) != signature(original):
        raise ValueError('Original aggregate CAMS request changed')
    # A staged policy authorizes bounded replacements of a failed piece only;
    # legacy configuration keeps its previous no-piece-retry behavior.
    piece_config = (config if getattr(config, 'mars_count_retry_delays_seconds', None)
                    else config.model_copy(update={'mars_count_retry_limit': 0}))
    children = []
    pending = False
    for part in plan['parts']:
        priority = newer_live_due(config, f'{date}{cycle:02d}', cycle)
        if priority:
            pending = True
            state.update(phase='partitioned', priority_init=priority, last_check=now())
            break
        child_folder = folder/'parts'/part['id']
        try:
            complete = group_tick(piece_config, child_folder, date, cycle, 'hourly', client, secret,
                                  request=part['request'])
        except BlockedRequestError:
            child = json.loads((child_folder/'state.json').read_text())
            state.update(phase=child['phase'], failed_part=part['id'], last_check=now(),
                         error=child.get('error'), request_id=child.get('request_id'))
            save(state_path, state)
            raise
        child = json.loads((child_folder/'state.json').read_text())
        children.append(child)
        if not complete:
            pending = True
            state.update(phase=child['phase'], active_part=part['id'], last_check=now())
            for key in ('retry_due_at', 'retry_wait_reason'):
                if key in child:
                    state[key] = child[key]
            if child.get('request_id'):
                state['request_id'] = child['request_id']
            break
    # An already adopted/downloaded later piece still contributes its bytes and
    # original transfer start while an earlier piece is pending.
    children = [json.loads(path.read_text()) for part in plan['parts']
                if (path := folder/'parts'/part['id']/'state.json').is_file()]
    state['parts_complete'] = sum(c['phase'] == 'complete' for c in children)
    state['parts_total'] = len(plan['parts'])
    state['request_ids'] = [c['request_id'] for c in children if c.get('request_id')]
    starts = [c['first_download_started_at'] for c in children if c.get('first_download_started_at')]
    if starts:
        state['first_download_started_at'] = min(starts)
    state['partial_bytes'] = sum(c.get('bytes', c.get('partial_bytes', 0)) for c in children)
    state['expected_bytes'] = sum(c.get('bytes', c.get('expected_bytes', 0)) for c in children)
    save(state_path, state)
    if pending:
        return False
    paths = [folder/'parts'/p['id']/'download.grib' for p in plan['parts']]
    combined = folder/'download.grib.part'
    with combined.open('wb') as stream:
        for path in paths:
            with path.open('rb') as source:
                shutil.copyfileobj(source, stream, length=8 << 20)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        validation = validate([combined], date, cycle, GROUPS['hourly'])
    except Exception as error:
        from climate_download.cams.state import safe_error
        state.update(phase='validation_failed', error=safe_error(error, secret))
        save(state_path, state)
        raise BlockedRequestError('Combined CAMS partition failed strict native validation') from None
    save(folder/'validation.json', validation)
    combined.replace(folder/'download.grib')
    state.update(phase='complete', completed_at=now(), download_finished_at=now(),
                 sha256=digest(folder/'download.grib'), bytes=(folder/'download.grib').stat().st_size,
                 messages=validation['messages'])
    state.pop('error', None)
    save(state_path, state)
    return True
