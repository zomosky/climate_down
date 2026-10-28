"""Finite backwards weekly CAMS queue using the production request/locking code.

The host supervisor accepts each whole week before opening the previous week.
This entry point performs at most one bounded init tick, never a failed-job retry.
"""
from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import json
from pathlib import Path
import shutil

from climate_download.cams.catalogue import DATASET, EXPECTED_MESSAGES, SOURCE, VARIABLES
from climate_download.cams.config import CamsConfig, load_config
from climate_download.cams.state import digest, now, safe_error, save, signature
from climate_download.cams.worker import BLOCKED, BlockedRequestError, pending_inits, run_init


def make_plan(start: str, end: str, *, authorized_at: str) -> dict:
    first, last = (dt.datetime.strptime(value, '%Y%m%d').date() for value in (start, end))
    if first > last or not authorized_at or dt.datetime.fromisoformat(authorized_at.replace('Z', '+00:00')).tzinfo is None:
        raise ValueError('An explicit bounded range and dated authorization are required')
    weeks = []
    while last >= first:
        beginning = max(first, last - dt.timedelta(days=6))
        dates = [(beginning + dt.timedelta(days=n)).strftime('%Y%m%d')
                 for n in range((last - beginning).days + 1)]
        weeks.append({'id': dates[0] + '-' + dates[-1], 'start': dates[0], 'end': dates[-1],
                      'inits': [d + f'{c:02d}' for d in dates for c in (0, 12)]})
        last = beginning - dt.timedelta(days=1)
    return {'schema_version': 1, 'source': SOURCE, 'dataset': DATASET,
            'start': start, 'end': end, 'authorized_at': authorized_at,
            'variables': len(VARIABLES), 'messages_per_init': EXPECTED_MESSAGES,
            'inits_total': sum(len(w['inits']) for w in weeks), 'weeks': weeks,
            'direction': 'backwards', 'realtime_priority': True,
            'failed_request_resubmission': False, 'acceptance_required_before_previous_week': True}


def load_plan(path: Path, expected_sha256: str | None = None) -> dict:
    if expected_sha256 is not None and digest(path) != expected_sha256:
        raise ValueError('Pinned weekly plan checksum changed')
    plan = json.loads(path.read_text())
    if plan != make_plan(plan['start'], plan['end'], authorized_at=plan['authorized_at']):
        raise ValueError('Weekly plan identity or scientific contract changed')
    return plan


def active_week(root: Path, plan: dict) -> dict | None:
    for week in plan['weeks']:
        folder = root/'weeks'/week['id']
        path = folder/'assessment.json'
        if not path.exists():
            return week
        receipt = json.loads(path.read_text())
        if (receipt.get('passed') is not True or receipt.get('week') != week
                or receipt.get('plan_signature') != signature(plan)
                or [c.get('init') for c in receipt.get('cases', [])] != week['inits']):
            raise ValueError('Weekly assessment identity or coverage differs')
        for case in receipt['cases']:
            if case.get('passed') is not True:
                raise ValueError('Weekly assessment contains a failed case')
            for stage in ('raw', 'http'):
                evidence = folder/'acceptance'/case['init']/(stage+'_acceptance.json')
                if not evidence.is_file() or digest(evidence) != case.get(stage+'_sha256'):
                    raise ValueError('Weekly assessment evidence changed or is missing')
    return None


def blocked_groups(folder: Path, key: str) -> list[dict]:
    rows = []
    for path in sorted((folder/'.cams').glob('*/state.json')):
        state = json.loads(path.read_text())
        if state.get('phase') in BLOCKED:
            rows.append({'init': key, 'group': path.parent.name, 'phase': state['phase'],
                         'request_id': state.get('request_id')})
    return rows


def tick(operational: CamsConfig, root: Path, cycle: int, *, expected_sha256=None) -> dict:
    if cycle not in (0, 12):
        raise ValueError('Only 00/12 UTC cycles are supported')
    plan = load_plan(root/'plan.json', expected_sha256)
    week = active_week(root, plan)
    if week is None:
        return {'status': 'complete', 'cycle': cycle}
    state = {'checked_at': now(), 'cycle': cycle, 'week': week['id'], 'blocked': []}
    priority = pending_inits(operational, cycle, dt.datetime.now(dt.timezone.utc))
    if priority:
        return {**state, 'status': 'waiting_for_realtime', 'priority_inits': priority}
    # Separate historical floor; normal cron keeps its original configuration.
    config = operational.model_copy(update={'first_init': plan['start']+'00', 'max_run_seconds': 0,
                                             'hourly_request_mode': 'bundle'})
    if shutil.disk_usage(operational.output_dir)[2] < 50 * 1024**3:
        return {**state, 'status': 'waiting_for_disk_space'}
    for key in week['inits']:
        if int(key[8:]) != cycle:
            continue
        folder = config.output_dir/SOURCE/key[:8]/f'{cycle:02d}z'
        if (folder/f'{key[:8]}_{cycle:02d}z_{SOURCE}.manifest.json').is_file():
            continue
        blocked = blocked_groups(folder, key)
        if blocked:
            state['blocked'].extend(blocked)
            continue
        try:
            return {**state, **run_init(config, key[:8], cycle), 'init': key}
        except BlockedRequestError as error:
            return {**state, 'status': 'blocked', 'init': key, 'error': safe_error(error, '')}
    return {**state, 'status': 'needs_inspection' if state['blocked'] else 'waiting_for_week_acceptance'}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--plan-sha256', required=True)
    parser.add_argument('--cycle', type=int, choices=(0, 12), required=True)
    args = parser.parse_args(argv)
    args.root.mkdir(parents=True, exist_ok=True)
    with (args.root/f'worker-{args.cycle:02d}.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0
        try:
            result = tick(load_config(args.config), args.root, args.cycle, expected_sha256=args.plan_sha256)
        except Exception as error:
            result = {'status': 'failed', 'cycle': args.cycle, 'checked_at': now(), 'error': safe_error(error, '')}
        save(args.root/f'worker-{args.cycle:02d}.json', result)
        print(json.dumps(result, ensure_ascii=False))
        return 1 if result['status'] in ('failed', 'blocked', 'needs_inspection') else 0


if __name__ == '__main__':
    raise SystemExit(main())
