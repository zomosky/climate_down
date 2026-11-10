#!/usr/bin/env python3
"""Finite GEFS history queue; share the live lock and yield before each live tick."""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, timezone
import fcntl
import json
from pathlib import Path
import subprocess
import time

from gefs_run import commands, completed, save


def dates_between(start, end):
    first = datetime.strptime(start, '%Y%m%d').date()
    last = datetime.strptime(end, '%Y%m%d').date()
    if first > last:
        raise ValueError('history start must not exceed end')
    return [(last - timedelta(days=i)).strftime('%Y%m%d') for i in range((last-first).days+1)]


def window_seconds(now):
    # Live cron runs at :00/:30. Reserve its first five minutes and stop a minute
    # before its next tick, including a further 30 seconds for timeout's SIGKILL.
    offset = now.minute % 30 * 60 + now.second + now.microsecond / 1e6
    return 0 if offset < 300 else max(0, int(1800-offset-60))


def tick(cfg, state, now, run=subprocess.run, monotonic=time.monotonic):
    dates = dates_between(cfg['history_start'], cfg['history_end'])
    if cfg['history_end'] >= cfg['live_start_date']:
        raise ValueError('history must end before live start_date')
    records = state.setdefault('dates', {})
    pending = [d for d in dates if not completed(cfg, d, records.get(d, {}))]
    if not pending:
        return {'status': 'complete', 'total': len(dates)}
    budget = window_seconds(now)
    if budget < 60:
        return {'status': 'yield_to_live', 'remaining': len(pending)}
    deadline = monotonic() + budget
    date = pending[0]
    record = records.setdefault(date, {})
    for phase, command in commands(cfg, date):
        if phase == 'download' and record.get('download_complete'):
            continue
        remaining = int(deadline-monotonic())
        # Real full-field publication takes ~12 minutes on this server. Do not
        # repeatedly start a decode that has too little time to finish.
        if remaining < (900 if phase == 'publish' else 60):
            save(Path(cfg['state_file']), state)
            return {'status': 'yield_to_live', 'date': date, 'next_phase': phase}
        command = command.copy()
        command[command.index('7200s')] = f'{remaining}s'
        record.update(status=phase, member_count=cfg.get('member_count', 5),
                      updated_at=datetime.now(timezone.utc).isoformat())
        save(Path(cfg['state_file']), state)
        print(json.dumps({'date': date, 'phase': phase, 'budget_seconds': remaining}), flush=True)
        result = run(command, check=False)
        if result.returncode:
            record.update(status='pending', failed_phase=phase, exit_code=result.returncode)
            # A failed publisher may indicate raw damage; revalidate via downloader.
            if phase == 'publish': record.pop('download_complete', None)
            save(Path(cfg['state_file']), state)
            return {'status': 'pending', 'date': date, 'phase': phase, 'exit_code': result.returncode}
        if phase == 'download':
            record['download_complete'] = True
            save(Path(cfg['state_file']), state)
    record.update(status='complete', completed_at=datetime.now(timezone.utc).isoformat())
    record.pop('failed_phase', None); record.pop('exit_code', None)
    save(Path(cfg['state_file']), state)
    if not completed(cfg, date, record):
        record['status'] = 'pending'
        save(Path(cfg['state_file']), state)
        raise RuntimeError('publisher returned success without both products')
    return {'status': 'published', 'date': date, 'remaining': len(pending)-1}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    cfg = json.loads(args.config.read_text())
    live = json.loads(Path(cfg['live_config']).read_text())
    # Paths, resource bounds and member selection always follow the live service.
    cfg = {**live, **cfg, 'lock_file': live['lock_file'], 'live_start_date': live['start_date']}
    state_path = Path(cfg['state_file'])
    with Path(cfg['lock_file']).open('a') as lock:
        try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print('GEFS live/backfill already running; history skipped'); return
        state = json.loads(state_path.read_text()) if state_path.exists() else {'dates': {}}
        if args.dry_run:
            print(json.dumps({'dates': dates_between(cfg['history_start'], cfg['history_end']),
                              'base_url': cfg['download_base_url'], 'member_count': cfg['member_count']})); return
        print(json.dumps(tick(cfg, state, datetime.now(timezone.utc))), flush=True)


if __name__ == '__main__': main()
