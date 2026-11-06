#!/usr/bin/env python3
"""Host cron for GEFS: UTC 09:30 eligibility, persistent retries, isolated pipeline."""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, timezone
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def save(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp = tempfile.mkstemp(prefix='.' + path.name, dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(data, f, indent=2); f.flush(); os.fsync(f.fileno())
        os.replace(temp, path)
    finally:
        if os.path.exists(temp): os.unlink(temp)


def eligible_dates(now, start_date, pending=()):
    """Today becomes eligible at 17:30 Beijing; unresolved older dates never expire."""
    latest = (now.astimezone(timezone.utc) - timedelta(hours=9, minutes=30)).date()
    start = datetime.strptime(start_date, '%Y%m%d').date()
    dates = {d for d in pending if start <= datetime.strptime(d, '%Y%m%d').date() <= latest}
    for offset in range(3):
        d = latest - timedelta(days=offset)
        if d >= start: dates.add(d.strftime('%Y%m%d'))
    return sorted(dates, reverse=True)


def commands(cfg, date):
    root, release = cfg['data_root'], cfg['release_root']
    members = cfg.get('member_count', 5)
    if type(members) is not int or not 1 <= members <= 31: raise ValueError('member_count must be 1..31')
    manifest = f'{root}/gefs-ens/{date}/00z/{date}_00z_gefs-ens.manifest.json'
    def inside(project, args):
        directory = f'{release}/{project}'
        return ['docker', 'exec', '-w', directory, cfg.get('container', 'zhangmy-dev'),
            'timeout', '--signal=TERM', '--kill-after=30s', '7200s',
            f'{directory}/.venv/bin/python', f'{release}/download/deploy/gefs_run.py',
            '--bounded-exec', '--', *args]
    download = inside('download', [f'{release}/download/.venv/bin/climate_download',
        'gefs-ens', '--date', date, '--member-count', str(members), '--workers', '4', '--output-dir', root,
        '--base-url', cfg.get('download_base_url', 'https://noaa-gefs-pds.s3.amazonaws.com')])
    restore = inside('climate_restorage', [f'{release}/climate_restorage/.venv/bin/python',
        f'{release}/climate_restorage/deploy/gefs_publish.py', '--manifest', manifest,
        '--output-dir', cfg['zarr_root'], '--member-count', str(members)])
    return [('download', download), ('publish', restore)]


def completed(cfg, date, record):
    """A successful receipt plus both physical products; changing member count is not silent."""
    if record.get('status') != 'complete' or record.get('member_count') != cfg.get('member_count', 5): return False
    root = Path(cfg['host_zarr_root'])
    return all((root / s / f'{date}_00z_{s}.zarr' / '.zmetadata').is_file() for s in ['gefs-ens', 'gefs-ctrl'])


def tick(cfg, state, now, run=subprocess.run):
    dates = eligible_dates(now, cfg['start_date'], state.get('dates', {}))
    records = state.setdefault('dates', {})
    for date in dates:
        record = records.setdefault(date, {})
        if completed(cfg, date, record): continue
        for phase, cmd in commands(cfg, date):
            record.update(status=phase, updated_at=datetime.now(timezone.utc).isoformat(), member_count=cfg.get('member_count', 5))
            save(Path(cfg['state_file']), state)
            print(json.dumps({'date': date, 'phase': phase, 'at': record['updated_at']}), flush=True)
            # Timeout is inside the container so its entire worker process group is terminated.
            result = run(cmd, check=False)
            if result.returncode:
                record.update(status='pending', failed_phase=phase, exit_code=result.returncode)
                break
        else:
            record.update(status='complete', completed_at=datetime.now(timezone.utc).isoformat())
            record.pop('failed_phase', None); record.pop('exit_code', None)
        save(Path(cfg['state_file']), state)
    return 0 if all(completed(cfg, d, records[d]) for d in dates) else 1


def bounded_exec(argv):
    import resource
    # Per-process virtual memory and two CPU affinity slots, inherited by child workers.
    resource.setrlimit(resource.RLIMIT_AS, (12 * 1024**3, 12 * 1024**3))
    if hasattr(os, 'sched_getaffinity'): os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:2])
    os.nice(10)
    for key in ['OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMEXPR_NUM_THREADS']:
        os.environ[key] = '1'
    os.execv(argv[0], argv)


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if argv[:1] == ['--bounded-exec']: bounded_exec(argv[2:]); return 0
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config', type=Path, required=True); p.add_argument('--dry-run', action='store_true')
    args = p.parse_args(argv); cfg = json.loads(args.config.read_text())
    state_path = Path(cfg['state_file'])
    lock = Path(cfg['lock_file']); lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open('a') as handle:
        try: fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError: print('GEFS pipeline already running'); return 0
        state = json.loads(state_path.read_text()) if state_path.exists() else {'dates': {}}
        now = datetime.now(timezone.utc)
        if args.dry_run:
            print(json.dumps({d:commands(cfg,d) for d in eligible_dates(now,cfg['start_date'],state['dates'])},indent=2));return 0
        return tick(cfg, state, now)


if __name__ == '__main__': raise SystemExit(main())
