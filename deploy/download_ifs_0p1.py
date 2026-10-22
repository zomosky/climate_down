#!/usr/bin/env python3
"""Check one current IFS 0p1 init, delegating downloads to the existing locked runner."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess

from reconcile_sources import POLICIES, candidate_dates, manifest_health, manifest_path


def run_target(*, cycle, now, data_root, runner, dry_run=False, execute=subprocess.run):
    policy = next(p for p in POLICIES if p.source == 'ifs-hres-0p1' and p.cycle == cycle)
    # Early polling uses the existing date-cutover contract without changing
    # the later completeness-reconciliation policy or its lookback.
    policy = policy._replace(publish_lag_hours=8, lookback_days=0)
    dates = candidate_dates(now, policy)
    if not dates:
        print(json.dumps({'event': 'ifs0p1_skip_before_first_init', 'cycle': cycle}), flush=True)
        return 0
    date = dates[0]
    manifest = manifest_path(data_root, policy, date)
    healthy, reason = manifest_health(manifest, data_root, policy.expected_steps)
    event = 'ifs0p1_skip_healthy' if healthy else 'ifs0p1_check_download'
    print(json.dumps({'event': event, 'date': date, 'cycle': cycle,
                      'reason': reason, 'dry_run': dry_run}), flush=True)
    if healthy or dry_run:
        return 0
    environment = os.environ.copy()
    environment.update(INIT_DATE=date, CYCLE=str(cycle), PUBLISH_LAG_HOURS='8', LOOKBACK_DAYS='0')
    # This retains the same (job, cycle) flock as the existing direct cron and
    # reconciliation invocations, including an already running download.
    result = execute([str(runner), policy.job], env=environment, check=False)
    return result.returncode


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cycle', type=int, choices=[0, 12], required=True)
    parser.add_argument('--data-root', type=Path, default=Path('/training_data/climate_data'))
    parser.add_argument('--runner', type=Path, default=Path('/home/zhangmingyu/operation/download_run.sh'))
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--now', help='Fixed timestamp for dry-run checks only')
    args = parser.parse_args(argv)
    if args.now and not args.dry_run:
        parser.error('--now requires --dry-run')
    now = datetime.fromisoformat(args.now.replace('Z', '+00:00')) if args.now else datetime.now(timezone.utc)
    if now.tzinfo is None:
        parser.error('--now requires an explicit timezone')
    return run_target(cycle=args.cycle, now=now, data_root=args.data_root,
                      runner=args.runner, dry_run=args.dry_run)


if __name__ == '__main__':
    raise SystemExit(main())
