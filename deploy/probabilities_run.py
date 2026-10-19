#!/usr/bin/env python3
"""Host cron: bounded, resumable ENS probability download and restore.

Asia/Shanghai: 05:15 --cycle 12; 17:15 --cycle 0.
Only stdlib is needed on the host. Projects run in the existing container.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import fcntl
import json
import os
from pathlib import Path
import subprocess
import time

SOURCES = ("ifs-ens-probabilities", "aifs-ens-probabilities")


def init_dates(now: datetime, cycle: int, lookback: int = 1) -> list[str]:
    latest = (now.astimezone(timezone.utc) - timedelta(hours=cycle + 9)).date()
    return [(latest - timedelta(days=n)).strftime("%Y%m%d") for n in range(lookback + 1)]


def commands(source: str, date: str, cycle: int) -> tuple[list[str], list[str]]:
    container = os.environ.get("CONTAINER", "zhangmy-dev")
    download_dir = os.environ.get("DOWNLOAD_DIR", "/workspace/climate_down")
    restore_dir = os.environ.get("RESTORE_DIR", "/workspace/climate_restorage")
    root = os.environ.get("DOWNLOAD_ROOT", "/climate_data")
    output = os.environ.get("ZARR_ROOT", "/climate_data/climate_data_storage/zarr")
    job = source.replace("-", "_")
    def inside(directory: str, argv: list[str]) -> list[str]:
        return ["docker", "exec", "-w", directory, container, "env",
                "OMP_NUM_THREADS=1", "OPENBLAS_NUM_THREADS=1", "NUMEXPR_NUM_THREADS=1",
                "uv", "run", "--no-sync", *argv]
    download = inside(download_dir, ["climate_download", "run", "--config",
        f"config/jobs/{job}_ser.yaml", "--date", date, "--cycle", str(cycle),
        "--output-dir", root, "--no-progress"])
    restore = inside(restore_dir, ["climate_restore", "scan-once", "--source", source,
        "--download-root", root, "--output-dir", output, "--output-format", "zarr", "--workers", "1"])
    return download, restore


def run_target(source: str, date: str, cycle: int, run=subprocess.run) -> bool:
    download, restore = commands(source, date, cycle)
    for phase, argv in (("download", download), ("restore", restore)):
        print(json.dumps({"at": datetime.now(timezone.utc).isoformat(), "source": source,
                          "date": date, "cycle": cycle, "phase": phase}), flush=True)
        try:
            rc = run(argv, timeout=2700, check=False).returncode
        except (OSError, subprocess.TimeoutExpired) as exc:
            print(f"{source} {date} {phase}: {exc}", flush=True)
            return False
        if rc:
            print(f"{source} {date} {phase}: exit {rc}; not ready", flush=True)
            return False
    return True


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cycle", type=int, choices=(0, 12), required=True)
    parser.add_argument("--date", help="Explicit UTC init date (YYYYMMDD), for backfill")
    parser.add_argument("--lookback-days", type=int, choices=(0, 1, 2), default=1)
    parser.add_argument("--attempts", type=int, choices=range(1, 5), default=3)
    parser.add_argument("--retry-seconds", type=int, default=900)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if args.date:
        datetime.strptime(args.date, "%Y%m%d")
    dates = [args.date] if args.date else init_dates(datetime.now(timezone.utc), args.cycle, args.lookback_days)
    targets = [(source, date) for date in dates for source in SOURCES]
    if args.dry_run:
        for source, date in targets:
            print(json.dumps({"source": source, "date": date, "cycle": args.cycle,
                              "commands": commands(source, date, args.cycle)}))
        return 0
    lock = Path(os.environ.get("PROBABILITY_LOCK", "/home/zhangmingyu/operation/lock/ens_probabilities.lock"))
    lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("ENS probability run already active; skipping duplicate", flush=True)
            return 0
        for attempt in range(args.attempts):
            targets = [(s, d) for s, d in targets if not run_target(s, d, args.cycle)]
            if not targets:
                return 0
            if attempt + 1 < args.attempts:
                time.sleep(max(0, args.retry_seconds))
    print(f"ENS probability run failed: {targets}", flush=True)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
