#!/usr/bin/env python3
"""Reconcile recent production NWP initializations on the host."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections.abc import Sequence
from typing import NamedTuple


class SourcePolicy(NamedTuple):
    source: str
    job: str
    cycle: int
    publish_lag_hours: int
    lookback_days: int
    expected_steps: tuple[int, ...]
    log_name: str


DWD_STEPS = tuple(range(0, 79)) + tuple(range(81, 181, 3))
GFS_STEPS = tuple(range(0, 121)) + tuple(range(123, 385, 3))
IFS_STEPS = tuple(range(0, 145, 3)) + tuple(range(150, 361, 6))
AIFS_STEPS = tuple(range(0, 361, 6))

POLICIES = (
    SourcePolicy("dwd-icon", "config/jobs/dwd-icon_renewables_ser.yaml", 12, 5, 0, DWD_STEPS, "download_icon.log"),
    SourcePolicy("dwd-icon", "config/jobs/dwd-icon_renewables_ser.yaml", 0, 3, 0, DWD_STEPS, "download_icon_00z.log"),
    SourcePolicy("gfs-0p25", "config/jobs/gfs_renewables_ser.yaml", 12, 5, 2, GFS_STEPS, "download_gfs.log"),
    SourcePolicy("aifs-single", "config/jobs/aifs_renewables_ser.yaml", 12, 6, 2, AIFS_STEPS, "download_aifs.log"),
    SourcePolicy("ifs-hres", "config/jobs/ifs_renewables_ser.yaml", 12, 8, 2, IFS_STEPS, "download_ifs.log"),
    SourcePolicy("ifs-hres", "config/jobs/ifs_renewables_ser.yaml", 0, 8, 2, IFS_STEPS, "download_ifs_00z.log"),
)


def latest_init_date(now: datetime, policy: SourcePolicy) -> str:
    """Return the newest init expected to be fully published at *now*."""

    shifted = now.astimezone(timezone.utc) - timedelta(
        hours=policy.cycle + policy.publish_lag_hours
    )
    return shifted.strftime("%Y%m%d")


def candidate_dates(now: datetime, policy: SourcePolicy) -> tuple[str, ...]:
    """Return recent init dates from oldest to newest for one policy."""

    latest = datetime.strptime(latest_init_date(now, policy), "%Y%m%d")
    return tuple(
        (latest - timedelta(days=days_ago)).strftime("%Y%m%d")
        for days_ago in range(policy.lookback_days, -1, -1)
    )


def _raw_path(path_value: str, data_root: Path, manifest: Path) -> Path:
    path = Path(path_value)
    try:
        relative = path.relative_to("/climate_data")
    except ValueError:
        return path if path.is_absolute() else manifest.parent / path
    return data_root / relative


def manifest_health(
    manifest: Path,
    data_root: Path,
    expected_steps: int | Sequence[int],
    *,
    verify_boundaries: bool = False,
) -> tuple[bool, str]:
    """Validate the ready signal, step set and raw-file sizes.

    Boundary reads are opt-in because a kernel-level storage wait can otherwise
    pin the periodic reconciler in uninterruptible I/O indefinitely.
    """

    if not manifest.is_file():
        return False, "manifest missing"
    try:
        payload = json.loads(manifest.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        return False, f"manifest unreadable: {exc}"
    if not payload.get("completed_at"):
        return False, "manifest has no completed_at"
    failures = payload.get("failures") or []
    if failures:
        return False, f"manifest has {len(failures)} failure(s)"
    files = payload.get("files") or []
    steps = [entry.get("step_hours") for entry in files]
    if len(steps) != len(set(steps)):
        return False, "manifest has duplicate steps"
    if isinstance(expected_steps, int):
        if len(steps) != expected_steps:
            return False, f"manifest has {len(steps)}/{expected_steps} steps"
    elif set(steps) != set(expected_steps):
        return False, f"manifest step set differs ({len(steps)}/{len(expected_steps)})"
    for entry in files:
        raw = _raw_path(str(entry.get("path", "")), data_root, manifest)
        try:
            if raw.stat().st_size != int(entry["size_bytes"]):
                return False, f"size mismatch: {raw.name}"
            if verify_boundaries:
                with raw.open("rb", buffering=0) as handle:
                    if handle.read(4) != b"GRIB":
                        return False, f"invalid GRIB boundary: {raw.name}"
                    handle.seek(-4, 2)
                    if handle.read(4) != b"7777":
                        return False, f"invalid GRIB boundary: {raw.name}"
        except (OSError, KeyError, TypeError, ValueError):
            return False, f"raw file missing or unreadable: {raw.name}"
    return True, "complete"


def manifest_path(data_root: Path, policy: SourcePolicy, date: str) -> Path:
    cycle = f"{policy.cycle:02d}z"
    return data_root / policy.source / date / cycle / f"{date}_{cycle}_{policy.source}.manifest.json"


def _parse_now(value: str | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _selected_policies(only: set[str], excluded: set[str]) -> tuple[SourcePolicy, ...]:
    return tuple(
        policy
        for policy in POLICIES
        if (not only or policy.source in only) and policy.source not in excluded
    )


def reconcile(
    *,
    now: datetime,
    data_root: Path,
    runner: Path,
    log_dir: Path,
    policies: Sequence[SourcePolicy],
    dry_run: bool,
    verify_boundaries: bool,
) -> int:
    unresolved = 0
    for policy in policies:
        for date in candidate_dates(now, policy):
            manifest = manifest_path(data_root, policy, date)
            healthy, reason = manifest_health(
                manifest,
                data_root,
                policy.expected_steps,
                verify_boundaries=verify_boundaries,
            )
            label = f"{policy.source} {date} {policy.cycle:02d}z"
            if healthy:
                print(f"HEALTHY {label}")
                continue
            print(f"REPAIR {label}: {reason}")
            if dry_run:
                continue
            log_dir.mkdir(parents=True, exist_ok=True)
            environment = os.environ.copy()
            environment.update(
                INIT_DATE=date,
                CYCLE=str(policy.cycle),
                PUBLISH_LAG_HOURS=str(policy.publish_lag_hours),
            )
            log_path = log_dir / policy.log_name
            try:
                with log_path.open("a") as log:
                    log.write(f"\n# reconcile_sources: {label} ({reason})\n")
                    log.flush()
                    result = subprocess.run(
                        [str(runner), policy.job],
                        env=environment,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        check=False,
                    )
            except OSError as exc:
                print(f"UNRESOLVED {label}: runner failed: {exc}", file=sys.stderr)
                unresolved += 1
                continue
            healthy, reason = manifest_health(
                manifest,
                data_root,
                policy.expected_steps,
                verify_boundaries=verify_boundaries,
            )
            if result.returncode != 0 or not healthy:
                print(
                    f"UNRESOLVED {label}: runner rc={result.returncode}; {reason}",
                    file=sys.stderr,
                )
                unresolved += 1
            else:
                print(f"RECOVERED {label}")
    return 1 if unresolved else 0


def _split_sources(values: Sequence[str]) -> set[str]:
    return {item for value in values for item in value.split(",") if item}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=Path("/training_data/climate_data"))
    parser.add_argument("--runner", type=Path, default=Path("/home/zhangmingyu/operation/download_run.sh"))
    parser.add_argument("--log-dir", type=Path, default=Path("/home/zhangmingyu/operation/logs"))
    parser.add_argument("--lock", type=Path, default=Path("/home/zhangmingyu/operation/lock/reconcile_sources.lock"))
    parser.add_argument("--now", help="fixed ISO-8601 UTC timestamp for validation")
    parser.add_argument("--only-source", action="append", default=[])
    parser.add_argument("--exclude-source", action="append", default=[])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--verify-boundaries",
        action="store_true",
        help="also read each raw file's GRIB/7777 markers (manual deep check)",
    )
    args = parser.parse_args(argv)

    only = _split_sources(args.only_source)
    excluded = _split_sources(args.exclude_source)
    policies = _selected_policies(only, excluded)
    unknown = (only | excluded) - {policy.source for policy in POLICIES}
    if unknown:
        parser.error(f"unknown source(s): {', '.join(sorted(unknown))}")

    args.lock.parent.mkdir(parents=True, exist_ok=True)
    with args.lock.open("a+") as lock_handle:
        try:
            fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("SKIP reconcile already running")
            return 0
        return reconcile(
            now=_parse_now(args.now),
            data_root=args.data_root,
            runner=args.runner,
            log_dir=args.log_dir,
            policies=policies,
            dry_run=args.dry_run,
            verify_boundaries=args.verify_boundaries,
        )


if __name__ == "__main__":
    raise SystemExit(main())
