"""Per-init manifest writer for S2S jobs.

Mirrors :mod:`climate_download.manifest` in spirit (atomic temp + rename,
sha256 per file, single-init scope) but describes the S2S "one retrieve per
group" reality rather than per-step files: each entry covers one group's
multi-message GRIB and records the request payload that produced it.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from climate_download.s2s.config import S2SJobConfig
from climate_download.s2s.jobs import S2SFailure, S2SResult

__all__ = ["build_s2s_manifest", "s2s_manifest_path", "write_s2s_manifest"]

_SCHEMA_VERSION = 1
_HASH_CHUNK = 1 << 20  # 1 MiB


def _sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(_HASH_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def _group_summary(config: S2SJobConfig) -> list[dict[str, Any]]:
    return [
        {
            "name": g.name,
            "level_type": g.level_type,
            "leadtime_kind": g.leadtime_kind,
            "variables": list(g.variables),
            "levels": list(g.levels) if g.levels is not None else None,
        }
        for g in config.groups
    ]


def s2s_manifest_path(
    config: S2SJobConfig,
    results: Sequence[S2SResult],
    *,
    failures: Sequence[S2SFailure] = (),
) -> Path:
    """Resolve where the manifest for one init should live.

    Either ``results`` or ``failures`` (or both) must be non-empty; the
    first available entry (results preferred) supplies the ``(date, cycle)``
    used to derive the path.
    """
    if not results and not failures:
        raise ValueError("results and failures must not both be empty")
    if results:
        first_date = results[0].date
        first_cycle = results[0].cycle
    else:
        first_date = failures[0].date
        first_cycle = failures[0].cycle
    base = config.download.output_dir / config.download.subdir_template.format(
        source=config.source.name, date=first_date, cycle=first_cycle,
    )
    return base / (
        f"{first_date}_{first_cycle:02d}z_{config.source.name}.manifest.json"
    )


def build_s2s_manifest(
    config: S2SJobConfig,
    results: Iterable[S2SResult],
    *,
    completed_at: dt.datetime | None = None,
    failures: Iterable[S2SFailure] = (),
) -> dict[str, Any]:
    """Serialise a finished S2S init into a manifest dictionary.

    ``failures`` (when supplied) populates a top-level ``failures: [...]``
    array so downstream consumers can tell ``"group never attempted"`` apart
    from ``"group attempted but errored"`` without reading the run report.
    """
    items = list(results)
    fail_items = list(failures)
    if not items and not fail_items:
        raise ValueError("results and failures must not both be empty")
    dates = {r.date for r in items} | {f.date for f in fail_items}
    cycles = {r.cycle for r in items} | {f.cycle for f in fail_items}
    if len(dates) != 1 or len(cycles) != 1:
        raise ValueError(
            f"manifest expects single (date, cycle); "
            f"got dates={dates} cycles={cycles}"
        )
    date = next(iter(dates))
    cycle = next(iter(cycles))
    init_time = (
        dt.datetime.strptime(date, "%Y%m%d")
        .replace(hour=cycle, tzinfo=dt.UTC)
        .isoformat()
    )
    when = (completed_at or dt.datetime.now(dt.UTC)).isoformat()

    files: list[dict[str, Any]] = []
    for r in sorted(items, key=lambda x: x.group):
        files.append(
            {
                "group": r.group,
                "path": str(r.output_path),
                "size_bytes": r.bytes_downloaded if r.bytes_downloaded
                else r.output_path.stat().st_size,
                "sha256": _sha256_of(r.output_path),
                "elapsed_seconds": round(r.elapsed_seconds, 2),
                "request": r.request,
                "resumed": r.resumed,
            }
        )

    failure_entries = [
        {"group": f.group, "phase": f.phase, "error": f.error}
        for f in sorted(fail_items, key=lambda f: (f.group is None, f.group or ""))
    ]
    return {
        "schema_version": _SCHEMA_VERSION,
        "kind": "s2s",
        "source": {
            "name": config.source.name,
            "description": config.source.description,
            "collection": config.source.collection,
            "origin": config.source.origin,
            "forecast_type": config.source.forecast_type,
        },
        "init_time": init_time,
        "date": date,
        "cycle": cycle,
        "completed_at": when,
        "groups": _group_summary(config),
        "download": {
            "output_dir": str(config.download.output_dir),
            "init_concurrency": config.download.init_concurrency,
            "request_timeout_seconds": config.download.request_timeout_seconds,
        },
        "files": files,
        "failures": failure_entries,
    }


def write_s2s_manifest(
    config: S2SJobConfig,
    results: Sequence[S2SResult],
    *,
    completed_at: dt.datetime | None = None,
    failures: Sequence[S2SFailure] = (),
) -> Path:
    """Write the manifest atomically and return its path."""
    payload = build_s2s_manifest(
        config, results, completed_at=completed_at, failures=failures,
    )
    out = s2s_manifest_path(config, results, failures=failures)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(out.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=False)
        fh.write("\n")
    os.replace(tmp, out)
    return out
