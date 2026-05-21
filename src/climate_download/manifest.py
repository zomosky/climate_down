"""Per-cycle manifest writer used to signal completion to downstream pipelines.

A manifest is one JSON document per ``(source, date, cycle)`` aggregating
every step downloaded in that run. The downstream ``cliamte_data`` pipeline
polls for the manifest file as a "ready" marker rather than racing the GRIB
itself: the manifest is written *atomically* (temp file + ``os.replace``)
only after every step in the cycle has been validated.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from climate_download.config import JobConfig, VariableGroup
from climate_download.jobs import JobResult

__all__ = ["build_manifest", "manifest_path", "write_manifest"]

_SCHEMA_VERSION = 1
_HASH_CHUNK = 1 << 20  # 1 MiB


def _sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(_HASH_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def _variable_summary(groups: Sequence[VariableGroup]) -> list[dict[str, Any]]:
    return [
        {
            "name": g.name,
            "levtype": g.levtype,
            "params": list(g.params),
            "levels": list(g.levels) if g.levels is not None else None,
        }
        for g in groups
    ]


def manifest_path(config: JobConfig, results: Sequence[JobResult]) -> Path:
    """Resolve where the manifest for ``results`` should live.

    All results are expected to share ``(date, cycle)``; we use the first
    entry to derive the filename. When ``download.subdir_template`` is set,
    the manifest lands in the same per-init subdirectory as the GRIB files
    so downstream sensors can poll a single tree.
    """
    if not results:
        raise ValueError("results must not be empty")
    first = results[0]
    base = config.download.output_dir
    if config.download.subdir_template:
        base = base / config.download.subdir_template.format(
            source=config.source.name,
            date=first.date, cycle=first.cycle, step=first.step,
        )
    return base / (
        f"{first.date}_{first.cycle:02d}z_{config.source.name}.manifest.json"
    )


def build_manifest(
    config: JobConfig,
    results: Iterable[JobResult],
    *,
    completed_at: dt.datetime | None = None,
) -> dict[str, Any]:
    """Serialise a finished job into a manifest dictionary."""
    items = list(results)
    if not items:
        raise ValueError("results must not be empty")
    dates = {r.date for r in items}
    cycles = {r.cycle for r in items}
    if len(dates) != 1 or len(cycles) != 1:
        raise ValueError(
            f"manifest expects single (date, cycle); got dates={dates} cycles={cycles}"
        )
    date = items[0].date
    cycle = items[0].cycle
    init_time = (
        dt.datetime.strptime(date, "%Y%m%d")
        .replace(hour=cycle, tzinfo=dt.UTC)
        .isoformat()
    )
    when = (completed_at or dt.datetime.now(dt.UTC)).isoformat()

    files: list[dict[str, Any]] = []
    for r in sorted(items, key=lambda x: x.step):
        files.append(
            {
                "step_hours": r.step,
                "path": str(r.output_path),
                "size_bytes": r.bytes_downloaded,
                "sha256": _sha256_of(r.output_path),
                "records_selected": r.records_selected,
                "records_total": r.records_total,
                "http_requests": r.http_requests,
                "savings_pct": r.savings_pct,
                "selected_breakdown": dict(r.selected_breakdown),
            }
        )

    return {
        "schema_version": _SCHEMA_VERSION,
        "source": {
            "name": config.source.name,
            "description": config.source.description,
        },
        "init_time": init_time,
        "date": date,
        "cycle": cycle,
        "completed_at": when,
        "variables": _variable_summary(config.variables),
        "download": {
            "output_dir": str(config.download.output_dir),
            "workers": config.download.workers,
            "gap_tolerance": config.download.gap_tolerance,
            "timeout_seconds": config.download.timeout_seconds,
            "init_concurrency": config.download.init_concurrency,
        },
        "files": files,
    }


def write_manifest(
    config: JobConfig,
    results: Sequence[JobResult],
    *,
    completed_at: dt.datetime | None = None,
) -> Path:
    """Write the manifest atomically and return its path."""
    payload = build_manifest(config, results, completed_at=completed_at)
    out = manifest_path(config, results)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(out.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=False)
        fh.write("\n")
    os.replace(tmp, out)
    return out
