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
from climate_download.jobs import JobFailure, JobResult

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


def manifest_path(
    config: JobConfig,
    results: Sequence[JobResult],
    *,
    failures: Sequence[JobFailure] = (),
) -> Path:
    """Resolve where the manifest for one init should live.

    All ``results`` and ``failures`` are expected to share ``(date, cycle)``;
    the first available entry (results preferred) derives the filename. When
    ``download.subdir_template`` is set, the manifest lands in the same
    per-init subdirectory as the GRIB files so downstream sensors can poll a
    single tree. ``step`` defaults to ``0`` when only init-scope failures
    exist, which matters only for templates that reference ``{step}``.
    """
    if not results and not failures:
        raise ValueError("results and failures must not both be empty")
    if results:
        first_date = results[0].date
        first_cycle = results[0].cycle
        first_step = results[0].step
    else:
        first_date = failures[0].date
        first_cycle = failures[0].cycle
        first_step = failures[0].step if failures[0].step is not None else 0
    base = config.download.output_dir
    if config.download.subdir_template:
        base = base / config.download.subdir_template.format(
            source=config.source.name,
            date=first_date, cycle=first_cycle, step=first_step,
        )
    return base / (
        f"{first_date}_{first_cycle:02d}z_{config.source.name}.manifest.json"
    )


def build_manifest(
    config: JobConfig,
    results: Iterable[JobResult],
    *,
    completed_at: dt.datetime | None = None,
    failures: Iterable[JobFailure] = (),
) -> dict[str, Any]:
    """Serialise a finished job into a manifest dictionary.

    ``failures`` (when supplied) populates a top-level ``failures: [...]``
    array so a downstream consumer can tell ``"step never attempted"`` apart
    from ``"step attempted but errored"`` without reading the run report.
    """
    items = list(results)
    fail_items = list(failures)
    if not items and not fail_items:
        raise ValueError("results and failures must not both be empty")
    dates = {r.date for r in items} | {f.date for f in fail_items}
    cycles = {r.cycle for r in items} | {f.cycle for f in fail_items}
    if len(dates) != 1 or len(cycles) != 1:
        raise ValueError(
            f"manifest expects single (date, cycle); got dates={dates} cycles={cycles}"
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

    failure_entries = [
        {"step": f.step, "phase": f.phase, "error": f.error}
        for f in sorted(fail_items, key=lambda f: (f.step is None, f.step or 0))
    ]
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
        "failures": failure_entries,
    }


def _product_signature(payload: dict[str, Any]) -> tuple:
    """The parts of a manifest that determine the downstream product + readiness.

    Excludes volatile fields that change between runs without changing what a
    consumer would build: the top-level ``completed_at`` timestamp and the
    per-file download stats (``http_requests`` / ``savings_pct`` / ``records_*``
    / ``selected_breakdown`` — these differ between a fresh download and a
    resume-only re-run of the very same files). What remains is each step's
    ``(step_hours, filename, sha256)`` plus the ``(step, phase)`` of any failure:
    the set of GRIB messages a consumer sees, and whether the init is ready.
    """
    files = sorted(
        (f.get("step_hours"), Path(str(f.get("path", ""))).name, f.get("sha256"))
        for f in payload.get("files", [])
    )
    fails = sorted(
        (f.get("step"), f.get("phase")) for f in payload.get("failures", [])
    )
    return (files, fails)


def _existing_signature(path: Path) -> tuple | None:
    try:
        old = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):  # missing / unreadable / invalid JSON
        return None
    return _product_signature(old)


def write_manifest(
    config: JobConfig,
    results: Sequence[JobResult],
    *,
    completed_at: dt.datetime | None = None,
    failures: Sequence[JobFailure] = (),
) -> Path:
    """Write the manifest atomically and return its path.

    No-op guard: if a manifest already exists at the target path with an
    identical *product signature* (:func:`_product_signature` — same step files +
    sha256 + failures), the write is skipped so the file's mtime is left
    untouched. This stops a resume-only re-run (e.g. an extra cron pass that
    found nothing new) from needlessly re-triggering the downstream restore
    rebuild, which keys off manifest-vs-output mtime.
    """
    payload = build_manifest(
        config, results, completed_at=completed_at, failures=failures,
    )
    out = manifest_path(config, results, failures=failures)
    if out.is_file() and _existing_signature(out) == _product_signature(payload):
        return out
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(out.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=False)
        fh.write("\n")
    os.replace(tmp, out)
    return out
