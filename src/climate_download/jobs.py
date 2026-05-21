"""Glue between :mod:`config` (YAML) and :mod:`grib` (download primitives).

A *job* fans out into ``len(dates) * len(cycles)`` independent *inits*; each
init in turn fans out into one download per requested step. Steps that the
upstream bucket does not publish (HEAD on the ``.index`` URL returns 404) are
skipped with a warning rather than failing the whole job — useful for ranges
like ``0-360`` where only a subset is actually available for a given cycle.

Per-init pipeline::

    probe step availability  ->  for each present step:
        resume check (skip if local file already valid)
        fetch .index  ->  filter by variable groups  ->  merge byte ranges
                      ->  PartialDownloader.download  ->  GRIB sanity check
    ->  write manifest.json  ->  return JobResult list

Inits run concurrently (``download.init_concurrency``); steps within an init
remain sequential to keep the per-init manifest write boundary simple.
Failures in one step or one init never crash the whole job — every error is
captured into a :class:`JobFailure` and the run finishes with a
:class:`JobOutcome` summary plus a ``_runs/run_<ts>.json`` report.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import queue
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Callable, Iterator

import httpx
import structlog

from climate_download.config import (
    DownloadConfig,
    JobConfig,
    VariableGroup,
)
from climate_download.grib import (
    IndexFilter,
    IndexRecord,
    PartialDownloader,
    filter_records,
)
from climate_download.sources import Source

__all__ = [
    "JobFailure",
    "JobOutcome",
    "JobResult",
    "probe_available_steps",
    "run_job",
    "select_records",
]

_log = structlog.get_logger(__name__)


@dataclass(slots=True)
class JobResult:
    """Outcome of one (date, cycle, step) download."""

    date: str
    cycle: int
    step: int
    output_path: Path
    bytes_total: int
    bytes_downloaded: int
    records_total: int
    records_selected: int
    http_requests: int
    selected_breakdown: dict[str, int] = field(default_factory=dict)
    resumed: bool = False

    @property
    def savings_pct(self) -> float:
        if self.bytes_total == 0:
            return 0.0
        return round(100 * (1 - self.bytes_downloaded / self.bytes_total), 2)


@dataclass(slots=True)
class JobFailure:
    """One failed unit of work captured by :func:`run_job`.

    ``step`` is ``None`` when the failure is at the init scope (e.g. the
    listing endpoint was unreachable). ``phase`` names where in the pipeline
    we gave up: ``probe``, ``list``, ``index``, ``select``, ``download``,
    ``validate``, ``manifest``.
    """

    date: str
    cycle: int
    step: int | None
    phase: str
    error: str


@dataclass(slots=True)
class JobOutcome:
    """What :func:`run_job` returns: successes, failures and report path."""

    succeeded: list[JobResult] = field(default_factory=list)
    failed: list[JobFailure] = field(default_factory=list)
    report_path: Path | None = None

    @property
    def total(self) -> int:
        return len(self.succeeded) + len(self.failed)

    @property
    def all_failed(self) -> bool:
        return not self.succeeded and bool(self.failed)


def select_records(
    records: list[IndexRecord], groups: list[VariableGroup]
) -> tuple[list[IndexRecord], dict[str, int]]:
    """Apply each variable group as an ``IndexFilter`` and concatenate hits."""
    selected: list[IndexRecord] = []
    breakdown: dict[str, int] = {}
    seen: set[int] = set()
    for group in groups:
        flt = IndexFilter(
            params=group.params, levtypes=[group.levtype], levels=group.levels
        )
        hits = filter_records(records, flt)
        breakdown[group.name] = len(hits)
        for rec in hits:
            if id(rec) in seen:
                continue
            seen.add(id(rec))
            selected.append(rec)
    return selected, breakdown


def _validate_grib(path: Path) -> int:
    data = path.read_bytes()
    if not data.startswith(b"GRIB"):
        raise RuntimeError(f"{path}: missing GRIB header")
    if not data.endswith(b"7777"):
        raise RuntimeError(f"{path}: missing 7777 trailer")
    return data.count(b"GRIB")


def _resume_check(path: Path) -> bool:
    """Return True if ``path`` already contains a valid GRIB document.

    Reads only the first/last 4 bytes so the cost is independent of file
    size. Callers should delete the file and re-download on False.
    """
    if not path.is_file():
        return False
    try:
        with path.open("rb") as fh:
            head = fh.read(4)
            fh.seek(-4, 2)
            tail = fh.read(4)
    except OSError:
        return False
    return head == b"GRIB" and tail == b"7777"


def _resolve_output_path(download: DownloadConfig, *, source_name: str,
                         date: str, cycle: int, step: int) -> Path:
    """Compose ``output_dir / subdir / filename`` for one (date, cycle, step)."""
    subdir = ""
    if download.subdir_template:
        subdir = download.subdir_template.format(
            source=source_name, date=date, cycle=cycle, step=step,
        )
    filename = download.filename_template.format(
        source=source_name, date=date, cycle=cycle, step=step,
    )
    root = download.output_dir / subdir if subdir else download.output_dir
    return root / filename


def _run_one_step(
    source: Source,
    variables: list[VariableGroup],
    download: DownloadConfig,
    date: str,
    cycle: int,
    step: int,
    *,
    client: httpx.Client,
    downloader: PartialDownloader,
) -> JobResult:
    out = _resolve_output_path(
        download, source_name=source.name, date=date, cycle=cycle, step=step,
    )
    if _resume_check(out):
        _log.info("step_skipped", date=date, cycle=cycle, step=step,
                  path=str(out), reason="existing valid GRIB")
        size = out.stat().st_size
        return JobResult(
            date=date, cycle=cycle, step=step, output_path=out,
            bytes_total=size, bytes_downloaded=0,
            records_total=0, records_selected=0,
            http_requests=0, selected_breakdown={}, resumed=True,
        )
    if out.exists():
        _log.warning("step_resume_corrupt", path=str(out),
                     action="delete and re-download")
        out.unlink()

    _log.info(
        "fetch_index",
        url=source.build_index_url(date=date, cycle=cycle, step=step),
    )
    records = source.fetch_records(client, date=date, cycle=cycle, step=step)

    selected, breakdown = select_records(records, variables)
    if not selected:
        raise RuntimeError(f"no records selected for {date} {cycle:02d}z step={step}")

    step_res = source.download_step(
        downloader,
        records=selected,
        output_path=out,
        gap_tolerance=download.gap_tolerance,
        date=date, cycle=cycle, step=step,
    )
    msg_count = _validate_grib(step_res.output_path)

    bytes_total = sum(r.length for r in records)
    result = JobResult(
        date=date, cycle=cycle, step=step, output_path=step_res.output_path,
        bytes_total=bytes_total, bytes_downloaded=step_res.bytes_downloaded,
        records_total=len(records), records_selected=len(selected),
        http_requests=step_res.http_requests, selected_breakdown=breakdown,
    )
    _log.info(
        "step_done",
        date=date, cycle=cycle, step=step, path=str(step_res.output_path),
        bytes_total=bytes_total, bytes_downloaded=step_res.bytes_downloaded,
        records_total=len(records), records_selected=len(selected),
        grib_messages=msg_count, http_requests=step_res.http_requests,
        savings_pct=result.savings_pct,
    )
    return result


def probe_available_steps(
    source: Source,
    *,
    date: str,
    cycle: int,
    candidate_steps: list[int],
    client: httpx.Client,
    max_workers: int = 8,
) -> tuple[list[int], list[int]]:
    """Concurrently probe each candidate step's availability.

    Returns ``(available, missing)`` step lists, both sorted ascending.
    Delegates the per-step decision to :meth:`Source.probe_step` so each
    adapter can use whichever signal makes sense for its upstream (HEAD on
    the sidecar, listing endpoint, etc.).
    """
    if not candidate_steps:
        return [], []

    def _probe(step: int) -> tuple[int, bool]:
        # ``probe_step`` already retries transient network errors via
        # ``request_with_retry``; this wrapper only adapts the signature for
        # the ThreadPoolExecutor map.
        return step, source.probe_step(client, date=date, cycle=cycle, step=step)

    available: list[int] = []
    missing: list[int] = []
    workers = max(1, min(max_workers, len(candidate_steps)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for step, ok in pool.map(_probe, candidate_steps):
            (available if ok else missing).append(step)
    return sorted(available), sorted(missing)


def _resolve_init_steps(
    source: Source, *, date: str, cycle: int,
    candidate_steps: list[int] | None,
    client: httpx.Client,
) -> tuple[list[int], list[int]]:
    """Resolve which steps to attempt for one (date, cycle).

    When ``candidate_steps`` is ``None`` the caller requested ``steps: all``
    and we ask the source to enumerate; falling back to an empty list when
    the source does not implement listing.
    """
    if candidate_steps is None:
        listed = source.list_available_steps(client, date=date, cycle=cycle)
        if listed is None:
            _log.warning("init_list_unsupported",
                         date=date, cycle=cycle, source=source.name)
            return [], []
        return sorted(listed), []
    return probe_available_steps(
        source, date=date, cycle=cycle,
        candidate_steps=candidate_steps, client=client,
    )


def _run_init(
    config: JobConfig,
    *,
    date: str,
    cycle: int,
    candidate_steps: list[int] | None,
    write_manifest: bool,
    on_init_resolved: Callable[[int], None] | None = None,
    on_step_done: Callable[[], None] | None = None,
) -> tuple[list[JobResult], list[JobFailure]]:
    """Download every available step for one (date, cycle) and write manifest.

    Per-step exceptions are captured into ``failures`` rather than raised so
    a single bad forecast hour cannot abort a multi-week backfill.

    ``on_init_resolved`` is invoked once with the number of available steps as
    soon as the upstream listing/probe completes — used by the step progress
    bar to reset its total to this init's actual step count.
    """
    timeout = config.download.timeout_seconds
    results: list[JobResult] = []
    failures: list[JobFailure] = []
    try:
        with httpx.Client(timeout=timeout) as client, \
                PartialDownloader(
                    client=httpx.Client(timeout=timeout),
                    max_workers=config.download.workers,
                    max_attempts=config.download.max_attempts,
                    # The per-step byte-range bar is suppressed when the
                    # outer/inner tqdm bars are active to avoid three lines
                    # of progress fighting over stderr.
                    progress=False,
                ) as downloader:
            try:
                available, missing = _resolve_init_steps(
                    config.source, date=date, cycle=cycle,
                    candidate_steps=candidate_steps, client=client,
                )
            except Exception as exc:
                _log.exception("init_list_failed", date=date, cycle=cycle)
                failures.append(JobFailure(
                    date=date, cycle=cycle, step=None,
                    phase="list" if candidate_steps is None else "probe",
                    error=f"{type(exc).__name__}: {exc}",
                ))
                return results, failures

            _log.info(
                "init_steps_resolved",
                date=date, cycle=cycle,
                requested=("all" if candidate_steps is None else candidate_steps),
                available=available, missing=missing,
            )
            # Reset the step bar to this init's actual count (bar now shows
            # per-init progress instead of a cumulative job-wide total).
            if on_init_resolved is not None:
                on_init_resolved(len(available))
            if missing:
                _log.warning("init_steps_missing",
                             date=date, cycle=cycle, missing=missing)
            if not available:
                _log.warning("init_skipped", date=date, cycle=cycle,
                             reason="no available steps")
                return results, failures

            for step in available:
                try:
                    results.append(_run_one_step(
                        config.source, config.variables, config.download,
                        date, cycle, step,
                        client=client, downloader=downloader,
                    ))
                except Exception as exc:
                    _log.exception("step_failed",
                                   date=date, cycle=cycle, step=step)
                    failures.append(JobFailure(
                        date=date, cycle=cycle, step=step,
                        phase="download",
                        error=f"{type(exc).__name__}: {exc}",
                    ))
                finally:
                    if on_step_done is not None:
                        on_step_done()
    except Exception as exc:  # client / downloader construction errors
        _log.exception("init_failed", date=date, cycle=cycle)
        failures.append(JobFailure(
            date=date, cycle=cycle, step=None, phase="init",
            error=f"{type(exc).__name__}: {exc}",
        ))
        return results, failures

    if write_manifest and results:
        # Local import keeps the manifest module out of the import cycle.
        from climate_download.manifest import write_manifest as _write
        try:
            path = _write(config, results)
            _log.info("manifest_written", path=str(path), files=len(results))
        except Exception as exc:
            _log.exception("manifest_failed", date=date, cycle=cycle)
            failures.append(JobFailure(
                date=date, cycle=cycle, step=None, phase="manifest",
                error=f"{type(exc).__name__}: {exc}",
            ))
    return results, failures


def _write_run_report(outcome: JobOutcome, *, output_dir: Path,
                      config_path: str | None) -> Path:
    """Persist a per-run summary under ``{output_dir}/_runs/run_<ts>.json``."""
    runs_dir = output_dir / "_runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    path = runs_dir / f"run_{ts}.json"
    payload = {
        "schema_version": 1,
        "completed_at": dt.datetime.now(dt.UTC).isoformat(),
        "config": config_path,
        "succeeded": len(outcome.succeeded),
        "failed": len(outcome.failed),
        "total": outcome.total,
        "results": [
            {
                "date": r.date, "cycle": r.cycle, "step": r.step,
                "path": str(r.output_path),
                "bytes_downloaded": r.bytes_downloaded,
                "records_selected": r.records_selected,
                "records_total": r.records_total,
                "http_requests": r.http_requests,
                "savings_pct": r.savings_pct,
                "resumed": r.resumed,
            }
            for r in outcome.succeeded
        ],
        "failures": [asdict(f) for f in outcome.failed],
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
        fh.write("\n")
    tmp.replace(path)
    return path


@contextlib.contextmanager
def _progress_context(
    *, enabled: bool, n_inits: int, n_steps: int | None, n_slots: int,
) -> Iterator[tuple[object | None, list[object] | None]]:
    """Yield ``(pbar_init, step_bars)`` tqdm bars, or ``(None, None)``.

    ``step_bars`` is a list of ``n_slots`` bars — one per concurrent init slot
    — so each in-flight init owns its own per-init step bar instead of
    sharing one (which would mix tick streams under ``init_concurrency > 1``
    and make the numerator meaningless). ``n_slots == 1`` keeps the
    classic two-bar layout; ``n_slots > 1`` adds suffixed bars (``steps #1``,
    ``steps #2``…). The custom stderr handler installed by
    :func:`configure_logging` already routes log lines through
    ``tqdm.write()``, so any WARNING/ERROR events (e.g. ``http_retry``)
    appear cleanly above the bars without corrupting them. Bars auto-disable
    when ``stderr`` is not a TTY so ``nohup`` and ``--log-file`` redirection
    never see ``\\r``-based progress noise. ``n_steps`` is the *per-init*
    step total — used as the initial bar total before the first init
    resolves; the caller resets each bar for the init that acquires it.
    """
    pbar_init: object | None = None
    step_bars: list[object] | None = None
    if enabled and n_inits > 0:
        if not sys.stderr.isatty():
            _log.warning("progress_disabled_non_tty",
                         reason="stderr is not a TTY (nohup / redirect); "
                                "progress bars suppressed to keep logs clean")
        else:
            try:
                from tqdm import tqdm
            except ImportError:  # pragma: no cover — tqdm is a base dep
                _log.warning("tqdm_unavailable")
            else:
                pbar_init = tqdm(
                    total=n_inits, desc="inits", unit="init",
                    position=0, leave=True, file=sys.stderr, dynamic_ncols=True,
                )
                step_bars = [
                    tqdm(
                        total=n_steps,
                        desc=("steps" if n_slots == 1 else f"steps #{i + 1}"),
                        unit="step", position=1 + i, leave=True,
                        file=sys.stderr, dynamic_ncols=True,
                    )
                    for i in range(n_slots)
                ]
    try:
        yield pbar_init, step_bars
    finally:
        if step_bars is not None:
            for b in step_bars:
                b.close()
        if pbar_init is not None:
            pbar_init.close()


def run_job(
    config: JobConfig,
    *,
    write_manifest: bool = True,
    write_report: bool = True,
    config_path: str | None = None,
) -> JobOutcome:
    """Execute every (date, cycle, step) combination declared by ``config``.

    Inits (one per ``(date, cycle)``) run concurrently up to
    ``download.init_concurrency``; steps within an init run sequentially.
    Per-step or per-init exceptions are captured into
    :class:`JobOutcome.failed` instead of aborting the run, so a long
    backfill keeps making forward progress when one upstream object is
    missing or corrupt. A timestamped summary is written under
    ``{output_dir}/_runs/`` when ``write_report`` is true.
    """
    config.download.output_dir.mkdir(parents=True, exist_ok=True)
    dates = config.time.expanded_dates()
    cycles = config.time.expanded_cycles()
    candidate_steps = config.time.expanded_steps()  # list[int] | None
    inits = [(d, c) for d in dates for c in cycles]
    _log.info(
        "job_planned",
        dates=dates, cycles=cycles,
        steps=("all" if candidate_steps is None else candidate_steps),
        inits=len(inits), init_concurrency=config.download.init_concurrency,
    )

    outcome = JobOutcome()
    workers = max(1, min(config.download.init_concurrency, len(inits)))
    done = 0

    # Each step bar tracks one *currently-running* init.  Allocating one bar
    # per worker slot avoids the mixed-tick problem under concurrency: every
    # in-flight init owns its own bar and resets it (via ``on_init_resolved``)
    # once the upstream listing returns the available step count.  Until the
    # first reset the bar uses the explicit step list length (when configured)
    # or stays open-ended for ``steps: all``.
    n_steps_initial = (
        len(candidate_steps) if candidate_steps is not None else None
    )

    with _progress_context(
        enabled=config.download.progress_bar,
        n_inits=len(inits), n_steps=n_steps_initial, n_slots=workers,
    ) as (pbar_init, step_bars):
        # Pool of free step bars; each ``_run_init`` checks one out for its
        # lifetime and returns it so the next pending init can reuse the slot.
        slot_queue: "queue.Queue[object] | None" = None
        if step_bars is not None:
            slot_queue = queue.Queue()
            for bar in step_bars:
                slot_queue.put(bar)

        def _run_init_with_slot(d: str, c: int) -> tuple[
            list[JobResult], list[JobFailure]
        ]:
            bar = slot_queue.get() if slot_queue is not None else None

            def _reset(n: int) -> None:
                if bar is not None:
                    bar.reset(total=n)

            def _bump() -> None:
                if bar is not None:
                    bar.update(1)

            try:
                return _run_init(
                    config, date=d, cycle=c,
                    candidate_steps=candidate_steps,
                    write_manifest=write_manifest,
                    on_init_resolved=_reset,
                    on_step_done=_bump,
                )
            finally:
                if bar is not None and slot_queue is not None:
                    slot_queue.put(bar)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(_run_init_with_slot, d, c): (d, c)
                for d, c in inits
            }
            for fut in as_completed(futures):
                d, c = futures[fut]
                try:
                    succ, fail = fut.result()
                except Exception as exc:  # pragma: no cover — _run_init catches
                    _log.exception("init_uncaught", date=d, cycle=c)
                    outcome.failed.append(JobFailure(
                        date=d, cycle=c, step=None, phase="init",
                        error=f"{type(exc).__name__}: {exc}",
                    ))
                else:
                    outcome.succeeded.extend(succ)
                    outcome.failed.extend(fail)
                done += 1
                if pbar_init is not None:
                    pbar_init.update(1)
                _log.info("progress", done=done, total=len(inits),
                          kind="init", date=d, cycle=c)

    outcome.succeeded.sort(key=lambda r: (r.date, r.cycle, r.step))
    outcome.failed.sort(key=lambda f: (f.date, f.cycle, f.step or -1))

    if write_report:
        outcome.report_path = _write_run_report(
            outcome, output_dir=config.download.output_dir,
            config_path=config_path,
        )
        _log.info("run_report_written", path=str(outcome.report_path),
                  succeeded=len(outcome.succeeded),
                  failed=len(outcome.failed))
    return outcome
