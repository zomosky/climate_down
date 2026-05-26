"""Submit-poll-download orchestrator for S2S jobs.

A job fans out into ``len(dates) * len(cycles)`` *inits*, each of which
fans out into one ``cdsapi.retrieve`` per :class:`S2SVariableGroup`. Groups
within an init run sequentially because ECDS shares one queue per token and
parallel submissions just bunch behind each other; inits across different
``(date, cycle)`` pairs run concurrently up to ``init_concurrency``.

Per-init pipeline::

    for each variable group:
        resume check (skip if local GRIB is already valid)
        build request -> client.retrieve -> validate header/trailer
    -> write manifest.json -> return S2SResult list

Failures in one group never abort the rest of the init; failures in one
init never abort the rest of the job. Every error is captured into a
:class:`S2SFailure` and the run finishes with an :class:`S2SOutcome`
summary plus a ``_runs/run_<ts>.json`` report (same shape as
:mod:`climate_download.jobs`).
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

import structlog

from climate_download.s2s.client import (
    ECDSClientFactory,
    ECDSClientLike,
    load_credentials,
    make_client,
)
from climate_download.s2s.config import (
    S2SDownloadConfig,
    S2SJobConfig,
    S2SLeadtimeRange,
    S2SVariableGroup,
)
from climate_download.s2s.source import S2SSource

__all__ = [
    "S2SFailure",
    "S2SOutcome",
    "S2SResult",
    "build_retrieve_request",
    "render_leadtimes",
    "run_s2s_job",
]

_log = structlog.get_logger(__name__)


@dataclass(slots=True)
class S2SResult:
    """Outcome of one (date, cycle, group) retrieve."""

    date: str
    cycle: int
    group: str
    output_path: Path
    bytes_downloaded: int
    elapsed_seconds: float
    request: dict
    resumed: bool = False


@dataclass(slots=True)
class S2SFailure:
    """One failed unit of work captured by :func:`run_s2s_job`.

    ``group`` is ``None`` when the failure is at the init scope (e.g. the
    cdsapi client could not be constructed). ``phase`` names where in the
    pipeline we gave up: ``client``, ``retrieve``, ``validate``, ``manifest``.
    """

    date: str
    cycle: int
    group: str | None
    phase: str
    error: str


@dataclass(slots=True)
class S2SOutcome:
    """What :func:`run_s2s_job` returns."""

    succeeded: list[S2SResult] = field(default_factory=list)
    failed: list[S2SFailure] = field(default_factory=list)
    report_path: Path | None = None

    @property
    def total(self) -> int:
        return len(self.succeeded) + len(self.failed)

    @property
    def all_failed(self) -> bool:
        return not self.succeeded and bool(self.failed)


def render_leadtimes(group: S2SVariableGroup, default: S2SLeadtimeRange) -> list[str]:
    """Materialise the ``leadtime_hour`` API values for one group.

    ``instant`` groups use the 6h-spaced grid (``"0", "6", ..., "1104"``);
    ``daily`` groups use 24h windows expressed as ``"<start>_<end>"`` (e.g.
    ``"0_24"``) walking forward by the configured step (typically 24).
    """
    rng = group.leadtime or default
    if group.leadtime_kind == "instant":
        return [str(h) for h in range(rng.start, rng.end + 1, rng.step)]
    # daily-averaged: each value covers a 24h window starting at multiples
    # of `rng.step`; the last window must still close at-or-before rng.end.
    values: list[str] = []
    h = rng.start
    while h + 24 <= rng.end:
        values.append(f"{h}_{h + 24}")
        h += rng.step
    return values


def build_retrieve_request(
    source: S2SSource,
    group: S2SVariableGroup,
    *,
    date: str,
    cycle: int,
    default_leadtime: S2SLeadtimeRange,
) -> dict:
    """Translate (source, group, init) into the ECDS form payload."""
    parsed = dt.datetime.strptime(date, "%Y%m%d")
    leadtimes = render_leadtimes(group, default_leadtime)
    if not leadtimes:
        raise ValueError(
            f"group {group.name!r}: leadtime range produced zero values "
            f"(start={default_leadtime.start}, end={default_leadtime.end}, "
            f"step={default_leadtime.step}, kind={group.leadtime_kind})"
        )
    req: dict = {
        "origin": source.origin,
        "forecast_type": source.forecast_type,
        "level_type": group.level_type,
        "variable": list(group.variables),
        "year": [f"{parsed.year:04d}"],
        "month": [f"{parsed.month:02d}"],
        "day": [f"{parsed.day:02d}"],
        "leadtime_hour": leadtimes,
        "time": [f"{cycle:02d}:00"],
        "data_format": "grib",
    }
    if group.levels:
        req["level_value"] = [_format_level(group.level_type, lv) for lv in group.levels]
    return req


def _format_level(level_type: str, level: str) -> str:
    """Match the ECDS form spelling: ``"925_hpa"`` for pressure, ``"320_k"``
    for isentropic. Bare numerics and already-suffixed values are accepted
    so the YAML can stay terse (``levels: [925, 1000]``).
    """
    s = str(level).strip().lower()
    if level_type == "pressure":
        return s if s.endswith("_hpa") else f"{s}_hpa"
    if level_type == "isentropic":
        return s if s.endswith("_k") else f"{s}_k"
    return s


def _resolve_paths(
    download: S2SDownloadConfig,
    *,
    source_name: str,
    date: str,
    cycle: int,
    group_name: str,
) -> Path:
    subdir = download.subdir_template.format(
        source=source_name, date=date, cycle=cycle,
    )
    filename = download.filename_template.format(
        source=source_name, date=date, cycle=cycle, group=group_name,
    )
    return download.output_dir / subdir / filename


def _resume_check(path: Path) -> bool:
    """Return True if ``path`` is already a valid multi-message GRIB file."""
    if not path.is_file() or path.stat().st_size < 8:
        return False
    try:
        with path.open("rb") as fh:
            head = fh.read(4)
            fh.seek(-4, 2)
            tail = fh.read(4)
    except OSError:
        return False
    return head == b"GRIB" and tail == b"7777"


def _validate_grib(path: Path) -> int:
    """Lightweight sanity check: header + trailer present, count GRIB markers."""
    data = path.read_bytes()
    if not data.startswith(b"GRIB"):
        raise RuntimeError(f"{path}: missing GRIB header")
    if not data.endswith(b"7777"):
        raise RuntimeError(f"{path}: missing 7777 trailer")
    return data.count(b"GRIB")


def _run_one_group(
    client: ECDSClientLike,
    source: S2SSource,
    group: S2SVariableGroup,
    download: S2SDownloadConfig,
    *,
    date: str,
    cycle: int,
    default_leadtime: S2SLeadtimeRange,
) -> S2SResult:
    out = _resolve_paths(
        download, source_name=source.name, date=date,
        cycle=cycle, group_name=group.name,
    )
    if _resume_check(out):
        size = out.stat().st_size
        # File already valid on disk — skip the retrieve, but rebuild the
        # MARS request locally (no network) so the resumed S2SResult /
        # manifest carry the request payload and on-disk size instead of
        # placeholder zeros. If request construction itself fails (e.g.
        # leadtime range yields zero values for this group) we keep the
        # valid file and fall back to an empty request with a warning.
        try:
            req = build_retrieve_request(
                source, group, date=date, cycle=cycle,
                default_leadtime=default_leadtime,
            )
        except Exception as exc:
            _log.warning(
                "group_resume_request_unavailable",
                date=date, cycle=cycle, group=group.name, path=str(out),
                error=f"{type(exc).__name__}: {exc}",
            )
            req = {}
        _log.info("group_skipped", date=date, cycle=cycle, group=group.name,
                  path=str(out), reason="existing valid GRIB",
                  size_bytes=size)
        return S2SResult(
            date=date, cycle=cycle, group=group.name, output_path=out,
            bytes_downloaded=size, elapsed_seconds=0.0, request=req,
            resumed=True,
        )
    if out.exists():
        _log.warning("group_resume_corrupt", path=str(out),
                     action="delete and re-download")
        out.unlink()
    out.parent.mkdir(parents=True, exist_ok=True)

    req = build_retrieve_request(
        source, group, date=date, cycle=cycle,
        default_leadtime=default_leadtime,
    )
    _log.info("retrieve_submit", date=date, cycle=cycle, group=group.name,
              origin=source.origin, level_type=group.level_type,
              variables=list(group.variables),
              leadtime_count=len(req["leadtime_hour"]))
    t0 = dt.datetime.now()
    client.retrieve(source.collection, req, str(out))
    elapsed = (dt.datetime.now() - t0).total_seconds()
    msg_count = _validate_grib(out)
    size = out.stat().st_size
    _log.info("group_done", date=date, cycle=cycle, group=group.name,
              path=str(out), size_bytes=size, grib_messages=msg_count,
              elapsed_seconds=round(elapsed, 1))
    return S2SResult(
        date=date, cycle=cycle, group=group.name, output_path=out,
        bytes_downloaded=size, elapsed_seconds=elapsed, request=req,
    )


def _run_init(
    config: S2SJobConfig,
    client_factory: ECDSClientFactory | None,
    *,
    date: str,
    cycle: int,
    write_manifest: bool,
    on_init_started: Callable[[int], None] | None = None,
    on_group_done: Callable[[], None] | None = None,
) -> tuple[list[S2SResult], list[S2SFailure]]:
    """Download every group for one (date, cycle) and write the manifest.

    ``on_init_started`` (when supplied) is invoked once with the number of
    groups this init will attempt — used by the per-init group bar to reset
    its total before any download begins.
    """
    results: list[S2SResult] = []
    failures: list[S2SFailure] = []
    # Notify the bar this init is starting so it can reset to 0/len(groups).
    if on_init_started is not None:
        on_init_started(len(config.groups))
    try:
        creds = load_credentials()
        client = make_client(creds, factory=client_factory)
    except Exception as exc:
        _log.exception("client_failed", date=date, cycle=cycle)
        failures.append(S2SFailure(
            date=date, cycle=cycle, group=None, phase="client",
            error=f"{type(exc).__name__}: {exc}",
        ))
        return results, failures
    for group in config.groups:
        try:
            results.append(_run_one_group(
                client, config.source, group, config.download,
                date=date, cycle=cycle,
                default_leadtime=config.time.leadtime,
            ))
        except Exception as exc:
            _log.exception("group_failed",
                           date=date, cycle=cycle, group=group.name)
            failures.append(S2SFailure(
                date=date, cycle=cycle, group=group.name,
                phase="retrieve",
                error=f"{type(exc).__name__}: {exc}",
            ))
        finally:
            if on_group_done is not None:
                on_group_done()

    if write_manifest and (results or failures):
        # Failures are folded into the manifest so restorage can distinguish
        # "group attempted but errored" from "group never attempted"
        # (absent from both lists) without reading the run report.
        from climate_download.s2s.manifest import write_s2s_manifest
        try:
            path = write_s2s_manifest(config, results, failures=failures)
            _log.info("manifest_written", path=str(path),
                      groups=len(results), failures=len(failures))
        except Exception as exc:
            _log.exception("manifest_failed", date=date, cycle=cycle)
            failures.append(S2SFailure(
                date=date, cycle=cycle, group=None, phase="manifest",
                error=f"{type(exc).__name__}: {exc}",
            ))
    return results, failures


def _write_run_report(outcome: S2SOutcome, *, output_dir: Path,
                      config_path: str | None) -> Path:
    """Persist a per-run summary under ``{output_dir}/_runs/run_<ts>.json``."""
    runs_dir = output_dir / "_runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    path = runs_dir / f"run_s2s_{ts}.json"
    payload = {
        "schema_version": 1,
        "kind": "s2s",
        "completed_at": dt.datetime.now(dt.UTC).isoformat(),
        "config": config_path,
        "succeeded": len(outcome.succeeded),
        "failed": len(outcome.failed),
        "total": outcome.total,
        "results": [
            {
                "date": r.date, "cycle": r.cycle, "group": r.group,
                "path": str(r.output_path),
                "bytes_downloaded": r.bytes_downloaded,
                "elapsed_seconds": round(r.elapsed_seconds, 2),
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
    *, enabled: bool, n_inits: int, n_groups: int, n_slots: int,
) -> Iterator[tuple[object | None, list[object] | None]]:
    """Yield ``(pbar_init, group_bars)`` tqdm bars, or ``(None, None)``.

    ``group_bars`` is a list of ``n_slots`` bars — one per concurrent init
    slot — so every in-flight init owns its own per-init group bar instead
    of sharing one (which would mix tick streams under ``init_concurrency >
    1`` and make the numerator meaningless). ``n_slots == 1`` keeps the
    classic two-bar layout; ``n_slots > 1`` adds suffixed bars
    (``groups #1``, ``groups #2``…). The custom stderr handler installed by
    :func:`configure_logging` routes log lines through ``tqdm.write()`` so
    WARNING/ERROR events (httpx retries, cdsapi queue messages) appear
    above the bars instead of corrupting them. Bars auto-disable when
    ``stderr`` is not a TTY so ``nohup`` and ``--log-file`` redirection
    never see ``\\r``-based progress noise.
    """
    pbar_init: object | None = None
    group_bars: list[object] | None = None
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
                group_bars = [
                    tqdm(
                        total=n_groups,
                        desc=("groups" if n_slots == 1 else f"groups #{i + 1}"),
                        unit="grp", position=1 + i, leave=True,
                        file=sys.stderr, dynamic_ncols=True,
                    )
                    for i in range(n_slots)
                ]
    try:
        yield pbar_init, group_bars
    finally:
        if group_bars is not None:
            for b in group_bars:
                b.close()
        if pbar_init is not None:
            pbar_init.close()


def run_s2s_job(
    config: S2SJobConfig,
    *,
    write_manifest: bool = True,
    write_report: bool = True,
    config_path: str | None = None,
    client_factory: ECDSClientFactory | None = None,
) -> S2SOutcome:
    """Execute every (date, cycle, group) combination declared by ``config``.

    Inits run concurrently up to ``download.init_concurrency``; groups
    within an init run sequentially. ``client_factory`` is for tests; in
    production it defaults to a real ``cdsapi.Client``.
    """
    config.download.output_dir.mkdir(parents=True, exist_ok=True)
    dates = config.time.expanded_dates()
    cycles = config.time.expanded_cycles()
    inits = [(d, c) for d in dates for c in cycles]
    _log.info(
        "s2s_job_planned",
        source=config.source.name, origin=config.source.origin,
        collection=config.source.collection,
        dates=dates, cycles=cycles, inits=len(inits),
        groups=[g.name for g in config.groups],
        init_concurrency=config.download.init_concurrency,
    )

    outcome = S2SOutcome()
    workers = max(1, min(config.download.init_concurrency, len(inits)))
    done = 0
    with _progress_context(
        enabled=config.download.progress_bar,
        n_inits=len(inits), n_groups=len(config.groups), n_slots=workers,
    ) as (pbar_init, group_bars):
        # Pool of free group bars; each ``_run_init`` checks one out for its
        # lifetime and returns it so the next pending init can reuse the slot.
        slot_queue: "queue.Queue[object] | None" = None
        if group_bars is not None:
            slot_queue = queue.Queue()
            for bar in group_bars:
                slot_queue.put(bar)

        def _run_init_with_slot(d: str, c: int) -> tuple[
            list[S2SResult], list[S2SFailure]
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
                    config, client_factory,
                    date=d, cycle=c, write_manifest=write_manifest,
                    on_init_started=_reset,
                    on_group_done=_bump,
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
                    outcome.failed.append(S2SFailure(
                        date=d, cycle=c, group=None, phase="init",
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

    outcome.succeeded.sort(key=lambda r: (r.date, r.cycle, r.group))
    outcome.failed.sort(key=lambda f: (f.date, f.cycle, f.group or ""))

    if write_report:
        outcome.report_path = _write_run_report(
            outcome, output_dir=config.download.output_dir,
            config_path=config_path,
        )
        _log.info("run_report_written", path=str(outcome.report_path),
                  succeeded=len(outcome.succeeded),
                  failed=len(outcome.failed))
    return outcome
