"""``climate-download`` command-line entry point.

Subcommands:

* ``run`` — load a job YAML, optionally apply ``--date / --cycle / --steps``
  overrides (same shorthand as the YAML fields), then call
  :func:`climate_download.jobs.run_job`.
* ``list-sources`` — print the source ``type:`` names registered in
  :data:`climate_download.sources.SOURCE_REGISTRY` (handy for spotting
  typos in YAML or after adding a new adapter file).
* ``list-steps`` — given a source YAML + ``(date, cycle)``, print every
  forecast hour published in the bucket prefix.
* ``list-variables`` — given a source YAML + ``(date, cycle, step)``, print
  every ``(param, levtype, levelist)`` triple found in the sidecar index.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
from pathlib import Path
from typing import Sequence

import httpx
import structlog

from climate_download.config import TimeConfig, load_job, load_source
from climate_download.config import _resolve_symbolic_date
from climate_download.jobs import run_job
from climate_download.logging_setup import configure_logging
from climate_download.sources import SOURCE_REGISTRY, Source

__all__ = ["build_parser", "main"]


def _add_progress_flags(p: argparse.ArgumentParser) -> None:
    """Wire mutually-exclusive ``--progress / --no-progress`` flags.

    Default is ``None`` (auto = enable when ``stderr`` is a TTY, otherwise
    stay silent). Callers resolve the tri-state via :func:`_resolve_progress`.
    """
    g = p.add_mutually_exclusive_group()
    g.add_argument("--progress", dest="progress",
                   action="store_const", const=True, default=None,
                   help="force-enable tqdm progress bars (default: on when "
                        "stderr is a TTY; suppressed when stderr is "
                        "redirected to a file / nohup, to keep logs clean)")
    g.add_argument("--no-progress", dest="progress",
                   action="store_const", const=False, default=None,
                   help="force-disable progress bars even on a TTY")


def _resolve_progress(flag: bool | None) -> bool:
    """Tri-state ``--progress / --no-progress / auto`` → final bool.

    ``None`` means auto: ``True`` iff ``stderr`` is a TTY. The runtime
    progress-bar helpers also defend against non-TTY environments, so even
    a forced ``True`` from a backgrounded shell will be neutralised before
    the bars touch stderr.
    """
    if flag is not None:
        return flag
    return sys.stderr.isatty()


def _resolve_source(spec: str) -> Source:
    """Accept a YAML path or a bare name like ``aifs``/``gfs``/``hrrr``.

    Bare names resolve against ``config/sources/<name>.yaml`` relative to
    the current working directory; this matches the layout used by every
    job YAML in the repo.
    """
    p = Path(spec)
    if p.is_file():
        return load_source(p)
    candidate = Path("config/sources") / f"{spec}.yaml"
    if candidate.is_file():
        return load_source(candidate)
    raise FileNotFoundError(
        f"source spec {spec!r}: neither a YAML path nor "
        f"a name resolvable to config/sources/<name>.yaml"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="climate-download",
        description="YAML-driven NWP partial-download pipeline "
                    "(AIFS / GFS / HRRR / custom adapters).",
    )
    sub = parser.add_subparsers(dest="command", required=True, metavar="<command>")

    p_run = sub.add_parser(
        "run",
        help="execute a job YAML (with optional CLI overrides)",
        description="Load a job YAML and download every (date, cycle, step) "
                    "combination it expands to.",
    )
    p_run.add_argument("--config", required=True, type=Path,
                       help="path to job YAML (e.g. config/jobs/aifs_wind_pv.yaml)")
    p_run.add_argument("--date",
                       help="override time.date (YYYYMMDD, today, yesterday, "
                            "'a,b,c' list, or 'a-b' YYYYMMDD range)")
    p_run.add_argument("--cycle",
                       help="override time.cycle (single int or comma list, e.g. 0 or 0,12)")
    p_run.add_argument("--steps",
                       help="override time.steps (comma list '0,6,12', range '0-120' "
                            "with default 6h step, '0-120:3' for custom step, or "
                            "MARS-style '0/120/6')")
    p_run.add_argument("--output-dir", type=Path,
                       help="override download.output_dir")
    p_run.add_argument("--init-concurrency", type=int,
                       help="override download.init_concurrency (1 = serial inits)")
    p_run.add_argument("--log-file", type=Path,
                       help="also append JSON log lines to this file "
                            "(in addition to stderr)")
    _add_progress_flags(p_run)
    p_run.set_defaults(func=cmd_run)

    p_list = sub.add_parser(
        "list-sources",
        help="print registered source types",
        description="Print every source ``type:`` registered with @register, "
                    "one per line.",
    )
    p_list.set_defaults(func=cmd_list_sources)

    p_steps = sub.add_parser(
        "list-steps",
        help="enumerate forecast hours published for one (date, cycle)",
        description="Call Source.list_available_steps and print the sorted "
                    "step ints; works for any source backed by S3 or GCS XML "
                    "listing (currently aifs / gfs / hrrr).",
    )
    p_steps.add_argument("--source", required=True,
                         help="path to source YAML, or a bare name resolved "
                              "against config/sources/<name>.yaml")
    p_steps.add_argument("--date", default=None,
                         help="YYYYMMDD, today, or yesterday "
                              "(default: two days ago UTC)")
    p_steps.add_argument("--cycle", type=int, default=12,
                         help="UTC cycle (0/6/12/18, default: 12)")
    p_steps.add_argument("--json", action="store_true",
                         help="emit JSON (one array) instead of one int per line")
    p_steps.set_defaults(func=cmd_list_steps)

    p_vars = sub.add_parser(
        "list-variables",
        help="enumerate variables in one (date, cycle, step) sidecar",
        description="Fetch the sidecar index for one step and print every "
                    "distinct (param, levtype, levelist) triple in the order "
                    "they appear in the file. ``--date / --cycle / --step`` "
                    "all default to a reasonably recent published init "
                    "(two days ago, 12z, step 0) so a bare "
                    "``--source <name>`` is enough for quick inspection.",
    )
    p_vars.add_argument("--source", required=True,
                        help="path to source YAML, or a bare name resolved "
                             "against config/sources/<name>.yaml")
    p_vars.add_argument("--date", default=None,
                        help="YYYYMMDD, today, or yesterday "
                             "(default: two days ago UTC)")
    p_vars.add_argument("--cycle", type=int, default=12,
                        help="UTC cycle (0/6/12/18, default: 12)")
    p_vars.add_argument("--step", type=int, default=0,
                        help="forecast hour (e.g. 0, 6, 24, default: 0)")
    p_vars.add_argument("--json", action="store_true",
                        help="emit JSON array of "
                             "{param, levtype, levelist, level_desc, count}")
    p_vars.add_argument("--yaml", action="store_true",
                        help="emit a VariableGroup scaffold (one group per "
                             "levtype) ready to paste under 'variables:' in a "
                             "job YAML")
    p_vars.set_defaults(func=cmd_list_variables)

    p_s2s = sub.add_parser(
        "s2s",
        help="ECMWF Data Store sub-seasonal (S2S) downloads",
        description="Async submit-poll-download against ECDS s2s-forecasts. "
                    "Runs one cdsapi.retrieve per declared variable group; "
                    "credentials read from ~/.ecdsapirc.",
    )
    p_s2s.add_argument("--config", required=True, type=Path,
                       help="path to S2S job YAML "
                            "(e.g. config/jobs/s2s_renewables.yaml)")
    p_s2s.add_argument("--date",
                       help="override time.date "
                            "(YYYYMMDD, today, yesterday, or 'a,b' list)")
    p_s2s.add_argument("--cycle",
                       help="override time.cycle (0 or 12, or '0,12' list)")
    p_s2s.add_argument("--output-dir", type=Path,
                       help="override download.output_dir")
    p_s2s.add_argument("--init-concurrency", type=int,
                       help="override download.init_concurrency")
    p_s2s.add_argument("--log-file", type=Path,
                       help="also append JSON log lines to this file")
    _add_progress_flags(p_s2s)
    p_s2s.set_defaults(func=cmd_s2s)

    return parser


def cmd_run(args: argparse.Namespace) -> int:
    progress = _resolve_progress(args.progress)
    # When tqdm bars are active, keep INFO chatter (e.g. partial_download_start)
    # off stderr; --log-file, if given, still captures the full JSON trail.
    configure_logging(
        log_file=args.log_file,
        stderr_level=logging.WARNING if progress else logging.INFO,
    )
    log = structlog.get_logger("climate_download.cli")

    cfg = load_job(args.config)

    overrides: dict[str, object] = {}
    if args.date is not None:
        overrides["date"] = args.date
    if args.cycle is not None:
        overrides["cycle"] = args.cycle
    if args.steps is not None:
        overrides["steps"] = args.steps
    if overrides:
        merged = cfg.time.model_dump(mode="python") | overrides
        cfg.time = TimeConfig.model_validate(merged)

    if args.output_dir is not None:
        cfg.download.output_dir = args.output_dir
    if args.init_concurrency is not None:
        cfg.download.init_concurrency = args.init_concurrency
    cfg.download.progress_bar = progress

    steps_view = cfg.time.expanded_steps()
    log.info("job_loaded", config=str(args.config), source=cfg.source.name,
             variable_groups=[g.name for g in cfg.variables],
             dates=cfg.time.expanded_dates(),
             cycles=cfg.time.expanded_cycles(),
             steps=("all" if steps_view is None else steps_view),
             init_concurrency=cfg.download.init_concurrency,
             progress=cfg.download.progress_bar)

    outcome = run_job(cfg, config_path=str(args.config))
    for r in outcome.succeeded:
        log.info("job_step_summary",
                 date=r.date, cycle=r.cycle, step=r.step,
                 path=str(r.output_path), bytes_downloaded=r.bytes_downloaded,
                 records_selected=r.records_selected, records_total=r.records_total,
                 http_requests=r.http_requests, savings_pct=r.savings_pct,
                 resumed=r.resumed, breakdown=r.selected_breakdown)
    for f in outcome.failed:
        log.warning("job_failure",
                    date=f.date, cycle=f.cycle, step=f.step,
                    phase=f.phase, error=f.error)
    log.info("job_done",
             inits=len({(r.date, r.cycle) for r in outcome.succeeded}),
             succeeded=len(outcome.succeeded),
             failed=len(outcome.failed),
             report=str(outcome.report_path) if outcome.report_path else None)
    if outcome.all_failed:
        return 2
    if outcome.failed:
        return 1
    return 0


def cmd_s2s(args: argparse.Namespace) -> int:
    progress = _resolve_progress(args.progress)
    configure_logging(
        log_file=args.log_file,
        stderr_level=logging.WARNING if progress else logging.INFO,
    )
    log = structlog.get_logger("climate_download.cli.s2s")
    # Local imports keep cdsapi out of the import path until s2s is invoked,
    # so the rest of the CLI keeps working when [s2s] extra is not installed.
    from climate_download.s2s import load_s2s_job, run_s2s_job
    from climate_download.s2s.config import S2STimeConfig

    cfg = load_s2s_job(args.config)

    overrides: dict[str, object] = {}
    if args.date is not None:
        overrides["date"] = args.date
    if args.cycle is not None:
        s = args.cycle.strip()
        overrides["cycle"] = (
            [int(x.strip()) for x in s.split(",") if x.strip()]
            if "," in s else int(s)
        )
    if overrides:
        merged = cfg.time.model_dump(mode="python") | overrides
        cfg.time = S2STimeConfig.model_validate(merged)
    if args.output_dir is not None:
        cfg.download.output_dir = args.output_dir
    if args.init_concurrency is not None:
        cfg.download.init_concurrency = args.init_concurrency
    cfg.download.progress_bar = progress

    log.info("s2s_job_loaded", config=str(args.config),
             source=cfg.source.name, origin=cfg.source.origin,
             collection=cfg.source.collection,
             groups=[g.name for g in cfg.groups],
             dates=cfg.time.expanded_dates(),
             cycles=cfg.time.expanded_cycles(),
             progress=cfg.download.progress_bar)

    outcome = run_s2s_job(cfg, config_path=str(args.config))
    for r in outcome.succeeded:
        log.info("s2s_group_summary",
                 date=r.date, cycle=r.cycle, group=r.group,
                 path=str(r.output_path),
                 bytes_downloaded=r.bytes_downloaded,
                 elapsed_seconds=round(r.elapsed_seconds, 1),
                 resumed=r.resumed)
    for f in outcome.failed:
        log.warning("s2s_failure",
                    date=f.date, cycle=f.cycle, group=f.group,
                    phase=f.phase, error=f.error)
    log.info("s2s_job_done",
             inits=len({(r.date, r.cycle) for r in outcome.succeeded}),
             succeeded=len(outcome.succeeded),
             failed=len(outcome.failed),
             report=str(outcome.report_path) if outcome.report_path else None)
    if outcome.all_failed:
        return 2
    if outcome.failed:
        return 1
    return 0


def cmd_list_sources(_args: argparse.Namespace) -> int:
    for name in sorted(SOURCE_REGISTRY):
        cls = SOURCE_REGISTRY[name]
        module = getattr(cls, "__module__", "?")
        print(f"{name}\t{cls.__name__}\t{module}")
    return 0


def cmd_list_steps(args: argparse.Namespace) -> int:
    configure_logging()
    source = _resolve_source(args.source)
    if args.date is None:
        date = (dt.datetime.now(dt.UTC).date()
                - dt.timedelta(days=2)).strftime("%Y%m%d")
    else:
        date = _resolve_symbolic_date(args.date)
    with httpx.Client(timeout=30.0) as client:
        steps = source.list_available_steps(
            client, date=date, cycle=args.cycle,
        )
    if steps is None:
        print(
            f"source {source.name!r} does not implement listing "
            f"(or template host unrecognised)",
            file=sys.stderr,
        )
        return 2
    if args.json:
        print(json.dumps(steps))
    else:
        for s in steps:
            print(s)
    return 0


def cmd_list_variables(args: argparse.Namespace) -> int:
    configure_logging()
    if args.json and args.yaml:
        print("--json and --yaml are mutually exclusive", file=sys.stderr)
        return 2
    source = _resolve_source(args.source)
    # Resolve omitted/symbolic date to a concrete YYYYMMDD; default is two
    # days ago UTC, which is conservatively past upstream publish lag for
    # every built-in source (AIFS/IFS/GFS/HRRR all have the prior 00z/12z
    # run available by then).
    if args.date is None:
        date = (dt.datetime.now(dt.UTC).date()
                - dt.timedelta(days=2)).strftime("%Y%m%d")
    else:
        date = _resolve_symbolic_date(args.date)
    with httpx.Client(timeout=60.0) as client:
        variables = source.list_available_variables(
            client, date=date, cycle=args.cycle, step=args.step,
        )
    if args.json:
        payload = [
            {"param": v.param, "levtype": v.levtype,
             "levelist": v.levelist, "level_desc": v.level_desc,
             "count": v.count}
            for v in variables
        ]
        print(json.dumps(payload, indent=2))
    elif args.yaml:
        print(_render_variables_yaml(variables, source_name=source.name,
                                     date=date, cycle=args.cycle,
                                     step=args.step))
    else:
        print(f"# {len(variables)} distinct variables")
        print(f"{'param':<12} {'levtype':<8} {'level':<28} count")
        for v in variables:
            level = v.level_desc if v.level_desc is not None else (
                v.levelist if v.levelist is not None else "-"
            )
            print(f"{v.param:<12} {v.levtype:<8} {level:<28} {v.count}")
    return 0


# levtypes where collecting `levels:` from `levelist` makes sense; anything
# else (sfc / atm / other) gets emitted without a levels block so the user
# is forced to look at the descriptors and decide.
_LEVTYPES_WITH_LEVELS = frozenset({"pl", "ml", "hag", "hbg", "sol"})


def _render_variables_yaml(
    variables: list, *, source_name: str, date: str, cycle: int, step: int,
) -> str:
    """Render `variables` as a paste-ready VariableGroup YAML scaffold.

    Records are bucketed by ``levtype``; within each bucket params are
    de-duplicated (preserving discovery order) and ``levels`` is filled
    from the sorted union of ``levelist`` values when the levtype has a
    meaningful level scale (``pl`` / ``ml`` / ``hag`` / ``hbg`` / ``sol``).
    Descriptors (``level_desc``) are emitted as trailing comments so the
    user can sanity-check each entry against the source's wording.
    """
    by_levtype: dict[str, list] = {}
    for v in variables:
        by_levtype.setdefault(v.levtype, []).append(v)

    lines: list[str] = [
        f"# scaffold: {source_name} {date} cycle={cycle:02d}z step={step}h",
        f"# {len(variables)} distinct variables across {len(by_levtype)} levtypes",
        "variables:",
    ]
    for levtype in sorted(by_levtype):
        bucket = by_levtype[levtype]
        params: list[str] = []
        seen_params: set[str] = set()
        for v in bucket:
            if v.param not in seen_params:
                seen_params.add(v.param)
                params.append(v.param)
        lines.append(f"  - name: {source_name}_{levtype}")
        lines.append(f"    levtype: {levtype}")
        lines.append(f"    params: [{', '.join(params)}]")
        if levtype in _LEVTYPES_WITH_LEVELS:
            levels = sorted({v.levelist for v in bucket if v.levelist},
                            key=_levelist_sort_key)
            if levels:
                quoted = ", ".join(f'"{lv}"' for lv in levels)
                lines.append(f"    levels: [{quoted}]")
        descs = [v.level_desc for v in bucket if v.level_desc]
        if descs:
            sample = ", ".join(dict.fromkeys(descs))[:120]
            lines.append(f"    # level_desc: {sample}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _levelist_sort_key(level: str) -> tuple[int, float | str]:
    """Numeric sort when possible, alpha fallback for hbg-style ranges."""
    try:
        return (0, float(level))
    except ValueError:
        return (1, level)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
