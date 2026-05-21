"""YAML-driven configuration models for download jobs.

A job file references one source plus business-level selectors (variables,
time, download knobs). Source instances are produced by the per-source
adapters in :mod:`climate_download.sources`; this module only handles the
generic glue (variable groups, time expansion, download knobs, YAML I/O).
"""

from __future__ import annotations

import datetime as dt
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from climate_download.sources import Source, get_source

__all__ = [
    "DateRange",
    "DownloadConfig",
    "JobConfig",
    "StepRange",
    "TimeConfig",
    "VariableGroup",
    "load_job",
    "load_source",
    "load_source_dict",
    "resolve_time",
]


def load_source_dict(raw: dict[str, Any]) -> Source:
    """Instantiate a source adapter from a parsed YAML mapping.

    The mapping must contain a ``type`` key naming a registered adapter
    (see :func:`climate_download.sources.list_sources`). All remaining keys
    are forwarded to the adapter's pydantic schema, so per-source field
    sets stay independent and a typo surfaces as a normal validation error.
    """
    if "type" not in raw:
        raise ValueError(
            "source mapping must declare 'type: <name>' "
            "(e.g. 'type: aifs', 'type: gfs', 'type: hrrr')"
        )
    payload = {k: v for k, v in raw.items() if k != "type"}
    cls = get_source(raw["type"])
    return cls.model_validate(payload)  # type: ignore[attr-defined]


class VariableGroup(BaseModel):
    """A subset of GRIB messages, expressed by levtype + params (+ levels)."""

    model_config = ConfigDict(extra="forbid")

    name: str
    levtype: str
    params: list[str]
    levels: list[str] | None = None

    @field_validator("levels", mode="before")
    @classmethod
    def _stringify_levels(cls, v: Any) -> Any:
        if v is None:
            return v
        return [str(x) for x in v]


class DateRange(BaseModel):
    """Inclusive date range; ``start``/``end`` are YYYYMMDD or today/yesterday."""

    model_config = ConfigDict(extra="forbid")

    start: str
    end: str

    @model_validator(mode="after")
    def _check(self) -> "DateRange":
        # Resolve symbolic values so ordering is checked against real dates.
        s = dt.datetime.strptime(_resolve_symbolic_date(self.start), "%Y%m%d").date()
        e = dt.datetime.strptime(_resolve_symbolic_date(self.end), "%Y%m%d").date()
        if e < s:
            raise ValueError(f"date range end {self.end} < start {self.start}")
        return self


class StepRange(BaseModel):
    """Inclusive forecast-step range in hours."""

    model_config = ConfigDict(extra="forbid")

    start: int
    end: int
    step: int = 6

    @model_validator(mode="after")
    def _check(self) -> "StepRange":
        if self.step <= 0:
            raise ValueError("steps.step must be > 0")
        if self.end < self.start:
            raise ValueError("steps.end must be >= steps.start")
        return self


def _resolve_symbolic_date(s: str) -> str:
    raw = s.strip().lower()
    today = dt.datetime.now(dt.UTC).date()
    if raw == "today":
        return today.strftime("%Y%m%d")
    if raw == "yesterday":
        return (today - dt.timedelta(days=1)).strftime("%Y%m%d")
    # Validate format by round-tripping through strptime.
    return dt.datetime.strptime(s.strip(), "%Y%m%d").strftime("%Y%m%d")


def _expand_date_range(start: str, end: str) -> list[str]:
    s = dt.datetime.strptime(_resolve_symbolic_date(start), "%Y%m%d").date()
    e = dt.datetime.strptime(_resolve_symbolic_date(end), "%Y%m%d").date()
    if e < s:
        raise ValueError(f"date_range end {end} < start {start}")
    return [
        (s + dt.timedelta(days=i)).strftime("%Y%m%d")
        for i in range((e - s).days + 1)
    ]


class TimeConfig(BaseModel):
    """When to fetch.

    Each of ``date``, ``cycle`` and ``steps`` accepts three shapes:

    * single value: ``date: 20260507`` / ``cycle: 0`` / ``steps: 6``
    * explicit list: ``date: [20260507, 20260508]`` / ``cycle: [0, 12]``
    * range, written as a mapping or a short string:

      - ``date: { start: 20260501, end: 20260507 }`` or ``"20260501-20260507"``
      - ``steps: { start: 0, end: 120, step: 6 }`` or ``"0-120:6"`` /
        MARS-style ``"0/120/6"``

    ``today`` / ``yesterday`` are resolved against UTC at expansion time.
    """

    model_config = ConfigDict(extra="forbid")

    date: str | list[str] | DateRange = "yesterday"
    cycle: int | list[int] = 0
    steps: int | list[int] | StepRange | str = Field(default_factory=lambda: [0])

    @field_validator("date", mode="before")
    @classmethod
    def _coerce_date(cls, v: Any) -> Any:
        if isinstance(v, str):
            s = v.strip()
            m = re.fullmatch(r"(\d{8})\s*-\s*(\d{8})", s)
            if m:
                return {"start": m.group(1), "end": m.group(2)}
            if "," in s:
                return [x.strip() for x in s.split(",") if x.strip()]
        elif isinstance(v, list):
            return [str(x).strip() for x in v]
        return v

    @field_validator("cycle", mode="before")
    @classmethod
    def _coerce_cycle(cls, v: Any) -> Any:
        if isinstance(v, str):
            s = v.strip()
            if "," in s:
                return [int(x.strip()) for x in s.split(",") if x.strip()]
            return int(s)
        return v

    @field_validator("steps", mode="before")
    @classmethod
    def _coerce_steps(cls, v: Any) -> Any:
        if isinstance(v, str):
            s = v.strip()
            if s.lower() == "all":
                return "all"
            m = re.fullmatch(r"(-?\d+)\s*/\s*(-?\d+)\s*/\s*(\d+)", s)
            if m:
                return {
                    "start": int(m.group(1)),
                    "end": int(m.group(2)),
                    "step": int(m.group(3)),
                }
            m = re.fullmatch(r"(-?\d+)\s*-\s*(-?\d+)(?:\s*:\s*(\d+))?", s)
            if m:
                return {
                    "start": int(m.group(1)),
                    "end": int(m.group(2)),
                    "step": int(m.group(3)) if m.group(3) else 6,
                }
            if "," in s:
                return [int(x.strip()) for x in s.split(",") if x.strip()]
            return int(s)
        return v

    @model_validator(mode="after")
    def _check_cycles(self) -> "TimeConfig":
        for c in self.expanded_cycles():
            if c not in (0, 6, 12, 18):
                raise ValueError(
                    f"cycle must be one of 0, 6, 12, 18 (UTC); got {c}"
                )
        return self

    def expanded_dates(self) -> list[str]:
        if isinstance(self.date, DateRange):
            return _expand_date_range(self.date.start, self.date.end)
        if isinstance(self.date, list):
            return [_resolve_symbolic_date(d) for d in self.date]
        return [_resolve_symbolic_date(self.date)]

    def expanded_cycles(self) -> list[int]:
        if isinstance(self.cycle, list):
            return list(self.cycle)
        return [self.cycle]

    def expanded_steps(self) -> list[int] | None:
        """Return the requested step list, or ``None`` for ``steps: all``.

        ``None`` signals the orchestrator to enumerate available steps via
        :meth:`Source.list_available_steps` per (date, cycle); when the
        source does not implement listing the init logs a warning and is
        skipped.
        """
        if isinstance(self.steps, str):
            if self.steps == "all":
                return None
            raise ValueError(f"unexpected steps string: {self.steps!r}")
        if isinstance(self.steps, StepRange):
            return list(
                range(self.steps.start, self.steps.end + 1, self.steps.step)
            )
        if isinstance(self.steps, list):
            return list(self.steps)
        return [self.steps]


class DownloadConfig(BaseModel):
    """Output location and HTTP behaviour.

    The default layout is ``{output_dir}/{source}/{date}/{cycle:02d}z/f{step:03d}.subset.grib2``
    so every job — past, present and future — drops files into the same
    hierarchical tree (one subtree per source / init time). Jobs can still
    override ``subdir_template`` / ``filename_template`` when downstream
    consumers expect a different path, but the default is intentionally
    opinionated so concurrent jobs against different sources never collide.
    """

    model_config = ConfigDict(extra="forbid")

    output_dir: Path = Path("output")
    subdir_template: str = "{source}/{date}/{cycle:02d}z"
    filename_template: str = "f{step:03d}.subset.grib2"
    workers: int = 4
    gap_tolerance: int = 0
    timeout_seconds: float = 120.0
    max_attempts: int = 4
    init_concurrency: int = Field(default=2, ge=1)
    progress_bar: bool = False


class JobConfig(BaseModel):
    """Full download job: source + variable groups + time + download knobs."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    source: Source
    variables: list[VariableGroup]
    time: TimeConfig = Field(default_factory=TimeConfig)
    download: DownloadConfig = Field(default_factory=DownloadConfig)

    @model_validator(mode="before")
    @classmethod
    def _coerce_source(cls, data: Any) -> Any:
        # Allow YAML dicts to flow in directly: dispatch to the registered
        # adapter via the 'type' discriminator. Already-instantiated Source
        # objects (e.g. from programmatic use) pass through unchanged.
        if isinstance(data, dict):
            src = data.get("source")
            if isinstance(src, dict):
                data = {**data, "source": load_source_dict(src)}
        return data

    @model_validator(mode="after")
    def _check_variables(self) -> "JobConfig":
        if not self.variables:
            raise ValueError("at least one variable group is required")
        names = [g.name for g in self.variables]
        if len(set(names)) != len(names):
            raise ValueError(f"duplicate variable group names: {names}")
        return self


def _load_yaml(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"{path}: top-level YAML must be a mapping")
    return data


def load_source(path: str | Path) -> Source:
    """Load a stand-alone source YAML (``config/sources/*.yaml``).

    The YAML must declare ``type: <registered-name>``; the rest of the
    fields are forwarded to that adapter's pydantic schema.
    """
    return load_source_dict(_load_yaml(path))


def load_job(
    path: str | Path,
    *,
    sources_dir: str | Path | None = None,
) -> JobConfig:
    """Load a job YAML.

    ``source`` may either be inlined as a mapping (with ``type:``) or given
    as a ``source: <name>`` string referring to ``{sources_dir}/{name}.yaml``.
    """
    raw = _load_yaml(path)
    src = raw.get("source")
    if isinstance(src, str):
        if sources_dir is None:
            sources_dir = Path(path).parent.parent / "sources"
        raw["source"] = _load_yaml(Path(sources_dir) / f"{src}.yaml")
    return JobConfig.model_validate(raw)


def resolve_time(time_cfg: TimeConfig) -> tuple[str, int, list[int] | None]:
    """Backward-compatible single-init view of ``time_cfg``.

    Returns the *first* expanded ``(date, cycle)`` plus the full step list
    (``None`` when ``steps: all``). Prefer :meth:`TimeConfig.expanded_dates`
    / ``expanded_cycles`` / :meth:`expanded_steps` for new code that needs
    to iterate every init.
    """
    dates = time_cfg.expanded_dates()
    cycles = time_cfg.expanded_cycles()
    return dates[0], cycles[0], time_cfg.expanded_steps()
