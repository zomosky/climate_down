"""YAML-driven configuration models for S2S download jobs.

A job declares:

* one source (which centre + collection — see :mod:`.source`),
* one or more *variable groups* (each = one ``retrieve`` call to ECDS,
  pinned to a single ``level_type`` and ``leadtime_kind``),
* a ``time`` block (init dates / cycles / leadtime range),
* a ``download`` block (output layout + cdsapi behaviour).

Each variable group becomes its own GRIB file under
``{output_dir}/{source.name}/{date}/{cycle:02d}z/{group.name}.grib2`` so the
output tree mirrors the byte-range sources' layout while honouring S2S's
"one retrieve, many messages" reality.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from climate_download.config import _resolve_symbolic_date  # type: ignore[reportPrivateUsage]
from climate_download.s2s.source import S2SSource

__all__ = [
    "S2SDownloadConfig",
    "S2SJobConfig",
    "S2SLeadtimeRange",
    "S2STimeConfig",
    "S2SVariableGroup",
    "load_s2s_job",
    "load_s2s_source",
]


class S2SLeadtimeRange(BaseModel):
    """Inclusive leadtime range in hours.

    Step granularity must match the ECDS leadtime grid for the chosen
    ``leadtime_kind``: the instantaneous + accumulated grid is 6h-spaced
    while daily-averaged uses 12h-spaced 24h windows. The range here only
    fixes endpoints + step; the orchestrator renders the actual API values.
    """

    model_config = ConfigDict(extra="forbid")

    start: int = 0
    end: int = 1104
    step: int = 6

    @model_validator(mode="after")
    def _check(self) -> "S2SLeadtimeRange":
        if self.start < 0:
            raise ValueError("leadtime.start must be >= 0")
        if self.end < self.start:
            raise ValueError("leadtime.end must be >= leadtime.start")
        if self.step <= 0:
            raise ValueError("leadtime.step must be > 0")
        return self


class S2SVariableGroup(BaseModel):
    """One retrieve call: a level_type × leadtime_kind × variable list bundle.

    The S2S form requires a single ``level_type`` per request and forces
    daily-averaged variables onto a different ``leadtime_hour`` grid; the
    cleanest mapping is therefore "one group = one request".
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    level_type: Literal["single_level", "pressure", "isentropic", "oceanic"]
    # 'instant' covers both Instantaneous and Accumulated form groups (they
    # share the 6h-spaced leadtime grid); 'daily' = Daily averaged group.
    leadtime_kind: Literal["instant", "daily"] = "instant"
    variables: list[str] = Field(min_length=1)
    # Required when ``level_type`` is pressure or isentropic; omit otherwise.
    levels: list[str] | None = None
    # Optional per-group override of the job-level leadtime range.
    leadtime: S2SLeadtimeRange | None = None

    @field_validator("levels", mode="before")
    @classmethod
    def _stringify_levels(cls, v: Any) -> Any:
        if v is None:
            return v
        return [str(x) for x in v]

    @model_validator(mode="after")
    def _check_levels(self) -> "S2SVariableGroup":
        needs_levels = self.level_type in ("pressure", "isentropic")
        if needs_levels and not self.levels:
            raise ValueError(
                f"variable group {self.name!r}: level_type={self.level_type} "
                f"requires a non-empty 'levels' list (e.g. ['925', '1000'])"
            )
        if not needs_levels and self.levels:
            raise ValueError(
                f"variable group {self.name!r}: 'levels' must be omitted when "
                f"level_type={self.level_type}"
            )
        return self


class S2STimeConfig(BaseModel):
    """When to fetch.

    ``date`` accepts a single string or a list (each YYYYMMDD or
    today/yesterday); the symbolic forms resolve against UTC at expansion
    time. ``cycle`` is restricted to the two values ECDS publishes for S2S.
    The default ``leadtime`` covers the ECMWF 46-day window every 6h.
    """

    model_config = ConfigDict(extra="forbid")

    date: str | list[str] = "yesterday"
    cycle: Literal[0, 12] | list[Literal[0, 12]] = 0
    leadtime: S2SLeadtimeRange = Field(default_factory=S2SLeadtimeRange)

    @field_validator("date", mode="before")
    @classmethod
    def _coerce_date(cls, v: Any) -> Any:
        if isinstance(v, str) and "," in v:
            return [x.strip() for x in v.split(",") if x.strip()]
        if isinstance(v, list):
            return [str(x).strip() for x in v]
        return v

    def expanded_dates(self) -> list[str]:
        if isinstance(self.date, list):
            return [_resolve_symbolic_date(d) for d in self.date]
        return [_resolve_symbolic_date(self.date)]

    def expanded_cycles(self) -> list[int]:
        if isinstance(self.cycle, list):
            return list(self.cycle)
        return [self.cycle]


class S2SDownloadConfig(BaseModel):
    """Output location and cdsapi behaviour.

    The default layout is ``{output_dir}/{source.name}/{date}/{cycle:02d}z/``
    with one file per variable group named ``{group}.grib2``. The init-level
    manifest sits beside the GRIBs. Group concurrency stays serial per init
    (cdsapi jobs share the same queue and run faster sequentially), but
    multiple inits can run in parallel via ``init_concurrency``.
    """

    model_config = ConfigDict(extra="forbid")

    output_dir: Path = Path("output")
    subdir_template: str = "{source}/{date}/{cycle:02d}z"
    filename_template: str = "{group}.grib2"
    init_concurrency: int = Field(default=1, ge=1)
    request_timeout_seconds: float = 7200.0
    progress_bar: bool = False


class S2SJobConfig(BaseModel):
    """Full S2S job: source + variable groups + time + download knobs."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    source: S2SSource
    groups: list[S2SVariableGroup]
    time: S2STimeConfig = Field(default_factory=S2STimeConfig)
    download: S2SDownloadConfig = Field(default_factory=S2SDownloadConfig)

    @model_validator(mode="before")
    @classmethod
    def _coerce_source(cls, data: Any) -> Any:
        if isinstance(data, dict):
            src = data.get("source")
            if isinstance(src, dict):
                payload = {k: v for k, v in src.items() if k != "type"}
                data = {**data, "source": S2SSource.model_validate(payload)}
        return data

    @model_validator(mode="after")
    def _check_groups(self) -> "S2SJobConfig":
        if not self.groups:
            raise ValueError("at least one variable group is required")
        names = [g.name for g in self.groups]
        if len(set(names)) != len(names):
            raise ValueError(f"duplicate group names: {names}")
        return self


def _load_yaml(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"{path}: top-level YAML must be a mapping")
    return data


def load_s2s_source(path: str | Path) -> S2SSource:
    """Load a stand-alone S2S source YAML (``config/sources/s2s_*.yaml``)."""
    raw = _load_yaml(path)
    payload = {k: v for k, v in raw.items() if k != "type"}
    return S2SSource.model_validate(payload)


def load_s2s_job(
    path: str | Path,
    *,
    sources_dir: str | Path | None = None,
) -> S2SJobConfig:
    """Load an S2S job YAML.

    ``source`` may either be inlined as a mapping (with ``type: s2s``) or
    given as a string referring to ``{sources_dir}/{name}.yaml``.
    """
    raw = _load_yaml(path)
    src = raw.get("source")
    if isinstance(src, str):
        if sources_dir is None:
            sources_dir = Path(path).parent.parent / "sources"
        raw["source"] = _load_yaml(Path(sources_dir) / f"{src}.yaml")
    return S2SJobConfig.model_validate(raw)
