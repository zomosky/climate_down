"""Source adapter contract.

Each upstream data source (ECMWF AIFS, NOAA GFS, NOAA HRRR, ...) lives in
its own module under :mod:`climate_download.sources` and exposes a class
that satisfies the :class:`Source` protocol. The orchestrator in
:mod:`climate_download.jobs` only ever calls protocol methods, so adding a
new source means writing one file plus registering it; no edits to
``config.py`` or ``jobs.py`` are required.

Two extension levels are supported:

* **Index sources** (the common case): override ``build_index_url`` /
  ``build_data_url`` / ``fetch_records`` and inherit the default
  ``download_step`` which performs HTTP byte-range downloads via
  :class:`PartialDownloader`.
* **Whole-file sources** (NetCDF over OPeNDAP, BUFR snapshots, ...):
  also override ``download_step`` to bypass byte-range downloading
  entirely.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable

import httpx

from climate_download.grib.index import IndexRecord, merge_ranges
from climate_download.grib.partial import PartialDownloader
from climate_download.sources._http import request_with_retry

__all__ = ["BaseSource", "Source", "StepDownloadResult", "VariableInfo"]


@dataclass(slots=True)
class StepDownloadResult:
    """What :meth:`Source.download_step` returns to the orchestrator."""

    output_path: Path
    bytes_downloaded: int
    http_requests: int


@dataclass(frozen=True, slots=True)
class VariableInfo:
    """One distinct ``(param, levtype, levelist)`` triple available in a step.

    ``count`` is the number of GRIB messages in the index that share the
    triple — almost always 1 for AIFS / GFS, but useful when a level family
    spans several entries (e.g. ensemble members in future sources).

    ``level_desc`` is the source's own human-readable level descriptor when
    one exists (e.g. wgrib2's ``"2 m above ground"`` / ``"0-0.1 m below
    ground"`` / ``"mean sea level"``). ECMWF ``.index`` files do not ship
    such a string, so AIFS / IFS leave it ``None``.
    """

    param: str
    levtype: str
    levelist: str | None
    count: int = 1
    level_desc: str | None = None


@runtime_checkable
class Source(Protocol):
    """Protocol every source adapter implements.

    Attributes
    ----------
    name
        Stable identifier used in filenames, manifests and logs.
    description
        Free-form human label.
    supports_byte_range
        Hint for future fallback behaviour; not used for routing today.
    """

    name: str
    description: str | None
    supports_byte_range: bool

    def build_index_url(self, *, date: str, cycle: int, step: int) -> str: ...

    def build_data_url(self, *, date: str, cycle: int, step: int) -> str: ...

    def probe_step(
        self, client: httpx.Client, *, date: str, cycle: int, step: int
    ) -> bool: ...

    def list_available_steps(
        self, client: httpx.Client, *, date: str, cycle: int
    ) -> list[int] | None: ...

    def fetch_records(
        self, client: httpx.Client, *, date: str, cycle: int, step: int
    ) -> list[IndexRecord]: ...

    def list_available_variables(
        self, client: httpx.Client, *, date: str, cycle: int, step: int
    ) -> list[VariableInfo]: ...

    def download_step(
        self,
        downloader: PartialDownloader,
        *,
        records: list[IndexRecord],
        output_path: Path,
        gap_tolerance: int,
        date: str,
        cycle: int,
        step: int,
    ) -> StepDownloadResult: ...


class BaseSource:
    """Default implementations for the byte-range index-sidecar pattern.

    Concrete sources mix this in alongside ``pydantic.BaseModel``; the
    pydantic side carries the per-source schema (URL templates, etc.) while
    this side supplies behaviour every byte-range source shares.
    """

    name: str
    description: str | None
    supports_byte_range: bool

    def probe_step(
        self, client: httpx.Client, *, date: str, cycle: int, step: int
    ) -> bool:
        url = self.build_index_url(date=date, cycle=cycle, step=step)  # type: ignore[attr-defined]
        resp = request_with_retry(client, "HEAD", url, follow_redirects=True)
        if resp.status_code == 404:
            return False
        resp.raise_for_status()
        return True

    def list_available_steps(
        self, client: httpx.Client, *, date: str, cycle: int
    ) -> list[int] | None:
        """Enumerate every forecast step published for one (date, cycle).

        Returning ``None`` means the source does not support listing — the
        orchestrator will then log a warning and skip the init when the job
        asks for ``steps: all``. Sources backed by S3-compatible buckets can
        delegate to :func:`climate_download.sources._listing.list_remote_steps`.
        """
        return None

    def list_available_variables(
        self, client: httpx.Client, *, date: str, cycle: int, step: int
    ) -> list[VariableInfo]:
        """Enumerate distinct ``(param, levtype, levelist)`` triples in one step.

        The default implementation calls :meth:`fetch_records` and projects
        the result; ordering follows first-occurrence in the sidecar so the
        natural variable grouping is preserved. Sources that store metadata
        differently (e.g. NetCDF) should override.
        """
        records = self.fetch_records(client, date=date, cycle=cycle, step=step)  # type: ignore[attr-defined]
        counts: dict[tuple[str, str, str | None], int] = {}
        descs: dict[tuple[str, str, str | None], str | None] = {}
        order: list[tuple[str, str, str | None]] = []
        for rec in records:
            key = (rec.param, rec.levtype, rec.levelist)
            if key not in counts:
                counts[key] = 0
                descs[key] = getattr(rec, "level_desc", None)
                order.append(key)
            counts[key] += 1
        return [
            VariableInfo(
                param=p, levtype=t, levelist=l,
                count=counts[(p, t, l)], level_desc=descs[(p, t, l)],
            )
            for (p, t, l) in order
        ]

    def download_step(
        self,
        downloader: PartialDownloader,
        *,
        records: list[IndexRecord],
        output_path: Path,
        gap_tolerance: int,
        date: str,
        cycle: int,
        step: int,
    ) -> StepDownloadResult:
        ranges = merge_ranges(records, gap_tolerance=gap_tolerance)
        url = self.build_data_url(date=date, cycle=cycle, step=step)  # type: ignore[attr-defined]
        written = downloader.download(url, ranges, output_path)
        return StepDownloadResult(
            output_path=output_path,
            bytes_downloaded=written,
            http_requests=len(ranges),
        )
