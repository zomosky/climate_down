"""DWD ICON open-data adapter (one bz2 GRIB file per variable, no sidecar).

DWD's open-data server is structurally unlike the ECMWF / NOAA sources:

* **No sidecar index.** There is no ``.index`` / ``.idx`` listing byte offsets,
  because DWD does not pack a whole forecast hour into one GRIB. Instead it
  publishes **one file per variable per step** (per level, for multi-level
  fields), each ``bzip2``-compressed::

      icon/grib/{cc}/{var}/icon_global_icosahedral_single-level_{YYYYMMDD}{cc}_{sss}_{VAR}.grib2.bz2
      icon/grib/{cc}/{var}/icon_global_icosahedral_model-level_{YYYYMMDD}{cc}_{sss}_{lll}_{VAR}.grib2.bz2

  Variable *selection* is therefore choosing which **files** to fetch, not which
  byte ranges inside one file — so this adapter overrides ``download_step`` to
  bypass :class:`PartialDownloader` entirely (the whole-file path documented in
  :mod:`climate_download.sources.base`). It downloads each selected file,
  ``bz2``-decompresses it, and concatenates the messages in selection order
  into one multi-message GRIB2 — the same per-step artifact every other source
  produces, so the manifest / resume / downstream contract is unchanged.

* **Static catalogue instead of a fetched index.** Because the file layout is
  fully deterministic, there is nothing to fetch before selecting. The set of
  fields the source *offers* lives in :data:`DEFAULT_ICON_RENEWABLE_CATALOG`
  (curated for PV / wind / power-trading, matching the other ``*_renewables``
  jobs); a job YAML's ``variables:`` groups then select a subset by ``param``
  exactly as for GFS / AIFS. Override it per source via a ``catalog:`` block.

* **Icosahedral grid.** ``icon`` (global) ships on the native icosahedral grid,
  NOT lat/lon — so ``clat`` / ``clon`` coordinate arrays are needed to regrid,
  and the downstream China-bbox crop in ``climate_restorage`` will not work
  unmodified. ``base_url`` + ``grid`` are configurable so the same adapter also
  drives the regular-lat-lon regional variants (``icon-eu`` / ``icon-d2``).

* **Near-real-time only.** DWD retains only the latest run or two per cycle, so
  historical backfill is not possible here (unlike the S3/GCS mirrors).
"""

from __future__ import annotations

import bz2
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import httpx
import structlog
from pydantic import BaseModel, ConfigDict, Field

from climate_download.grib.index import IndexRecord
from climate_download.grib.partial import PartialDownloader
from climate_download.sources._http import request_with_retry
from climate_download.sources.base import BaseSource, StepDownloadResult
from climate_download.sources.registry import register

__all__ = ["DEFAULT_ICON_RENEWABLE_CATALOG", "IconSource", "IconVariable"]

_log = structlog.get_logger(__name__)


class IconVariable(BaseModel):
    """One field DWD publishes as its own ``.grib2.bz2`` file.

    ``name`` is the DWD token as it appears in the path and filename — the
    directory is its lower-case form (``u_10m``) and the filename suffix its
    upper-case form (``U_10M``); both are derived from this single value, so a
    catalogue entry is just the token plus how to classify it.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    levtype: str = "single"
    levels: list[str] | None = None
    level_desc: str | None = None

    @property
    def is_multi_level(self) -> bool:
        return bool(self.levels)


# Renewable-energy / power-trading single-level subset for DWD ICON, curated to
# mirror the GFS / IFS / AIFS ``*_renewables`` jobs. Names are DWD's own GRIB
# shortNames (kept native, as with every source — restorage renames per source).
#   * 10 m wind + gusts (hub-height 80/100 m is NOT a single-level ICON field)
#   * 2 m temperature / dewpoint / ground temperature + 2 m relative humidity
#   * surface + mean-sea-level pressure
#   * PV radiation: direct + diffuse downward shortwave (GHI = ASWDIR_S+ASWDIFD_S),
#     net shortwave/longwave at the surface — all time-averaged, ~0 at step 0
#   * total + low/mid/high cloud cover (PV variability)
#   * accumulated precipitation, CAPE, total column water vapour, freezing level
DEFAULT_ICON_RENEWABLE_CATALOG: list[IconVariable] = [
    # ── Wind ──────────────────────────────────────────────────────
    IconVariable(name="U_10M", levtype="single", level_desc="10 m above ground"),
    IconVariable(name="V_10M", levtype="single", level_desc="10 m above ground"),
    IconVariable(name="VMAX_10M", levtype="single", level_desc="10 m gust"),
    # ── Temperature & humidity ────────────────────────────────────
    IconVariable(name="T_2M", levtype="single", level_desc="2 m above ground"),
    IconVariable(name="TD_2M", levtype="single", level_desc="2 m dew point"),
    IconVariable(name="T_G", levtype="single", level_desc="surface / ground"),
    IconVariable(name="RELHUM_2M", levtype="single", level_desc="2 m relative humidity"),
    # ── Pressure ──────────────────────────────────────────────────
    IconVariable(name="PS", levtype="single", level_desc="surface pressure"),
    IconVariable(name="PMSL", levtype="single", level_desc="mean sea level"),
    # ── Radiation (PV) ────────────────────────────────────────────
    IconVariable(name="ASWDIR_S", levtype="single", level_desc="avg direct SW down sfc"),
    IconVariable(name="ASWDIFD_S", levtype="single", level_desc="avg diffuse SW down sfc"),
    IconVariable(name="ASWDIFU_S", levtype="single", level_desc="avg diffuse SW up sfc"),
    IconVariable(name="ASOB_S", levtype="single", level_desc="avg net SW sfc"),
    IconVariable(name="ATHB_S", levtype="single", level_desc="avg net LW sfc"),
    # ── Cloud cover ───────────────────────────────────────────────
    IconVariable(name="CLCT", levtype="single", level_desc="total cloud cover"),
    IconVariable(name="CLCL", levtype="single", level_desc="low cloud cover"),
    IconVariable(name="CLCM", levtype="single", level_desc="mid cloud cover"),
    IconVariable(name="CLCH", levtype="single", level_desc="high cloud cover"),
    # ── Precip / convection / moisture ────────────────────────────
    IconVariable(name="TOT_PREC", levtype="single", level_desc="total precip (accum)"),
    IconVariable(name="CAPE_ML", levtype="single", level_desc="CAPE mixed layer"),
    IconVariable(name="TQV", levtype="single", level_desc="total column water vapour"),
    IconVariable(name="HZEROCL", levtype="single", level_desc="freezing-level height"),
]


@register("icon")
class IconSource(BaseSource, BaseModel):
    """Adapter for DWD ICON open data (one bz2 file per variable, no sidecar).

    ``base_url`` and ``grid`` are the only parts that differ between ICON
    variants, so the same class serves global icosahedral data and the regular
    lat/lon regional models via different source YAMLs:

    * global:  ``base_url=.../icon/grib``     ``grid=icon_global_icosahedral``
    * icon-eu: ``base_url=.../icon-eu/grib``  ``grid=icon-eu_europe_regular-lat-lon``
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    description: str | None = None
    base_url: str = "https://opendata.dwd.de/weather/nwp/icon/grib"
    grid: str = "icon_global_icosahedral"
    catalog: list[IconVariable] = Field(
        default_factory=lambda: list(DEFAULT_ICON_RENEWABLE_CATALOG)
    )
    probe_var: str = "T_2M"
    workers: int = 6
    timeout_seconds: float = 120.0
    supports_byte_range: bool = False

    # --- URL construction --------------------------------------------------

    def _single_url(self, token: str, *, date: str, cycle: int, step: int) -> str:
        return (
            f"{self.base_url}/{cycle:02d}/{token.lower()}/"
            f"{self.grid}_single-level_{date}{cycle:02d}_{step:03d}_"
            f"{token.upper()}.grib2.bz2"
        )

    def _multi_url(
        self, token: str, leveltype: str, level: str,
        *, date: str, cycle: int, step: int,
    ) -> str:
        return (
            f"{self.base_url}/{cycle:02d}/{token.lower()}/"
            f"{self.grid}_{leveltype}-level_{date}{cycle:02d}_{step:03d}_{level}_"
            f"{token.upper()}.grib2.bz2"
        )

    def _record_url(
        self, record: IndexRecord, *, date: str, cycle: int, step: int
    ) -> str:
        if record.levelist is None:
            return self._single_url(record.param, date=date, cycle=cycle, step=step)
        leveltype = "pressure" if record.levtype == "pressure" else "model"
        return self._multi_url(
            record.param, leveltype, record.levelist,
            date=date, cycle=cycle, step=step,
        )

    def build_index_url(self, *, date: str, cycle: int, step: int) -> str:
        # DWD has no sidecar; return the run directory for human-readable logs.
        return f"{self.base_url}/{cycle:02d}/"

    def build_data_url(self, *, date: str, cycle: int, step: int) -> str:
        # No single per-step file exists; the probe variable's file is the most
        # representative URL (used only for logging / the base probe fallback).
        return self._single_url(self.probe_var, date=date, cycle=cycle, step=step)

    # --- Availability ------------------------------------------------------

    def probe_step(
        self, client: httpx.Client, *, date: str, cycle: int, step: int
    ) -> bool:
        url = self._single_url(self.probe_var, date=date, cycle=cycle, step=step)
        resp = request_with_retry(client, "HEAD", url, follow_redirects=True)
        if resp.status_code == 404:
            return False
        resp.raise_for_status()
        return True

    def list_available_steps(
        self, client: httpx.Client, *, date: str, cycle: int
    ) -> list[int] | None:
        """Parse the probe variable's Apache autoindex page for its steps.

        DWD is plain directory HTML (not S3/GCS XML), so the shared
        :mod:`_listing` helper does not apply. We scrape the one directory we
        know exists for every init (the probe variable) and keep only steps
        whose filename carries the requested ``(date, cycle)``.
        """
        dir_url = f"{self.base_url}/{cycle:02d}/{self.probe_var.lower()}/"
        resp = request_with_retry(client, "GET", dir_url)
        resp.raise_for_status()
        token = self.probe_var.upper()
        pattern = re.compile(
            rf"{re.escape(self.grid)}_single-level_{date}{cycle:02d}_"
            rf"(\d+)_{re.escape(token)}\.grib2\.bz2"
        )
        steps = sorted({int(m) for m in pattern.findall(resp.text)})
        return steps or None

    # --- Selection ---------------------------------------------------------

    def fetch_records(
        self, client: httpx.Client, *, date: str, cycle: int, step: int
    ) -> list[IndexRecord]:
        """Project the static catalogue into one record per (var, level).

        Offsets/lengths are placeholders (this is not a byte-range source);
        ``download_step`` keys off ``param`` / ``levtype`` / ``levelist`` to
        rebuild each file URL. Selection in :mod:`climate_download.jobs` then
        works identically to the sidecar sources.
        """
        records: list[IndexRecord] = []
        for var in self.catalog:
            levels = var.levels if var.is_multi_level else [None]
            for level in levels:
                records.append(IndexRecord.model_validate({
                    "param": var.name,
                    "levtype": var.levtype,
                    "levelist": str(level) if level is not None else None,
                    "level_desc": var.level_desc,
                    "step": str(step),
                    "date": date,
                    "_offset": 0,
                    "_length": 0,
                }))
        return records

    # --- Whole-file download ----------------------------------------------

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
        """Fetch + bz2-decompress each selected file, concatenate into one GRIB.

        ``downloader`` (the byte-range helper) is intentionally unused — DWD
        files are whole, separate objects. Files are fetched concurrently but
        written back in ``records`` order so the output is deterministic. A
        per-file 404 is skipped with a warning (a field may be absent at some
        steps); the step fails only if *nothing* downloaded.
        """
        if not records:
            raise ValueError("download_step requires at least one record")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        urls = [
            self._record_url(r, date=date, cycle=cycle, step=step)
            for r in records
        ]
        payloads: dict[int, bytes] = {}
        compressed_bytes = 0

        with httpx.Client(timeout=self.timeout_seconds) as client:
            workers = max(1, min(self.workers, len(urls)))
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {
                    pool.submit(self._fetch_decompress, client, url): i
                    for i, url in enumerate(urls)
                }
                for fut in as_completed(futures):
                    i = futures[fut]
                    data, raw_len = fut.result()
                    compressed_bytes += raw_len
                    if data is None:
                        _log.warning(
                            "icon_file_missing", url=urls[i],
                            param=records[i].param, step=step,
                        )
                        continue
                    payloads[i] = data

        # Check before opening the file so an all-missing step leaves no empty
        # output behind (the resume check would otherwise have to clean it up).
        if not payloads:
            raise RuntimeError(
                f"ICON: no files downloaded for {date} {cycle:02d}z step={step} "
                f"({len(urls)} requested, all missing)"
            )

        written = 0
        with output_path.open("wb") as fh:
            for i in range(len(urls)):
                data = payloads.get(i)
                if data is None:
                    continue
                fh.write(data)
                written += len(data)
        return StepDownloadResult(
            output_path=output_path,
            bytes_downloaded=compressed_bytes,
            http_requests=len(urls),
        )

    def _fetch_decompress(
        self, client: httpx.Client, url: str
    ) -> tuple[bytes | None, int]:
        """GET one ``.grib2.bz2`` and decompress it; ``(None, 0)`` on 404."""
        resp = request_with_retry(client, "GET", url, follow_redirects=True)
        if resp.status_code == 404:
            return None, 0
        resp.raise_for_status()
        raw = resp.content
        return bz2.decompress(raw), len(raw)
