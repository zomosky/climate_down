"""NOAA AIGFS (GraphCast) merged sfc+pres adapter.

The post-2026-04-19 AIGFS product splits each ``(date, cycle, step)`` into two
GRIB files — ``aigfs.t{cc}z.sfc.fXXX.grib2`` (surface) and
``aigfs.t{cc}z.pres.fXXX.grib2`` (pressure levels). The *earlier* archive
(2024-02-05 ~ 2026-04-18, :mod:`graphcast_history`) instead merges both into a
single ``pgrb2`` file, and restore turns each single file into **one** flat
Zarr per init.

To keep a single, uniform product across the format cutover, this adapter
fetches **both** AIGFS files for a step and concatenates the selected GRIB
messages into **one** ``.subset.grib2`` — structurally identical to the old
merged file, so the same restore adapter yields one uniform Zarr per init on
both sides of the cutover.

It reuses the wgrib2 ``.idx`` machinery (same protocol as :class:`GfsSource`);
the only differences are (1) two idx/data URL template pairs instead of one and
(2) a ``download_step`` that groups the selected records by their source file,
byte-range-fetches each, and concatenates. Concatenating two valid GRIB2
documents is itself a valid GRIB2 document (messages are self-delimiting), so
``_validate_grib`` passes unchanged.
"""

from __future__ import annotations

from pathlib import Path

import httpx
from pydantic import BaseModel, ConfigDict

from climate_download.grib.index import IndexRecord, merge_ranges, parse_wgrib2_idx_text
from climate_download.sources._http import request_with_retry
from climate_download.sources._listing import list_remote_steps
from climate_download.sources.base import BaseSource, StepDownloadResult
from climate_download.sources.registry import register

__all__ = ["GraphcastMergedSource"]


@register("graphcast-merged")
class GraphcastMergedSource(BaseSource, BaseModel):
    """Adapter that merges the AIGFS sfc + pres files into one GRIB subset.

    All four templates accept ``{date}``, ``{cycle:02d}`` and ``{step:03d}``.
    ``probe_step`` / ``list_available_steps`` key off the **sfc** file, whose
    presence marks the init as published.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    description: str | None = None
    sfc_index_url_template: str
    sfc_data_url_template: str
    pres_index_url_template: str
    pres_data_url_template: str
    supports_byte_range: bool = True

    # -- URL builders (probe/list use the sfc file as the "published" signal) --
    def build_index_url(self, *, date: str, cycle: int, step: int) -> str:
        return self.sfc_index_url_template.format(date=date, cycle=cycle, step=step)

    def build_data_url(self, *, date: str, cycle: int, step: int) -> str:
        return self.sfc_data_url_template.format(date=date, cycle=cycle, step=step)

    def _fetch_file_records(
        self, client: httpx.Client, *, idx_url: str, data_url: str
    ) -> list[IndexRecord]:
        """Parse one wgrib2 ``.idx`` file, tagging each record with its data URL."""
        idx_resp = request_with_retry(client, "GET", idx_url)
        idx_resp.raise_for_status()
        head = request_with_retry(client, "HEAD", data_url, follow_redirects=True)
        head.raise_for_status()
        total_size = int(head.headers["content-length"])
        records = parse_wgrib2_idx_text(idx_resp.text, total_size=total_size)
        # ``data_url`` rides along as an extra field (IndexRecord is
        # ``extra="allow"``) so ``download_step`` can regroup after selection.
        return [r.model_copy(update={"data_url": data_url}) for r in records]

    def fetch_records(
        self, client: httpx.Client, *, date: str, cycle: int, step: int
    ) -> list[IndexRecord]:
        sfc = self._fetch_file_records(
            client,
            idx_url=self.sfc_index_url_template.format(date=date, cycle=cycle, step=step),
            data_url=self.sfc_data_url_template.format(date=date, cycle=cycle, step=step),
        )
        pres = self._fetch_file_records(
            client,
            idx_url=self.pres_index_url_template.format(date=date, cycle=cycle, step=step),
            data_url=self.pres_data_url_template.format(date=date, cycle=cycle, step=step),
        )
        return sfc + pres

    def download_step(
        self,
        downloader,
        *,
        records: list[IndexRecord],
        output_path: Path,
        gap_tolerance: int,
        date: str,
        cycle: int,
        step: int,
    ) -> StepDownloadResult:
        """Group selected records by source file, fetch each, concatenate.

        Each record carries the ``data_url`` of the file it came from (attached
        in :meth:`fetch_records`). We byte-range-download each file's ranges to
        a temp file, then concatenate the temps into ``output_path`` in a
        deterministic (url-sorted) order.
        """
        by_file: dict[str, list[IndexRecord]] = {}
        for rec in records:
            url = getattr(rec, "data_url", None)
            if url is None:  # pragma: no cover — defensive; fetch_records always tags
                url = self.build_data_url(date=date, cycle=cycle, step=step)
            by_file.setdefault(url, []).append(rec)

        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)

        parts: list[Path] = []
        total_bytes = 0
        total_requests = 0
        try:
            for i, url in enumerate(sorted(by_file)):
                ranges = merge_ranges(by_file[url], gap_tolerance=gap_tolerance)
                part = out.with_suffix(out.suffix + f".part{i}")
                total_bytes += downloader.download(url, ranges, part)
                total_requests += len(ranges)
                parts.append(part)

            with out.open("wb") as dst:
                for part in parts:
                    dst.write(part.read_bytes())
        finally:
            for part in parts:
                part.unlink(missing_ok=True)

        return StepDownloadResult(
            output_path=out,
            bytes_downloaded=total_bytes,
            http_requests=total_requests,
        )

    def list_available_steps(
        self, client: httpx.Client, *, date: str, cycle: int
    ) -> list[int] | None:
        return list_remote_steps(
            client,
            index_url_template=self.sfc_index_url_template,
            date=date, cycle=cycle,
        )
