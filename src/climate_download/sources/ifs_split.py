"""ECMWF split-file Open Data: merge selected surface and pressure messages.

Each physical file has its own offset space. Keep that origin on every
IndexRecord and only coalesce ranges within one physical file.
"""
from __future__ import annotations

import os
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
from typing import Literal

import httpx

from climate_download.grib.index import IndexRecord, merge_ranges, parse_index_text
from climate_download.grib.partial import PartialDownloader
from climate_download.sources._http import request_with_retry
from climate_download.sources._listing import list_remote_steps
from climate_download.sources.aifs import AifsSource
from climate_download.sources.base import StepDownloadResult
from climate_download.sources.registry import register

_PARTS = ("sfc", "pl")
_ORIGIN = "_ifs_file_part"


@register("ifs-split")
class IfsSplitSource(AifsSource):
    """The template adds ``{levtype}`` for the physical ``sfc`` / ``pl`` file."""

    def _url(self, *, date: str, cycle: int, step: int, suffix: str,
             levtype: Literal["sfc", "pl"] = "sfc") -> str:
        return self.url_template.format(date=date, cycle=cycle, step=step,
                                        suffix=suffix, levtype=levtype)

    def build_index_url(self, *, date: str, cycle: int, step: int,
                        levtype: Literal["sfc", "pl"] = "sfc") -> str:
        return self._url(date=date, cycle=cycle, step=step, levtype=levtype, suffix="index")

    def build_data_url(self, *, date: str, cycle: int, step: int,
                       levtype: Literal["sfc", "pl"] = "sfc") -> str:
        return self._url(date=date, cycle=cycle, step=step, levtype=levtype, suffix="grib2")

    def probe_step(self, client: httpx.Client, *, date: str, cycle: int, step: int) -> bool:
        present = False
        for part in _PARTS:
            response = request_with_retry(client, "HEAD", self.build_index_url(
                date=date, cycle=cycle, step=step, levtype=part), follow_redirects=True)
            if response.status_code == 404:
                continue
            response.raise_for_status()
            present = True
        # One published part is attempted, so a missing companion becomes
        # an index failure rather than silently disappearing from the init.
        return present

    def list_available_steps(self, client: httpx.Client, *, date: str, cycle: int) -> list[int] | None:
        steps: set[int] = set()
        for part in _PARTS:
            template = self.url_template.replace("{levtype}", part).replace("{suffix}", "index")
            listed = list_remote_steps(client, index_url_template=template, date=date, cycle=cycle)
            if listed is None:
                return None
            steps.update(listed)
        return sorted(steps)

    def fetch_records(self, client: httpx.Client, *, date: str, cycle: int, step: int) -> list[IndexRecord]:
        records = []
        for part in _PARTS:
            response = request_with_retry(client, "GET", self.build_index_url(
                date=date, cycle=cycle, step=step, levtype=part))
            response.raise_for_status()
            entries = parse_index_text(response.text)
            if not entries:
                raise ValueError(f"empty IFS {part} index")
            previous_end = 0
            for entry in sorted(entries, key=lambda record: record.offset):
                if entry.length <= 0 or entry.offset < previous_end:
                    raise ValueError(f"invalid or overlapping offsets in IFS {part} index")
                previous_end = entry.end
            records.extend(entry.model_copy(update={_ORIGIN: part}) for entry in entries)
        return records

    def download_step(self, downloader: PartialDownloader, *, records: list[IndexRecord],
                      output_path: Path, gap_tolerance: int, date: str, cycle: int,
                      step: int) -> StepDownloadResult:
        if not records:
            raise ValueError("records must not be empty")
        grouped: dict[str, list[IndexRecord]] = {part: [] for part in _PARTS}
        for record in records:
            origin = (record.model_extra or {}).get(_ORIGIN)
            if origin not in grouped:
                raise ValueError("IFS record has no physical-file origin")
            grouped[origin].append(record)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        total_bytes = total_requests = 0
        with TemporaryDirectory(prefix=f".{output_path.name}.", dir=output_path.parent) as temporary:
            folder = Path(temporary)
            parts = []
            for part in _PARTS:
                if not grouped[part]:
                    continue
                ranges = merge_ranges(grouped[part], gap_tolerance=gap_tolerance)
                path = folder / f"{part}.grib2"
                total_bytes += downloader.download(self.build_data_url(
                    date=date, cycle=cycle, step=step, levtype=part), ranges, path)
                total_requests += len(ranges)
                parts.append(path)
            merged = folder / "merged.grib2"
            with merged.open("wb") as target:
                for path in parts:
                    with path.open("rb") as source:
                        shutil.copyfileobj(source, target)
            os.replace(merged, output_path)
        return StepDownloadResult(output_path=output_path, bytes_downloaded=total_bytes,
                                  http_requests=total_requests)
