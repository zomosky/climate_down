"""NOAA HRRR adapter (wgrib2 ``.idx`` sidecar, S3 mirror).

HRRR is a 3 km CONUS rapid-refresh model with multiple product slices
(``wrfsfc`` / ``wrfprs`` / ``wrfnat`` / ``wrfsubh``). Although the sidecar
format is identical to GFS, the URL layout is meaningfully different
(per-product slice in the basename, two-digit forecast hour) which is the
whole reason it lives in its own adapter file: each source's URL quirks
stay isolated even when the parser is shared.
"""

from __future__ import annotations

import httpx
from pydantic import BaseModel, ConfigDict

from climate_download.grib.index import IndexRecord, parse_wgrib2_idx_text
from climate_download.sources._http import request_with_retry
from climate_download.sources._listing import list_remote_steps
from climate_download.sources.base import BaseSource
from climate_download.sources.registry import register

__all__ = ["HrrrSource"]


@register("hrrr")
class HrrrSource(BaseSource, BaseModel):
    """Adapter for NOAA HRRR on s3://noaa-hrrr-bdp-pds.

    Defaults match the standard public mirror; ``product`` selects which
    slice (default ``wrfsfc``) so a single YAML can swap surface for
    pressure-level by changing one field.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    description: str | None = None
    index_url_template: str
    data_url_template: str
    supports_byte_range: bool = True

    def build_index_url(self, *, date: str, cycle: int, step: int) -> str:
        return self.index_url_template.format(date=date, cycle=cycle, step=step)

    def build_data_url(self, *, date: str, cycle: int, step: int) -> str:
        return self.data_url_template.format(date=date, cycle=cycle, step=step)

    def fetch_records(
        self, client: httpx.Client, *, date: str, cycle: int, step: int
    ) -> list[IndexRecord]:
        idx_url = self.build_index_url(date=date, cycle=cycle, step=step)
        data_url = self.build_data_url(date=date, cycle=cycle, step=step)
        idx_resp = request_with_retry(client, "GET", idx_url)
        idx_resp.raise_for_status()
        head = request_with_retry(client, "HEAD", data_url, follow_redirects=True)
        head.raise_for_status()
        total_size = int(head.headers["content-length"])
        return parse_wgrib2_idx_text(idx_resp.text, total_size=total_size)

    def list_available_steps(
        self, client: httpx.Client, *, date: str, cycle: int
    ) -> list[int] | None:
        return list_remote_steps(
            client,
            index_url_template=self.index_url_template,
            date=date, cycle=cycle,
        )
