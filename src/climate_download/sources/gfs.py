"""NOAA wgrib2 ``.idx`` adapter (split URL templates).

GFS publishes the GRIB file with no extension and the sidecar with an
``.idx`` suffix appended, so the two URLs do not share a clean suffix swap.
The wgrib2 idx format omits message lengths, so ``fetch_records`` issues a
HEAD against the GRIB file to recover the total size needed by
:func:`parse_wgrib2_idx_text`.

Registered under multiple ``type:`` aliases because the *protocol* is the
same across every NOAA wgrib2-style product — GFS, NOAA GraphCastGFS /
``aigfs``, and any future NCEP product on AWS S3 differ only in the URL
template, which lives in the source YAML.
"""

from __future__ import annotations

import httpx
from pydantic import BaseModel, ConfigDict

from climate_download.grib.index import IndexRecord, parse_wgrib2_idx_text
from climate_download.sources._http import request_with_retry
from climate_download.sources._listing import list_remote_steps
from climate_download.sources.base import BaseSource
from climate_download.sources.registry import register

__all__ = ["GfsSource"]


@register("graphcast")
@register("gfs")
class GfsSource(BaseSource, BaseModel):
    """Adapter for NOAA GFS-style sources on AWS S3.

    Both URL templates accept ``{date}``, ``{cycle:02d}`` and ``{step}``;
    GFS uses ``{step:03d}`` for zero-padded forecast hours (f000 .. f384).
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
