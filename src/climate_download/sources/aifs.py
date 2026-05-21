"""ECMWF open-data adapter (JSONL ``.index`` sidecar).

URL shape: a single ``url_template`` with a ``{suffix}`` placeholder that
becomes ``index`` for the sidecar and ``grib2`` for the data file. Used by
ECMWF open-data on Google Cloud Storage where the two artifacts differ
only by extension.

Registered under multiple ``type:`` aliases because the *protocol* is shared
across every ECMWF open-data model — the difference between AIFS, IFS HRES
and any future ECMWF data-driven model is only the URL template, which
lives in the source YAML. Adapters are protocol-bound, not model-bound.
"""

from __future__ import annotations

import httpx
from pydantic import BaseModel, ConfigDict

from climate_download.grib.index import IndexRecord, parse_index_text
from climate_download.sources._http import request_with_retry
from climate_download.sources._listing import list_remote_steps
from climate_download.sources.base import BaseSource
from climate_download.sources.registry import register

__all__ = ["AifsSource"]


@register("ifs")
@register("aifs")
class AifsSource(BaseSource, BaseModel):
    """Adapter for ECMWF open-data style sources with a single URL template.

    The same template renders both the sidecar (``suffix=index``) and the
    GRIB file (``suffix=grib2``); placeholders are ``{date}``, ``{cycle:02d}``,
    ``{step}`` and ``{suffix}``.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    description: str | None = None
    url_template: str
    supports_byte_range: bool = True

    def build_index_url(self, *, date: str, cycle: int, step: int) -> str:
        return self.url_template.format(
            date=date, cycle=cycle, step=step, suffix="index"
        )

    def build_data_url(self, *, date: str, cycle: int, step: int) -> str:
        return self.url_template.format(
            date=date, cycle=cycle, step=step, suffix="grib2"
        )

    def fetch_records(
        self, client: httpx.Client, *, date: str, cycle: int, step: int
    ) -> list[IndexRecord]:
        url = self.build_index_url(date=date, cycle=cycle, step=step)
        resp = request_with_retry(client, "GET", url)
        resp.raise_for_status()
        return parse_index_text(resp.text)

    def list_available_steps(
        self, client: httpx.Client, *, date: str, cycle: int
    ) -> list[int] | None:
        # Render the sidecar template (suffix=index) so the regex matches
        # ``…-{step}h-oper-fc.index`` keys on GCS.
        index_template = self.url_template.replace("{suffix}", "index")
        return list_remote_steps(
            client, index_url_template=index_template, date=date, cycle=cycle,
        )
