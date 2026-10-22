"""ECMWF event probabilities, published in two multi-time bundles per init.

The bundle names (240/360) are packaging boundaries, not a two-step forecast.
Both are always attempted so a late bundle leaves manifest.failures populated.
"""
from __future__ import annotations

from typing import Literal

import httpx
import structlog

from climate_download.grib.ecmwf import normalize_offsets
from climate_download.grib.index import IndexRecord
from climate_download.sources._http import request_with_retry
from climate_download.sources.aifs import AifsSource
from climate_download.sources.registry import register

IFS_PARAMS = ("tpg20", "tpg50", "tpg100", "10fgg15", "10fgg25")
RAIN_PARAMS = ("tpg1", "tpg5", "tpg10", "tpg20", "tpg25", "tpg50", "tpg100")
INSTANT_PARAMS = ("2tl273", "10spg10", "10spg15")
MULTIDAY_PARAMS = ("2tl273", "tpl01", "tprl1", "tprg3", "tprg5")


def expected_windows(model: str, bundle: int) -> dict[str, set[str]]:
    if bundle not in (240, 360):
        raise ValueError("probability bundles must be 240 or 360")
    ends = range(24, 241, 12) if bundle == 240 else range(252, 361, 12)
    daily = {f"{end - 24}-{end}" for end in ends}
    if model == "ifs":
        return {p: set(daily) for p in IFS_PARAMS}
    if model != "aifs-ens":
        raise ValueError(f"unsupported probability model {model!r}")
    result = {p: set(daily) for p in RAIN_PARAMS}
    instants = range(12, 241, 12) if bundle == 240 else range(252, 361, 12)
    result.update({p: {str(end) for end in instants} for p in INSTANT_PARAMS})
    multi = {"120-168", "168-240", "120-240"} if bundle == 240 else {"240-360"}
    for param in MULTIDAY_PARAMS:
        result.setdefault(param, set()).update(multi)
    return result


@register("ecmwf-probabilities")
class EcmwfProbabilitySource(AifsSource):
    model: Literal["ifs", "aifs-ens"]

    def _check(self, cycle: int, step: int) -> None:
        if cycle not in (0, 12):
            raise ValueError("this operational probability feed uses 00/12 UTC cycles")
        if step not in (240, 360):
            raise ValueError("probability bundles must be 240 or 360")

    def build_index_url(self, *, date: str, cycle: int, step: int) -> str:
        self._check(cycle, step)
        return super().build_index_url(date=date, cycle=cycle, step=step)

    def build_data_url(self, *, date: str, cycle: int, step: int) -> str:
        self._check(cycle, step)
        return super().build_data_url(date=date, cycle=cycle, step=step)

    def list_available_steps(self, client: httpx.Client, *, date: str, cycle: int) -> list[int]:
        self._check(cycle, 240)
        return [240, 360]

    def probe_step(self, client: httpx.Client, *, date: str, cycle: int, step: int) -> bool:
        # Unlike ordinary sparse NWP ladders, a missing bundle is a failure.
        self._check(cycle, step)
        return True

    def fetch_records(self, client: httpx.Client, *, date: str, cycle: int, step: int) -> list[IndexRecord]:
        records = super().fetch_records(client, date=date, cycle=cycle, step=step)
        if any(b.offset < a.end for a, b in zip(records, records[1:])):
            # Rebuild the full em/ep/es sequence before filtering out other types.
            # A truncated index must fail rather than invent message positions.
            response = request_with_retry(client, "HEAD", self.build_data_url(date=date, cycle=cycle, step=step))
            response.raise_for_status()
            object_size = int(response.headers["Content-Length"])
            records, mode = normalize_offsets(records, object_size)
            structlog.get_logger(__name__).info("probability_offsets_rebuilt", source=self.name,
                date=date, cycle=cycle, step=step, mode=mode, object_size=object_size)
        expected = expected_windows(self.model, step)
        selected = [r for r in records if (r.model_extra or {}).get("type") == "ep" and r.param in expected]
        seen: set[tuple[str, str | None]] = set()
        for rec in selected:
            key = (rec.param, rec.step)
            if key in seen:
                raise ValueError(f"duplicate probability event {key}")
            seen.add(key)
        wanted = {(p, window) for p, windows in expected.items() for window in windows}
        if seen != wanted:
            raise ValueError(f"incomplete probability bundle {step}: missing={sorted(wanted - seen)!r}, unexpected={sorted(seen - wanted)!r}")
        return selected
