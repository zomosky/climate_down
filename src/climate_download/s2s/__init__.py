"""ECMWF Data Store (ECDS) S2S sub-seasonal forecast adapter.

S2S is fundamentally different from the byte-range / index-sidecar sources in
:mod:`climate_download.sources` (AIFS, IFS, GFS, HRRR, GraphCast):

* Async submit-poll-download via the ``cdsapi`` client, not synchronous HTTP.
* No ``.index`` sidecar; subsetting is server-side via form fields.
* One ``retrieve`` call returns a multi-message GRIB covering every requested
  step / variable / level — there is no per-step file.
* Init frequency is twice-weekly for ECMWF (Mon/Thu) and varies per centre.

The orchestration therefore lives in this parallel package rather than
sharing :mod:`climate_download.jobs`. See :mod:`climate_download.s2s.jobs`
for the entrypoint.
"""

from __future__ import annotations

from climate_download.s2s.config import (
    S2SJobConfig,
    S2SVariableGroup,
    load_s2s_job,
    load_s2s_source,
)
from climate_download.s2s.jobs import (
    S2SFailure,
    S2SOutcome,
    S2SResult,
    run_s2s_job,
)
from climate_download.s2s.source import S2SSource

__all__ = [
    "S2SFailure",
    "S2SJobConfig",
    "S2SOutcome",
    "S2SResult",
    "S2SSource",
    "S2SVariableGroup",
    "load_s2s_job",
    "load_s2s_source",
    "run_s2s_job",
]
