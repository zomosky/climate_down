"""Pydantic schema for one S2S source (one centre + one collection).

A source pins the *who* and *what dataset* dimensions: which centre's data
to pull (``ecmwf`` / ``cma`` / ``ncep`` / ...) and which ECDS collection
hosts it (``s2s-forecasts`` for real-time, ``s2s-reforecasts`` for the
hindcast set). Per-job dimensions (variables, leadtime, init date) live in
:mod:`climate_download.s2s.config` so one source YAML can serve multiple
job YAMLs.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

__all__ = ["S2SSource", "VALID_ORIGINS"]


# Snapshot of origins exposed by the ECDS s2s-forecasts dataset (form schema
# fetched 2026-05-15). Kept here as a literal for fast validation and to
# document supported centres without a live HTTP call.
VALID_ORIGINS: tuple[str, ...] = (
    "bom",
    "cma",
    "cnr_isac",
    "cnrm",
    "cptec",
    "eccc",
    "ecmwf",
    "hmcr",
    "iap_cas",
    "jma",
    "kma",
    "ncep",
    "ukmo",
)


class S2SSource(BaseModel):
    """One S2S source: a centre × collection pair with default knobs.

    Attributes
    ----------
    type
        Discriminator; always ``"s2s"`` for now (kept for symmetry with the
        byte-range source registry).
    name
        Stable identifier used in output paths, manifests and logs (e.g.
        ``s2s-ecmwf`` or ``s2s-cma``); should be unique across sources.
    description
        Free-form human label.
    collection
        ECDS dataset id; ``s2s-forecasts`` (real-time) or ``s2s-reforecasts``.
    origin
        One of :data:`VALID_ORIGINS` (the ECDS form's ``origin`` field).
    forecast_type
        ``control_forecast`` (1 deterministic member) or
        ``perturbed_forecast`` (every available ensemble member).
    """

    model_config = ConfigDict(extra="forbid")

    type: Literal["s2s"] = "s2s"
    name: str = Field(min_length=1)
    description: str | None = None
    collection: Literal["s2s-forecasts", "s2s-reforecasts"] = "s2s-forecasts"
    origin: str
    forecast_type: Literal["control_forecast", "perturbed_forecast"] = (
        "control_forecast"
    )

    @property
    def supports_byte_range(self) -> bool:
        """S2S is async submit-poll-download; never byte-range."""
        return False

    def model_post_init(self, _ctx: object) -> None:  # noqa: D401
        if self.origin not in VALID_ORIGINS:
            valid = ", ".join(VALID_ORIGINS)
            raise ValueError(
                f"S2SSource.origin={self.origin!r} not in S2S catalogue; "
                f"valid origins are: {valid}"
            )
