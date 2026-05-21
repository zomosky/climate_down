"""Unit tests for the S2S adapter (config + orchestrator).

Real cdsapi calls are never made: ``StubECDSClient`` writes a tiny valid
GRIB stub for each request and records the payload so the test asserts on
the request schema the orchestrator built.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from climate_download.s2s.client import ECDSCredentials, load_credentials
from climate_download.s2s.config import (
    S2SDownloadConfig,
    S2SJobConfig,
    S2SLeadtimeRange,
    S2SSource,
    S2STimeConfig,
    S2SVariableGroup,
    load_s2s_job,
)
from climate_download.s2s.jobs import (
    build_retrieve_request,
    render_leadtimes,
    run_s2s_job,
)


# ── helpers ───────────────────────────────────────────────────────────


_GRIB_HEAD = b"GRIB"
_GRIB_TRAILER = b"7777"


def _stub_grib_blob(n_messages: int = 3) -> bytes:
    # One simulated message = "GRIB" + 8 zero bytes + "7777"; n in a row
    # to mimic the multi-message files real S2S returns. The orchestrator
    # only checks header / trailer / count, not message validity.
    return (_GRIB_HEAD + b"\x00" * 8 + _GRIB_TRAILER) * n_messages


class StubECDSClient:
    """Captures every ``retrieve`` call and writes a stub GRIB to ``target``."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict, str]] = []

    def retrieve(self, name: str, request: dict, target: str) -> None:
        self.calls.append((name, dict(request), target))
        Path(target).parent.mkdir(parents=True, exist_ok=True)
        Path(target).write_bytes(_stub_grib_blob())


@pytest.fixture
def stub_client_factory():
    stub = StubECDSClient()

    def _factory(*, url: str, key: str, quiet: bool = False) -> StubECDSClient:
        return stub

    _factory.stub = stub  # expose for assertions
    return _factory


@pytest.fixture
def fake_creds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    rc = tmp_path / ".ecdsapirc"
    rc.write_text("url: https://test.example/api\nkey: dummy-token\n")
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))  # type: ignore[arg-type]
    return rc


def _make_job(tmp_path: Path) -> S2SJobConfig:
    src = S2SSource(name="s2s-ecmwf", origin="ecmwf")
    groups = [
        S2SVariableGroup(
            name="single_inst", level_type="single_level",
            leadtime_kind="instant",
            variables=["10_m_u_component_of_wind", "mean_sea_level_pressure"],
        ),
        S2SVariableGroup(
            name="single_daily", level_type="single_level",
            leadtime_kind="daily",
            leadtime=S2SLeadtimeRange(start=0, end=72, step=24),
            variables=["2_m_temperature"],
        ),
        S2SVariableGroup(
            name="pressure_low", level_type="pressure",
            leadtime_kind="instant",
            levels=["925", "1000"],
            variables=["u_component_of_wind", "geopotential_height"],
        ),
    ]
    return S2SJobConfig(
        source=src, groups=groups,
        time=S2STimeConfig(date="20260507", cycle=0,
                           leadtime=S2SLeadtimeRange(start=0, end=24, step=6)),
        download=S2SDownloadConfig(output_dir=tmp_path / "out"),
    )


# ── credential loading ──────────────────────────────────────────────


def test_load_credentials_parses_url_and_key(tmp_path: Path) -> None:
    rc = tmp_path / ".ecdsapirc"
    rc.write_text("# leading comment\nurl: https://x/api\nkey: abc\n\n")
    creds = load_credentials(rc)
    assert creds == ECDSCredentials(url="https://x/api", key="abc")
    # Repr must redact the token to keep it out of accidental logs.
    assert "abc" not in repr(creds)


def test_load_credentials_rejects_unknown_keys(tmp_path: Path) -> None:
    rc = tmp_path / ".ecdsapirc"
    rc.write_text("url: https://x/api\nkey: abc\nemail: oops@x\n")
    with pytest.raises(ValueError, match="unexpected key 'email'"):
        load_credentials(rc)


def test_load_credentials_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="credentials file not found"):
        load_credentials(tmp_path / ".does-not-exist")


# ── leadtime rendering ──────────────────────────────────────────────


def test_render_leadtimes_instant_uses_six_hour_grid() -> None:
    g = S2SVariableGroup(
        name="g", level_type="single_level", leadtime_kind="instant",
        variables=["x"],
    )
    out = render_leadtimes(g, S2SLeadtimeRange(start=0, end=24, step=6))
    assert out == ["0", "6", "12", "18", "24"]


def test_render_leadtimes_daily_uses_window_strings() -> None:
    g = S2SVariableGroup(
        name="g", level_type="single_level", leadtime_kind="daily",
        variables=["x"],
    )
    out = render_leadtimes(g, S2SLeadtimeRange(start=0, end=72, step=24))
    assert out == ["0_24", "24_48", "48_72"]


def test_render_leadtimes_per_group_override() -> None:
    g = S2SVariableGroup(
        name="g", level_type="single_level", leadtime_kind="instant",
        variables=["x"],
        leadtime=S2SLeadtimeRange(start=12, end=24, step=6),
    )
    out = render_leadtimes(g, S2SLeadtimeRange(start=0, end=1104, step=6))
    assert out == ["12", "18", "24"]


# ── request building ────────────────────────────────────────────────


def test_build_retrieve_request_pressure_includes_level_value() -> None:
    src = S2SSource(name="s", origin="ecmwf")
    g = S2SVariableGroup(
        name="g", level_type="pressure", leadtime_kind="instant",
        levels=["925", "1000"], variables=["u_component_of_wind"],
    )
    req = build_retrieve_request(
        src, g, date="20260507", cycle=0,
        default_leadtime=S2SLeadtimeRange(start=0, end=6, step=6),
    )
    assert req["origin"] == "ecmwf"
    assert req["level_type"] == "pressure"
    assert req["level_value"] == ["925_hpa", "1000_hpa"]
    assert req["year"] == ["2026"]
    assert req["month"] == ["05"]
    assert req["day"] == ["07"]
    assert req["time"] == ["00:00"]
    assert req["data_format"] == "grib"


def test_build_retrieve_request_single_level_omits_level_value() -> None:
    src = S2SSource(name="s", origin="cma")
    g = S2SVariableGroup(
        name="g", level_type="single_level", leadtime_kind="instant",
        variables=["10_m_u_component_of_wind"],
    )
    req = build_retrieve_request(
        src, g, date="20260508", cycle=12,
        default_leadtime=S2SLeadtimeRange(start=0, end=6, step=6),
    )
    assert "level_value" not in req
    assert req["origin"] == "cma"
    assert req["time"] == ["12:00"]


# ── source / group validation ───────────────────────────────────────


def test_source_rejects_unknown_origin() -> None:
    with pytest.raises(ValueError, match="not in S2S catalogue"):
        S2SSource(name="bad", origin="atlantis")


def test_pressure_group_requires_levels() -> None:
    with pytest.raises(ValueError, match="requires a non-empty 'levels'"):
        S2SVariableGroup(
            name="g", level_type="pressure", leadtime_kind="instant",
            variables=["u_component_of_wind"],
        )


def test_single_level_group_rejects_levels() -> None:
    with pytest.raises(ValueError, match="must be omitted"):
        S2SVariableGroup(
            name="g", level_type="single_level", leadtime_kind="instant",
            variables=["10_m_u_component_of_wind"], levels=["sfc"],
        )


# ── orchestrator end-to-end with stub client ────────────────────────


def test_run_s2s_job_writes_one_file_per_group(
    tmp_path: Path, fake_creds: Path, stub_client_factory
) -> None:
    job = _make_job(tmp_path)
    outcome = run_s2s_job(
        job, write_report=False, client_factory=stub_client_factory,
    )
    assert outcome.failed == []
    assert len(outcome.succeeded) == 3
    base = tmp_path / "out" / "s2s-ecmwf" / "20260507" / "00z"
    for group in ("single_inst", "single_daily", "pressure_low"):
        assert (base / f"{group}.grib2").is_file()
    # Manifest is written beside the GRIBs.
    manifest = base / "20260507_00z_s2s-ecmwf.manifest.json"
    assert manifest.is_file()
    payload = json.loads(manifest.read_text())
    assert payload["kind"] == "s2s"
    assert payload["source"]["origin"] == "ecmwf"
    assert {f["group"] for f in payload["files"]} == {
        "single_inst", "single_daily", "pressure_low",
    }


def test_run_s2s_job_records_request_payload(
    tmp_path: Path, fake_creds: Path, stub_client_factory
) -> None:
    job = _make_job(tmp_path)
    run_s2s_job(job, write_report=False, client_factory=stub_client_factory)
    calls = stub_client_factory.stub.calls
    assert len(calls) == 3
    by_group = {c[1]["origin"] + ":" + c[1]["level_type"] + ":"
                + c[1]["leadtime_hour"][0]: c[1] for c in calls}
    assert "ecmwf:single_level:0" in by_group
    daily_call = next(c[1] for c in calls if c[1]["leadtime_hour"][0] == "0_24")
    assert daily_call["leadtime_hour"] == ["0_24", "24_48", "48_72"]


def test_run_s2s_job_resumes_on_existing_valid_grib(
    tmp_path: Path, fake_creds: Path, stub_client_factory
) -> None:
    job = _make_job(tmp_path)
    target = (tmp_path / "out" / "s2s-ecmwf" / "20260507" / "00z"
              / "single_inst.grib2")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(_stub_grib_blob(2))
    outcome = run_s2s_job(
        job, write_report=False, client_factory=stub_client_factory,
    )
    resumed = [r for r in outcome.succeeded if r.resumed]
    assert {r.group for r in resumed} == {"single_inst"}
    # Stub client should NOT have been called for the resumed group.
    called_groups = [c[1]["variable"] for c in stub_client_factory.stub.calls]
    assert ["10_m_u_component_of_wind", "mean_sea_level_pressure"] not in called_groups


# ── shipped YAML loads cleanly ──────────────────────────────────────


def test_shipped_ecmwf_job_yaml_loads() -> None:
    cfg = load_s2s_job("config/jobs/s2s_renewables_ecmwf.yaml")
    assert cfg.source.name == "s2s-ecmwf"
    assert cfg.source.origin == "ecmwf"
    assert {g.name for g in cfg.groups} == {
        "single_inst", "single_daily", "pressure_low",
    }


def test_shipped_cma_job_yaml_loads() -> None:
    cfg = load_s2s_job("config/jobs/s2s_renewables_cma.yaml")
    assert cfg.source.origin == "cma"
