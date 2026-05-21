"""Tests for the per-source adapter framework.

Covers the registry mechanics, the three built-in adapters, the YAML
dispatch entry point, and the documented extension surface (a custom
source that overrides ``download_step`` to bypass byte-range entirely —
the path a NetCDF / OPeNDAP source would follow).
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
import respx
from pydantic import BaseModel, ConfigDict, ValidationError

from climate_download.config import load_source_dict
from climate_download.grib.index import IndexRecord
from climate_download.sources import (
    BaseSource,
    SOURCE_REGISTRY,
    StepDownloadResult,
    get_source,
    list_sources,
    register,
)


# --- Registry --------------------------------------------------------------

def test_builtin_sources_registered():
    assert set(list_sources()) >= {"aifs", "gfs", "graphcast", "hrrr", "ifs"}


def test_protocol_aliases_share_implementation():
    # 'ifs' and 'aifs' both bind to AifsSource (ECMWF open-data JSONL protocol);
    # 'graphcast' and 'gfs' both bind to GfsSource (NOAA wgrib2-idx protocol).
    # Locked in so future renames don't silently break the documented YAML type names.
    assert get_source("ifs") is get_source("aifs")
    assert get_source("ifs").__name__ == "AifsSource"
    assert get_source("graphcast") is get_source("gfs")
    assert get_source("graphcast").__name__ == "GfsSource"


def test_register_rejects_duplicate():
    @register("dup-test-only")
    class _A:
        pass

    with pytest.raises(ValueError, match="already registered"):

        @register("dup-test-only")
        class _B:
            pass

    SOURCE_REGISTRY.pop("dup-test-only", None)


def test_get_source_unknown_raises():
    with pytest.raises(KeyError, match="unknown source type"):
        get_source("does-not-exist")


# --- AifsSource ------------------------------------------------------------

def test_aifs_from_dict_renders_urls():
    src = load_source_dict({
        "type": "aifs",
        "name": "aifs-x",
        "url_template": "https://h/{date}/{cycle:02d}z/f{step}h.{suffix}",
    })
    assert type(src).__name__ == "AifsSource"
    assert src.build_index_url(date="20260507", cycle=0, step=6) \
        == "https://h/20260507/00z/f6h.index"
    assert src.build_data_url(date="20260507", cycle=0, step=6) \
        == "https://h/20260507/00z/f6h.grib2"


def test_aifs_rejects_unknown_field():
    with pytest.raises(ValidationError):
        load_source_dict({
            "type": "aifs",
            "name": "x",
            "url_template": "https://h/{suffix}",
            "stray": True,
        })


# --- GfsSource -------------------------------------------------------------

def test_gfs_from_dict_renders_split_urls():
    src = load_source_dict({
        "type": "gfs",
        "name": "gfs-x",
        "index_url_template": "https://h/{date}/{cycle:02d}/f{step:03d}.idx",
        "data_url_template": "https://h/{date}/{cycle:02d}/f{step:03d}",
    })
    assert type(src).__name__ == "GfsSource"
    assert src.build_index_url(date="20260507", cycle=12, step=6) \
        == "https://h/20260507/12/f006.idx"
    assert src.build_data_url(date="20260507", cycle=12, step=6) \
        == "https://h/20260507/12/f006"


# --- Shipped source YAMLs --------------------------------------------------

@pytest.mark.parametrize("name,expected_cls,expected_type", [
    ("aifs",            "AifsSource", "aifs"),
    ("ifs",             "AifsSource", "ifs"),
    ("gfs",             "GfsSource",  "gfs"),
    ("graphcast_pres",  "GfsSource",  "graphcast"),
    ("graphcast_sfc",   "GfsSource",  "graphcast"),
    ("hrrr",            "HrrrSource", "hrrr"),
])
def test_shipped_source_yaml_loads(name, expected_cls, expected_type):
    # Smoke-test the files under config/sources/: each must parse, resolve
    # to the documented adapter class, and use the documented 'type:' alias.
    import yaml
    repo_root = Path(__file__).resolve().parent.parent
    raw = yaml.safe_load((repo_root / "config" / "sources" / f"{name}.yaml").read_text())
    assert raw["type"] == expected_type
    src = load_source_dict(raw)
    assert type(src).__name__ == expected_cls


# --- Dispatch error paths --------------------------------------------------

def test_load_source_dict_requires_type():
    with pytest.raises(ValueError, match="must declare 'type"):
        load_source_dict({"name": "x"})


def test_load_source_dict_unknown_type():
    with pytest.raises(KeyError, match="unknown source type"):
        load_source_dict({"type": "no-such", "name": "x"})


# --- Default probe_step uses HEAD ------------------------------------------

@respx.mock
def test_default_probe_step_returns_false_on_404():
    src = load_source_dict({
        "type": "aifs",
        "name": "aifs-x",
        "url_template": "https://probe.test/{date}/{cycle:02d}/f{step}.{suffix}",
    })
    respx.head("https://probe.test/20260507/00/f6.index").mock(
        return_value=httpx.Response(404)
    )
    respx.head("https://probe.test/20260507/00/f12.index").mock(
        return_value=httpx.Response(200, headers={"content-length": "100"})
    )
    with httpx.Client() as client:
        assert src.probe_step(client, date="20260507", cycle=0, step=6) is False
        assert src.probe_step(client, date="20260507", cycle=0, step=12) is True


# --- Custom source: extension via download_step override ------------------

class _FakeNcSource(BaseSource, BaseModel):
    """A whole-file source that bypasses byte-range; mocks a NetCDF mirror."""

    model_config = ConfigDict(extra="forbid")

    name: str
    description: str | None = None
    base_url: str
    supports_byte_range: bool = False

    def build_index_url(self, *, date, cycle, step):
        return f"{self.base_url}/{date}/{cycle:02d}/f{step:03d}.json"

    def build_data_url(self, *, date, cycle, step):
        return f"{self.base_url}/{date}/{cycle:02d}/f{step:03d}.nc"

    def fetch_records(self, client, *, date, cycle, step):
        # Pretend the upstream sidecar lists one logical "message" per file.
        return [IndexRecord.model_validate(
            {"param": "t2m", "levtype": "sfc",
             "_offset": 0, "_length": 4096}
        )]

    def download_step(self, downloader, *, records, output_path,
                      gap_tolerance, date, cycle, step):
        # Whole-file fetch path: ignore downloader entirely.
        output_path.parent.mkdir(parents=True, exist_ok=True)
        payload = b"GRIB" + b"\0" * 4088 + b"7777"  # min valid skeleton
        output_path.write_bytes(payload)
        return StepDownloadResult(
            output_path=output_path, bytes_downloaded=len(payload),
            http_requests=1,
        )


def test_custom_source_overrides_download_step(tmp_path: Path):
    register("test-fake-nc")(_FakeNcSource)
    try:
        src = load_source_dict({
            "type": "test-fake-nc",
            "name": "fake-nc",
            "base_url": "https://nc.test",
        })
        out = tmp_path / "fake.grib2"
        result = src.download_step(
            downloader=None,  # type: ignore[arg-type]  # unused by override
            records=src.fetch_records(None, date="20260507", cycle=0, step=6),  # type: ignore[arg-type]
            output_path=out,
            gap_tolerance=0,
            date="20260507", cycle=0, step=6,
        )
        assert result.output_path == out
        assert result.bytes_downloaded == 4096
        assert result.http_requests == 1
        assert out.read_bytes().startswith(b"GRIB")
        assert out.read_bytes().endswith(b"7777")
    finally:
        SOURCE_REGISTRY.pop("test-fake-nc", None)



# --- GCS XML listing (AIFS path) -------------------------------------------

_GCS_LIST_XML = """<?xml version='1.0' encoding='UTF-8'?>
<ListBucketResult xmlns='http://doc.s3.amazonaws.com/2006-03-01'>
  <Name>ecmwf-open-data</Name>
  <Prefix>20260510/00z/aifs-single/0p25/oper/</Prefix>
  <Marker></Marker>
  <IsTruncated>false</IsTruncated>
  <Contents><Key>20260510/00z/aifs-single/0p25/oper/20260510000000-0h-oper-fc.grib2</Key></Contents>
  <Contents><Key>20260510/00z/aifs-single/0p25/oper/20260510000000-0h-oper-fc.index</Key></Contents>
  <Contents><Key>20260510/00z/aifs-single/0p25/oper/20260510000000-6h-oper-fc.index</Key></Contents>
  <Contents><Key>20260510/00z/aifs-single/0p25/oper/20260510000000-12h-oper-fc.index</Key></Contents>
  <Contents><Key>20260510/00z/aifs-single/0p25/oper/20260510000000-360h-oper-fc.index</Key></Contents>
</ListBucketResult>
"""


@respx.mock
def test_aifs_list_available_steps_via_gcs():
    from climate_download.sources.aifs import AifsSource
    src = AifsSource(
        name="aifs",
        url_template=(
            "https://storage.googleapis.com/ecmwf-open-data/{date}/{cycle:02d}z/"
            "aifs-single/0p25/oper/{date}{cycle:02d}0000-{step}h-oper-fc.{suffix}"
        ),
    )
    respx.get("https://storage.googleapis.com/ecmwf-open-data/").mock(
        return_value=httpx.Response(200, text=_GCS_LIST_XML)
    )
    with httpx.Client() as c:
        steps = src.list_available_steps(c, date="20260510", cycle=0)
    assert steps == [0, 6, 12, 360]


@respx.mock
def test_gcs_listing_pagination_with_marker():
    from climate_download.sources._listing import list_remote_steps
    template = (
        "https://storage.googleapis.com/ecmwf-open-data/{date}/{cycle:02d}z/"
        "aifs-single/0p25/oper/{date}{cycle:02d}0000-{step}h-oper-fc.index"
    )
    page1 = """<?xml version='1.0'?>
<ListBucketResult xmlns='http://doc.s3.amazonaws.com/2006-03-01'>
  <IsTruncated>true</IsTruncated>
  <NextMarker>20260510/00z/aifs-single/0p25/oper/20260510000000-12h-oper-fc.index</NextMarker>
  <Contents><Key>20260510/00z/aifs-single/0p25/oper/20260510000000-0h-oper-fc.index</Key></Contents>
  <Contents><Key>20260510/00z/aifs-single/0p25/oper/20260510000000-6h-oper-fc.index</Key></Contents>
</ListBucketResult>"""
    page2 = """<?xml version='1.0'?>
<ListBucketResult xmlns='http://doc.s3.amazonaws.com/2006-03-01'>
  <IsTruncated>false</IsTruncated>
  <Contents><Key>20260510/00z/aifs-single/0p25/oper/20260510000000-12h-oper-fc.index</Key></Contents>
  <Contents><Key>20260510/00z/aifs-single/0p25/oper/20260510000000-18h-oper-fc.index</Key></Contents>
</ListBucketResult>"""
    route = respx.get("https://storage.googleapis.com/ecmwf-open-data/").mock(
        side_effect=[httpx.Response(200, text=page1), httpx.Response(200, text=page2)]
    )
    with httpx.Client() as c:
        steps = list_remote_steps(c, index_url_template=template,
                                  date="20260510", cycle=0)
    assert steps == [0, 6, 12, 18]
    assert route.call_count == 2
    # Second request must carry the marker, not continuation-token.
    second_req = route.calls[1].request
    assert "marker=" in str(second_req.url)
    assert "continuation-token=" not in str(second_req.url)


# --- request_with_retry shared resilience floor ----------------------------

@respx.mock
def test_request_with_retry_retries_transient_connect_error():
    """Two simulated SSL EOFs followed by 200 should resolve to a single
    successful response without raising — the same pattern that previously
    aborted the IFS probe phase on GCS."""
    from climate_download.sources._http import request_with_retry

    route = respx.get("https://example.test/idx").mock(
        side_effect=[
            httpx.ConnectError("[SSL: UNEXPECTED_EOF_WHILE_READING]"),
            httpx.ConnectError("[SSL: UNEXPECTED_EOF_WHILE_READING]"),
            httpx.Response(200, text="ok"),
        ]
    )
    with httpx.Client() as c:
        resp = request_with_retry(c, "GET", "https://example.test/idx",
                                  max_attempts=4)
    assert resp.status_code == 200
    assert resp.text == "ok"
    assert route.call_count == 3


@respx.mock
def test_request_with_retry_retries_5xx_then_succeeds():
    from climate_download.sources._http import request_with_retry

    route = respx.get("https://example.test/list").mock(
        side_effect=[
            httpx.Response(503, text="busy"),
            httpx.Response(200, text="ok"),
        ]
    )
    with httpx.Client() as c:
        resp = request_with_retry(c, "GET", "https://example.test/list",
                                  max_attempts=4)
    assert resp.status_code == 200
    assert route.call_count == 2


@respx.mock
def test_request_with_retry_passes_404_through():
    """404 is a *semantic* answer (e.g. step not published) — must NOT retry."""
    from climate_download.sources._http import request_with_retry

    route = respx.head("https://example.test/missing").mock(
        return_value=httpx.Response(404)
    )
    with httpx.Client() as c:
        resp = request_with_retry(c, "HEAD", "https://example.test/missing")
    assert resp.status_code == 404
    assert route.call_count == 1


@respx.mock
def test_probe_step_recovers_from_transient_ssl_eof():
    """End-to-end: AifsSource.probe_step should swallow one ConnectError."""
    from climate_download.sources.aifs import AifsSource

    src = AifsSource(
        name="aifs", url_template="https://example.test/{step}h.{suffix}",
    )
    route = respx.head("https://example.test/6h.index").mock(
        side_effect=[
            httpx.ConnectError("[SSL: UNEXPECTED_EOF_WHILE_READING]"),
            httpx.Response(200),
        ]
    )
    with httpx.Client() as c:
        assert src.probe_step(c, date="20260501", cycle=12, step=6) is True
    assert route.call_count == 2


@respx.mock
def test_aifs_fetch_records_recovers_from_transient_ssl_eof():
    """End-to-end: AifsSource.fetch_records should swallow one ConnectError —
    this is exactly the IFS step 24 / 45 failure mode the retry helper closes."""
    from climate_download.sources.aifs import AifsSource

    src = AifsSource(
        name="aifs", url_template="https://example.test/{step}h.{suffix}",
    )
    body = (
        '{"param": "u", "levtype": "pl", "levelist": "850", '
        '"_offset": 0, "_length": 10}\n'
    )
    route = respx.get("https://example.test/6h.index").mock(
        side_effect=[
            httpx.ConnectError("[SSL: UNEXPECTED_EOF_WHILE_READING]"),
            httpx.Response(200, text=body),
        ]
    )
    with httpx.Client() as c:
        records = src.fetch_records(c, date="20260501", cycle=12, step=6)
    assert len(records) == 1 and records[0].param == "u"
    assert route.call_count == 2


# --- list_available_variables default implementation -----------------------

def test_list_available_variables_default_dedupes_and_preserves_order():
    """The default impl projects fetch_records to unique triples in order."""

    class StubSource(BaseSource):
        name = "stub"
        description = None
        supports_byte_range = True

        def build_index_url(self, *, date, cycle, step):
            return "x://i"

        def build_data_url(self, *, date, cycle, step):
            return "x://d"

        def fetch_records(self, client, *, date, cycle, step):
            payloads = [
                {"param": "u", "levtype": "pl", "levelist": "850",
                 "level_desc": "850 mb",
                 "_offset": 0, "_length": 10},
                {"param": "v", "levtype": "pl", "levelist": "850",
                 "level_desc": "850 mb",
                 "_offset": 10, "_length": 10},
                {"param": "u", "levtype": "pl", "levelist": "850",
                 "level_desc": "850 mb",
                 "_offset": 20, "_length": 10},   # duplicate triple
                {"param": "t", "levtype": "sfc", "levelist": None,
                 "level_desc": "surface",
                 "_offset": 30, "_length": 10},
            ]
            return [IndexRecord.model_validate(p) for p in payloads]

    src = StubSource()
    out = src.list_available_variables(client=None, date="x", cycle=0, step=0)
    assert [(v.param, v.levtype, v.levelist) for v in out] == [
        ("u", "pl", "850"),
        ("v", "pl", "850"),
        ("t", "sfc", None),
    ]
    by_param = {v.param: v.count for v in out}
    assert by_param == {"u": 2, "v": 1, "t": 1}
    # level_desc is captured from the first record in each triple's group.
    assert [v.level_desc for v in out] == ["850 mb", "850 mb", "surface"]



# --- CLI --yaml scaffold for list-variables --------------------------------

def test_render_variables_yaml_groups_by_levtype_and_emits_levels():
    """`--yaml` should produce a paste-ready VariableGroup block."""
    import yaml

    from climate_download.cli import _render_variables_yaml
    from climate_download.sources.base import VariableInfo

    variables = [
        VariableInfo("UGRD", "pl", "850", level_desc="850 mb"),
        VariableInfo("VGRD", "pl", "850", level_desc="850 mb"),
        VariableInfo("UGRD", "pl", "500", level_desc="500 mb"),
        VariableInfo("TMP", "hag", "2", level_desc="2 m above ground"),
        VariableInfo("PRMSL", "atm", None, level_desc="mean sea level"),
    ]
    text = _render_variables_yaml(
        variables, source_name="gfs", date="20260510", cycle=0, step=6,
    )
    # Header comments must mention source / time anchor.
    assert "gfs 20260510 cycle=00z step=6h" in text
    # YAML body (strip the leading comments) must parse and yield 3 groups
    # for the 3 distinct levtypes.
    body = yaml.safe_load(text)
    assert {g["name"] for g in body["variables"]} == {
        "gfs_pl", "gfs_hag", "gfs_atm",
    }
    pl = next(g for g in body["variables"] if g["levtype"] == "pl")
    assert set(pl["params"]) == {"UGRD", "VGRD"}
    assert pl["levels"] == ["500", "850"]   # numeric sort
    # hag is also a "with-levels" levtype.
    hag = next(g for g in body["variables"] if g["levtype"] == "hag")
    assert hag["levels"] == ["2"]
    # atm is levelless: no `levels` key.
    atm = next(g for g in body["variables"] if g["levtype"] == "atm")
    assert "levels" not in atm
