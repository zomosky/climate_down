"""Tests for the DWD ICON adapter (whole-file, bz2, static catalogue).

ICON is the one built-in source that bypasses byte-range downloading: it
fetches one bzip2 GRIB file per variable and concatenates them. These tests
cover URL construction, catalogue→records projection, the bz2 download/concat
path (including per-file 404 tolerance), and step listing via the DWD Apache
autoindex page. All HTTP is mocked with respx — no network.
"""

from __future__ import annotations

import bz2
from pathlib import Path

import httpx
import pytest
import respx

from climate_download.config import load_source_dict
from climate_download.jobs import select_records
from climate_download.sources import get_source, list_sources
from climate_download.sources.icon import (
    DEFAULT_ICON_RENEWABLE_CATALOG,
    IconSource,
)


def _make_source(**overrides) -> IconSource:
    payload = {
        "type": "icon",
        "name": "icon-test",
        "base_url": "https://dwd.test/icon/grib",
        "grid": "icon_global_icosahedral",
        "catalog": [
            {"name": "T_2M", "levtype": "single"},
            {"name": "U_10M", "levtype": "single"},
        ],
        **overrides,
    }
    return load_source_dict(payload)


# --- Registry / shipped YAML ----------------------------------------------

def test_icon_registered():
    assert "icon" in list_sources()
    assert get_source("icon").__name__ == "IconSource"


def test_shipped_icon_yaml_loads_with_default_catalog():
    import yaml
    repo_root = Path(__file__).resolve().parent.parent
    raw = yaml.safe_load(
        (repo_root / "config" / "sources" / "dwd_icon.yaml").read_text()
    )
    assert raw["type"] == "icon"
    src = load_source_dict(raw)
    assert type(src).__name__ == "IconSource"
    # No catalog in the YAML → falls back to the built-in renewable set.
    assert len(src.catalog) == len(DEFAULT_ICON_RENEWABLE_CATALOG)
    assert src.supports_byte_range is False


def test_default_catalog_uses_native_dwd_names():
    names = {v.name for v in DEFAULT_ICON_RENEWABLE_CATALOG}
    assert {"U_10M", "V_10M", "T_2M", "ASWDIR_S", "ASWDIFD_S", "CLCT"} <= names


# --- URL construction ------------------------------------------------------

def test_single_level_url():
    src = _make_source()
    assert src._single_url("T_2M", date="20260507", cycle=0, step=6) == (
        "https://dwd.test/icon/grib/00/t_2m/"
        "icon_global_icosahedral_single-level_2026050700_006_T_2M.grib2.bz2"
    )


def test_multi_level_url_inserts_level_token():
    src = _make_source(catalog=[{"name": "T", "levtype": "model", "levels": ["90"]}])
    rec = src.fetch_records(None, date="20260507", cycle=12, step=24)[0]
    assert rec.levelist == "90"
    assert src._record_url(rec, date="20260507", cycle=12, step=24) == (
        "https://dwd.test/icon/grib/12/t/"
        "icon_global_icosahedral_model-level_2026050712_024_90_T.grib2.bz2"
    )


# --- Catalogue → records → selection ---------------------------------------

def test_fetch_records_projects_catalogue():
    src = _make_source()
    recs = src.fetch_records(None, date="20260507", cycle=0, step=0)
    assert [r.param for r in recs] == ["T_2M", "U_10M"]
    assert all(r.levtype == "single" and r.levelist is None for r in recs)


def test_multi_level_catalogue_expands_one_record_per_level():
    src = _make_source(catalog=[{"name": "T", "levtype": "model", "levels": ["89", "90"]}])
    recs = src.fetch_records(None, date="20260507", cycle=0, step=0)
    assert [r.levelist for r in recs] == ["89", "90"]


def test_selection_uses_standard_variable_groups():
    # A job-style group picks a subset by param exactly as for GFS/AIFS.
    from climate_download.config import VariableGroup
    src = _make_source()
    recs = src.fetch_records(None, date="20260507", cycle=0, step=0)
    group = VariableGroup(name="wind_10m", levtype="single", params=["U_10M"])
    selected, breakdown = select_records(recs, [group])
    assert [r.param for r in selected] == ["U_10M"]
    assert breakdown == {"wind_10m": 1}


# --- Download / decompress / concat ---------------------------------------

def _bz2_grib(body: bytes) -> bytes:
    """A minimal valid-looking GRIB skeleton, bz2-compressed."""
    return bz2.compress(b"GRIB" + body + b"7777")


@respx.mock
def test_download_step_decompresses_and_concatenates(tmp_path: Path):
    src = _make_source()
    recs = src.fetch_records(None, date="20260507", cycle=0, step=6)
    t2m = _bz2_grib(b"_T2M_")
    u10 = _bz2_grib(b"_U10M_")
    respx.get(src._record_url(recs[0], date="20260507", cycle=0, step=6)).mock(
        return_value=httpx.Response(200, content=t2m)
    )
    respx.get(src._record_url(recs[1], date="20260507", cycle=0, step=6)).mock(
        return_value=httpx.Response(200, content=u10)
    )
    out = tmp_path / "f006.subset.grib2"
    res = src.download_step(
        None, records=recs, output_path=out, gap_tolerance=0,
        date="20260507", cycle=0, step=6,
    )
    # Output is the decompressed messages concatenated in record order.
    assert out.read_bytes() == bz2.decompress(t2m) + bz2.decompress(u10)
    assert out.read_bytes().startswith(b"GRIB")
    assert out.read_bytes().endswith(b"7777")
    assert res.http_requests == 2
    assert res.bytes_downloaded == len(t2m) + len(u10)  # compressed bytes moved


@respx.mock
def test_download_step_tolerates_per_file_404(tmp_path: Path):
    src = _make_source()
    recs = src.fetch_records(None, date="20260507", cycle=0, step=0)
    t2m = _bz2_grib(b"_T2M_")
    respx.get(src._record_url(recs[0], date="20260507", cycle=0, step=0)).mock(
        return_value=httpx.Response(200, content=t2m)
    )
    respx.get(src._record_url(recs[1], date="20260507", cycle=0, step=0)).mock(
        return_value=httpx.Response(404)
    )
    out = tmp_path / "f000.subset.grib2"
    res = src.download_step(
        None, records=recs, output_path=out, gap_tolerance=0,
        date="20260507", cycle=0, step=0,
    )
    assert out.read_bytes() == bz2.decompress(t2m)  # only the present file
    assert res.bytes_downloaded == len(t2m)


@respx.mock
def test_download_step_raises_when_all_missing(tmp_path: Path):
    src = _make_source()
    recs = src.fetch_records(None, date="20260507", cycle=0, step=0)
    for r in recs:
        respx.get(src._record_url(r, date="20260507", cycle=0, step=0)).mock(
            return_value=httpx.Response(404)
        )
    out = tmp_path / "f000.subset.grib2"
    with pytest.raises(RuntimeError, match="no files downloaded"):
        src.download_step(
            None, records=recs, output_path=out, gap_tolerance=0,
            date="20260507", cycle=0, step=0,
        )
    assert not out.exists()  # no empty file left behind


# --- Availability ----------------------------------------------------------

@respx.mock
def test_probe_step_head():
    src = _make_source()
    ok = src._single_url("T_2M", date="20260507", cycle=0, step=6)
    miss = src._single_url("T_2M", date="20260507", cycle=0, step=240)
    respx.head(ok).mock(return_value=httpx.Response(200))
    respx.head(miss).mock(return_value=httpx.Response(404))
    with httpx.Client() as client:
        assert src.probe_step(client, date="20260507", cycle=0, step=6) is True
        assert src.probe_step(client, date="20260507", cycle=0, step=240) is False


@respx.mock
def test_list_available_steps_parses_autoindex_and_filters_date():
    src = _make_source()
    grid = src.grid
    html = (
        '<a href="../">../</a>\n'
        f'<a href="{grid}_single-level_2026050700_000_T_2M.grib2.bz2">x</a>\n'
        f'<a href="{grid}_single-level_2026050700_006_T_2M.grib2.bz2">x</a>\n'
        f'<a href="{grid}_single-level_2026050700_012_T_2M.grib2.bz2">x</a>\n'
        # different date — must be ignored:
        f'<a href="{grid}_single-level_2026050600_018_T_2M.grib2.bz2">x</a>\n'
    )
    respx.get("https://dwd.test/icon/grib/00/t_2m/").mock(
        return_value=httpx.Response(200, text=html)
    )
    with httpx.Client() as client:
        steps = src.list_available_steps(client, date="20260507", cycle=0)
    assert steps == [0, 6, 12]


@respx.mock
def test_list_available_steps_none_when_empty():
    src = _make_source()
    respx.get("https://dwd.test/icon/grib/00/t_2m/").mock(
        return_value=httpx.Response(200, text="<a href='../'>../</a>")
    )
    with httpx.Client() as client:
        assert src.list_available_steps(client, date="20260507", cycle=0) is None
