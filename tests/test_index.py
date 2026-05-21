"""Tests for climate_download.grib.index."""

from __future__ import annotations

from pathlib import Path

import pytest

from climate_download.grib.index import (
    ByteRange,
    IndexFilter,
    IndexRecord,
    filter_records,
    merge_ranges,
    parse_index,
    parse_index_text,
    parse_wgrib2_idx_text,
)


# --- parse_index_text -------------------------------------------------------


def test_parse_index_text_basic() -> None:
    text = (
        '{"param":"2t","levtype":"sfc","step":"0","_offset":0,"_length":100}\n'
        '{"param":"u","levtype":"pl","levelist":"850","step":"0",'
        '"_offset":100,"_length":200}\n'
    )
    records = parse_index_text(text)
    assert len(records) == 2
    assert records[0].param == "2t"
    assert records[0].levtype == "sfc"
    assert records[0].levelist is None
    assert records[0].offset == 0
    assert records[0].length == 100
    assert records[0].end == 100
    assert records[1].levelist == "850"


def test_parse_index_text_skips_blank_lines() -> None:
    text = (
        '\n'
        '{"param":"2t","levtype":"sfc","_offset":0,"_length":10}\n'
        '   \n'
    )
    assert len(parse_index_text(text)) == 1


def test_parse_index_text_invalid_json() -> None:
    with pytest.raises(ValueError, match="line 2"):
        parse_index_text(
            '{"param":"2t","levtype":"sfc","_offset":0,"_length":10}\n'
            '{not json}\n'
        )


def test_parse_index_text_coerces_numeric_levelist() -> None:
    rec = parse_index_text(
        '{"param":"u","levtype":"pl","levelist":850,'
        '"step":0,"_offset":0,"_length":10}\n'
    )[0]
    assert rec.levelist == "850"
    assert rec.step == "0"


# --- IndexFilter ------------------------------------------------------------


def _rec(**kw: object) -> IndexRecord:
    base = {
        "param": "u",
        "levtype": "pl",
        "levelist": "850",
        "step": "0",
        "_offset": 0,
        "_length": 10,
    }
    base.update(kw)
    return IndexRecord.model_validate(base)


def test_filter_empty_selector_keeps_everything() -> None:
    records = [_rec(), _rec(param="v"), _rec(levtype="sfc", levelist=None)]
    assert filter_records(records, IndexFilter()) == records


def test_filter_by_param_levtype_level() -> None:
    records = [
        _rec(param="u", levelist="850"),
        _rec(param="u", levelist="500"),
        _rec(param="v", levelist="850"),
        _rec(param="2t", levtype="sfc", levelist=None),
    ]
    selector = IndexFilter(params=["u", "v"], levtypes=["pl"], levels=["850"])
    selected = filter_records(records, selector)
    assert [r.param for r in selected] == ["u", "v"]


def test_filter_excludes_records_without_required_levelist() -> None:
    records = [_rec(param="2t", levtype="sfc", levelist=None)]
    selector = IndexFilter(levels=["850"])
    assert filter_records(records, selector) == []


# --- merge_ranges -----------------------------------------------------------


def test_merge_ranges_contiguous() -> None:
    records = [_rec(_offset=0, _length=10), _rec(_offset=10, _length=20)]
    assert merge_ranges(records) == [ByteRange(start=0, end=30)]


def test_merge_ranges_with_gap_and_tolerance() -> None:
    records = [
        _rec(_offset=0, _length=10),
        _rec(_offset=20, _length=10),  # 10-byte gap
        _rec(_offset=100, _length=10),  # 70-byte gap
    ]
    no_tol = merge_ranges(records)
    assert no_tol == [
        ByteRange(start=0, end=10),
        ByteRange(start=20, end=30),
        ByteRange(start=100, end=110),
    ]
    with_tol = merge_ranges(records, gap_tolerance=10)
    assert with_tol == [ByteRange(start=0, end=30), ByteRange(start=100, end=110)]


def test_merge_ranges_unsorted_input() -> None:
    records = [_rec(_offset=50, _length=10), _rec(_offset=0, _length=50)]
    assert merge_ranges(records) == [ByteRange(start=0, end=60)]


def test_merge_ranges_rejects_negative_tolerance() -> None:
    with pytest.raises(ValueError):
        merge_ranges([_rec()], gap_tolerance=-1)


# --- real .index sample -----------------------------------------------------


def test_parse_real_aifs_index(aifs_index_path: Path) -> None:
    records = parse_index(aifs_index_path)
    assert len(records) == 106
    levtypes = {r.levtype for r in records}
    assert levtypes == {"sfc", "pl", "sol"}
    # Messages must form a contiguous, non-overlapping sequence.
    ordered = sorted(records, key=lambda r: r.offset)
    for prev, curr in zip(ordered, ordered[1:]):
        assert curr.offset == prev.end
    # Selecting 850/925 hPa wind components should give 4 records.
    selector = IndexFilter(
        params=["u", "v"], levtypes=["pl"], levels=["850", "925"]
    )
    wind = filter_records(records, selector)
    assert len(wind) == 4
    assert {(r.param, r.levelist) for r in wind} == {
        ("u", "850"), ("v", "850"), ("u", "925"), ("v", "925"),
    }
    # Merging the full set of records collapses to a single byte range.
    merged = merge_ranges(records)
    assert merged == [ByteRange(start=0, end=ordered[-1].end)]



# --- parse_wgrib2_idx_text (NOAA GFS) ---------------------------------------


_GFS_IDX_SAMPLE = (
    "1:0:d=2026050712:PRMSL:mean sea level:6 hour fcst:\n"
    "2:1014443:d=2026050712:UGRD:10 m above ground:6 hour fcst:\n"
    "3:1108460:d=2026050712:VGRD:10 m above ground:6 hour fcst:\n"
    "4:1368422:d=2026050712:TMP:2 m above ground:6 hour fcst:\n"
    "5:1612267:d=2026050712:HGT:500 mb:6 hour fcst:\n"
    "6:1692360:d=2026050712:UGRD:500 mb:6 hour fcst:\n"
    "7:1733108:d=2026050712:VGRD:500 mb:6 hour fcst:\n"
    "8:2563610:d=2026050712:DSWRF:surface:0-6 hour ave fcst:\n"
    "9:3394531:d=2026050712:TSOIL:0-0.1 m below ground:6 hour fcst:\n"
)


def test_parse_wgrib2_idx_basic_fields_and_lengths() -> None:
    total = 4_000_000
    records = parse_wgrib2_idx_text(_GFS_IDX_SAMPLE, total_size=total)
    assert len(records) == 9
    # Spot-check one record from each levtype category.
    by_param = {(r.param, r.levtype, r.levelist): r for r in records}
    assert by_param[("PRMSL", "atm", None)].offset == 0
    assert by_param[("UGRD", "hag", "10")].offset == 1_014_443
    assert by_param[("TMP", "hag", "2")].offset == 1_368_422
    assert by_param[("HGT", "pl", "500")].offset == 1_612_267
    assert by_param[("DSWRF", "sfc", None)].step == "6"
    assert by_param[("TSOIL", "hbg", "0-0.1")].offset == 3_394_531
    # Length of every non-last record equals the gap to the next offset.
    for prev, nxt in zip(records, records[1:]):
        assert prev.length == nxt.offset - prev.offset
    # Last record's length is derived from total_size.
    assert records[-1].length == total - records[-1].offset


def test_parse_wgrib2_idx_preserves_raw_level_desc() -> None:
    """The 5th colon field should land verbatim in IndexRecord.level_desc."""
    records = parse_wgrib2_idx_text(_GFS_IDX_SAMPLE, total_size=4_000_000)
    by_param = {(r.param, r.levtype): r for r in records}
    assert by_param[("PRMSL", "atm")].level_desc == "mean sea level"
    assert by_param[("UGRD", "hag")].level_desc == "10 m above ground"
    assert by_param[("HGT", "pl")].level_desc == "500 mb"
    assert by_param[("DSWRF", "sfc")].level_desc == "surface"
    assert by_param[("TSOIL", "hbg")].level_desc == "0-0.1 m below ground"


def test_parse_index_text_level_desc_defaults_to_none() -> None:
    """AIFS JSONL sidecars do not ship descriptors."""
    text = (
        '{"param":"u","levtype":"pl","levelist":"850","_offset":0,"_length":10}\n'
    )
    rec = parse_index_text(text)[0]
    assert rec.level_desc is None


def test_parse_wgrib2_idx_step_extracted_from_fcst() -> None:
    text = (
        "1:0:d=2026050712:HGT:500 mb:anl:\n"
        "2:100:d=2026050712:HGT:500 mb:24 hour fcst:\n"
        "3:200:d=2026050712:APCP:surface:24-30 hour acc fcst:\n"
    )
    records = parse_wgrib2_idx_text(text, total_size=300)
    assert [r.step for r in records] == ["0", "24", "30"]


def test_parse_wgrib2_idx_rejects_bad_total_size() -> None:
    with pytest.raises(ValueError, match="total_size"):
        parse_wgrib2_idx_text(_GFS_IDX_SAMPLE, total_size=0)


def test_parse_wgrib2_idx_rejects_non_monotonic_offsets() -> None:
    text = (
        "1:0:d=2026050712:HGT:500 mb:anl:\n"
        "2:100:d=2026050712:HGT:500 mb:anl:\n"
        "3:50:d=2026050712:HGT:500 mb:anl:\n"  # goes backwards
    )
    with pytest.raises(ValueError, match="non-monotonic"):
        parse_wgrib2_idx_text(text, total_size=200)


def test_parse_wgrib2_idx_filter_then_merge() -> None:
    records = parse_wgrib2_idx_text(_GFS_IDX_SAMPLE, total_size=4_000_000)
    pl_wind = filter_records(
        records, IndexFilter(params=["UGRD", "VGRD"], levtypes=["pl"])
    )
    assert {(r.param, r.levelist) for r in pl_wind} == {
        ("UGRD", "500"), ("VGRD", "500"),
    }
    merged = merge_ranges(pl_wind)
    # Two records are contiguous in the sample => single merged range.
    assert len(merged) == 1
