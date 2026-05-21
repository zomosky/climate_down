"""Unit tests for the new fault-tolerance / resume / listing plumbing.

These tests cover the pure helpers added in the gfsdown-inspired refactor:

* ``_resume_check`` (GRIB header/footer marker check on disk)
* ``_resolve_output_path`` (subdir_template + filename_template composition)
* ``TimeConfig.expanded_steps`` accepting the literal ``"all"``
* ``sources._listing.list_s3_steps`` (S3 LIST XML pagination + step regex)
* ``JobOutcome`` aggregation flags

Network access is avoided throughout (respx mocks the S3 LIST endpoint).
"""

from __future__ import annotations

import httpx
import pytest
import respx

from climate_download.config import DownloadConfig, TimeConfig
from climate_download.jobs import (
    JobFailure,
    JobOutcome,
    JobResult,
    _resolve_output_path,
    _resume_check,
)
from climate_download.sources._listing import list_s3_steps


# --- _resume_check ---------------------------------------------------------

def test_resume_check_missing_file(tmp_path):
    assert _resume_check(tmp_path / "absent.grib2") is False


def test_resume_check_valid_grib(tmp_path):
    p = tmp_path / "ok.grib2"
    p.write_bytes(b"GRIB" + b"\0" * 64 + b"7777")
    assert _resume_check(p) is True


def test_resume_check_missing_header(tmp_path):
    p = tmp_path / "bad_head.grib2"
    p.write_bytes(b"XXXX" + b"\0" * 64 + b"7777")
    assert _resume_check(p) is False


def test_resume_check_missing_trailer(tmp_path):
    p = tmp_path / "bad_tail.grib2"
    p.write_bytes(b"GRIB" + b"\0" * 64 + b"ABCD")
    assert _resume_check(p) is False


# --- _resolve_output_path --------------------------------------------------

def test_resolve_output_path_default_layout(tmp_path):
    """Default DownloadConfig should land at <out>/<source>/<date>/<cycle>z/f<step>."""
    dl = DownloadConfig(output_dir=tmp_path)
    out = _resolve_output_path(dl, source_name="gfs", date="20260101",
                               cycle=12, step=6)
    assert out == tmp_path / "gfs" / "20260101" / "12z" / "f006.subset.grib2"


def test_resolve_output_path_no_subdir(tmp_path):
    """Empty subdir_template flattens everything next to output_dir."""
    dl = DownloadConfig(output_dir=tmp_path,
                        subdir_template="",
                        filename_template="{source}_f{step:03d}.grib2")
    out = _resolve_output_path(dl, source_name="gfs", date="20260101",
                               cycle=0, step=6)
    assert out == tmp_path / "gfs_f006.grib2"


def test_resolve_output_path_with_custom_subdir(tmp_path):
    dl = DownloadConfig(
        output_dir=tmp_path,
        subdir_template="{date}/{cycle:02d}z",
        filename_template="{source}_f{step:03d}.grib2",
    )
    out = _resolve_output_path(dl, source_name="gfs", date="20260101",
                               cycle=12, step=6)
    assert out == tmp_path / "20260101" / "12z" / "gfs_f006.grib2"


# --- TimeConfig steps: all -------------------------------------------------

def test_timeconfig_steps_all_string():
    t = TimeConfig(date="20260101", cycle=0, steps="all")
    assert t.expanded_steps() is None


def test_timeconfig_steps_all_case_insensitive():
    t = TimeConfig(date="20260101", cycle=0, steps="ALL")
    assert t.expanded_steps() is None


def test_timeconfig_steps_range_still_works():
    t = TimeConfig(date="20260101", cycle=0, steps="0-12:6")
    assert t.expanded_steps() == [0, 6, 12]


# --- JobOutcome ------------------------------------------------------------

def test_job_outcome_all_failed():
    o = JobOutcome(failed=[JobFailure(date="20260101", cycle=0, step=0,
                                      phase="download", error="boom")])
    assert o.all_failed is True
    assert o.total == 1


def test_job_outcome_partial_not_all_failed(tmp_path):
    r = JobResult(date="20260101", cycle=0, step=0,
                  output_path=tmp_path / "x.grib2",
                  bytes_total=10, bytes_downloaded=5,
                  records_total=1, records_selected=1, http_requests=1)
    o = JobOutcome(succeeded=[r],
                   failed=[JobFailure(date="20260101", cycle=0, step=6,
                                      phase="download", error="boom")])
    assert o.all_failed is False
    assert o.total == 2


# --- list_s3_steps ---------------------------------------------------------

_S3_LIST_XML = """<?xml version='1.0' encoding='UTF-8'?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>noaa-gfs-bdp-pds</Name>
  <Prefix>gfs.20260101/00/atmos/</Prefix>
  <IsTruncated>false</IsTruncated>
  <Contents><Key>gfs.20260101/00/atmos/gfs.t00z.pgrb2.0p25.f000.idx</Key></Contents>
  <Contents><Key>gfs.20260101/00/atmos/gfs.t00z.pgrb2.0p25.f000</Key></Contents>
  <Contents><Key>gfs.20260101/00/atmos/gfs.t00z.pgrb2.0p25.f006.idx</Key></Contents>
  <Contents><Key>gfs.20260101/00/atmos/gfs.t00z.pgrb2.0p25.f012.idx</Key></Contents>
</ListBucketResult>
"""


@respx.mock
def test_list_s3_steps_gfs_pattern():
    template = (
        "https://noaa-gfs-bdp-pds.s3.amazonaws.com/"
        "gfs.{date}/{cycle:02d}/atmos/gfs.t{cycle:02d}z.pgrb2.0p25.f{step:03d}.idx"
    )
    respx.get("https://noaa-gfs-bdp-pds.s3.amazonaws.com/").mock(
        return_value=httpx.Response(200, text=_S3_LIST_XML)
    )
    with httpx.Client() as c:
        steps = list_s3_steps(c, index_url_template=template,
                              date="20260101", cycle=0)
    assert steps == [0, 6, 12]


def test_list_s3_steps_none_when_no_step_placeholder():
    template = "https://h.test/static.idx"
    with httpx.Client() as c:
        assert list_s3_steps(c, index_url_template=template,
                             date="20260101", cycle=0) is None
