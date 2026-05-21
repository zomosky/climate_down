"""Tests for climate_download.grib.partial."""

from __future__ import annotations

import re
from pathlib import Path

import httpx
import pytest
import respx

from climate_download.grib.index import ByteRange
from climate_download.grib.partial import PartialDownloader, PartialDownloadError

URL = "https://example.test/forecast.grib2"


def _serve_range(blob: bytes) -> "respx.Route":
    """Install a respx route that serves any ``Range`` request against ``blob``."""

    def _handler(request: httpx.Request) -> httpx.Response:
        header = request.headers.get("range", "")
        match = re.fullmatch(r"bytes=(\d+)-(\d+)", header)
        if not match:
            return httpx.Response(400, text=f"bad range: {header!r}")
        start, end_inclusive = int(match.group(1)), int(match.group(2))
        end = end_inclusive + 1
        if start < 0 or end > len(blob):
            return httpx.Response(416)
        return httpx.Response(
            206,
            content=blob[start:end],
            headers={"Content-Range": f"bytes {start}-{end_inclusive}/{len(blob)}"},
        )

    return respx.get(URL).mock(side_effect=_handler)


@respx.mock
def test_download_single_range_writes_exact_bytes(
    tmp_path: Path, synthetic_grib_bytes: bytes
) -> None:
    _serve_range(synthetic_grib_bytes)
    out = tmp_path / "subset.grib2"
    with PartialDownloader(max_workers=1) as dl:
        written = dl.download(URL, [ByteRange(start=10, end=110)], out)
    assert written == 100
    assert out.read_bytes() == synthetic_grib_bytes[10:110]


@respx.mock
def test_download_multiple_ranges_concatenates_in_offset_order(
    tmp_path: Path, synthetic_grib_bytes: bytes
) -> None:
    _serve_range(synthetic_grib_bytes)
    ranges = [
        ByteRange(start=200, end=260),
        ByteRange(start=0, end=50),
        ByteRange(start=500, end=520),
    ]
    out = tmp_path / "multi.grib2"
    with PartialDownloader(max_workers=3) as dl:
        written = dl.download(URL, ranges, out)
    expected = (
        synthetic_grib_bytes[0:50]
        + synthetic_grib_bytes[200:260]
        + synthetic_grib_bytes[500:520]
    )
    assert written == len(expected)
    assert out.read_bytes() == expected


@respx.mock
def test_download_creates_parent_directory(
    tmp_path: Path, synthetic_grib_bytes: bytes
) -> None:
    _serve_range(synthetic_grib_bytes)
    out = tmp_path / "nested" / "dir" / "subset.grib2"
    with PartialDownloader() as dl:
        dl.download(URL, [ByteRange(start=0, end=8)], out)
    assert out.is_file()


@respx.mock
def test_download_retries_on_transient_5xx(
    tmp_path: Path, synthetic_grib_bytes: bytes
) -> None:
    calls = {"n": 0}

    def _flaky(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(503)
        return httpx.Response(
            206,
            content=synthetic_grib_bytes[0:32],
            headers={"Content-Range": f"bytes 0-31/{len(synthetic_grib_bytes)}"},
        )

    respx.get(URL).mock(side_effect=_flaky)
    out = tmp_path / "retry.grib2"
    with PartialDownloader(max_workers=1, max_attempts=4) as dl:
        dl.download(URL, [ByteRange(start=0, end=32)], out)
    assert calls["n"] == 3
    assert out.read_bytes() == synthetic_grib_bytes[0:32]


@respx.mock
def test_download_raises_on_persistent_failure(tmp_path: Path) -> None:
    respx.get(URL).mock(return_value=httpx.Response(500))
    with PartialDownloader(max_workers=1, max_attempts=2) as dl:
        with pytest.raises(PartialDownloadError):
            dl.download(URL, [ByteRange(start=0, end=8)], tmp_path / "x.grib2")


@respx.mock
def test_download_detects_short_read(
    tmp_path: Path, synthetic_grib_bytes: bytes
) -> None:
    # Server returns fewer bytes than requested; downloader must complain
    # rather than silently producing a truncated GRIB file.
    respx.get(URL).mock(
        return_value=httpx.Response(
            206,
            content=synthetic_grib_bytes[0:10],
            headers={"Content-Range": f"bytes 0-31/{len(synthetic_grib_bytes)}"},
        )
    )
    with PartialDownloader(max_workers=1, max_attempts=1) as dl:
        with pytest.raises(PartialDownloadError, match="short read"):
            dl.download(URL, [ByteRange(start=0, end=32)], tmp_path / "x.grib2")


def test_download_rejects_empty_ranges(tmp_path: Path) -> None:
    with PartialDownloader() as dl:
        with pytest.raises(ValueError):
            dl.download(URL, [], tmp_path / "x.grib2")
