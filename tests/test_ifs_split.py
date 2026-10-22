"""IFS split-file contracts: offsets are local to each physical file."""
from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
import respx

from climate_download.config import load_job, load_source_dict
from climate_download.grib.partial import PartialDownloader, PartialDownloadError


def source():
    return load_source_dict({
        "type": "ifs-split", "name": "ifs-hres-0p1",
        "url_template": "https://example.test/{date}/{cycle:02d}z/f{step}-{levtype}.{suffix}",
    })


def index(param, levtype, payload):
    return json.dumps({"param": param, "levtype": levtype, "levelist": "850" if levtype == "pl" else None,
                       "_offset": 0, "_length": len(payload), "step": "3"})


def mock_indices():
    sfc, pl = b"GRIBsurface7777", b"GRIBpressure7777"
    for part, param, body in (("sfc", "2t", sfc), ("pl", "u", pl)):
        respx.get(f"https://example.test/20261020/12z/f3-{part}.index").mock(
            return_value=httpx.Response(200, text=index(param, part, body)))
    return sfc, pl


@respx.mock
def test_same_offset_from_two_files_is_downloaded_and_concatenated(tmp_path):
    src = source()
    sfc, pl = mock_indices()
    for part, body in (("sfc", sfc), ("pl", pl)):
        respx.get(f"https://example.test/20261020/12z/f3-{part}.grib2",
                  headers={"range": f"bytes=0-{len(body)-1}"}).mock(
                      return_value=httpx.Response(206, content=body))
    with httpx.Client() as client, PartialDownloader(client=client) as downloader:
        records = src.fetch_records(client, date="20261020", cycle=12, step=3)
        out = tmp_path / "f003.subset.grib2"
        result = src.download_step(downloader, records=records, output_path=out,
                                   gap_tolerance=0, date="20261020", cycle=12, step=3)
    assert out.read_bytes() == sfc + pl
    assert result.bytes_downloaded == len(sfc) + len(pl)
    assert result.http_requests == 2


@respx.mock
def test_surface_selection_never_downloads_pressure_payload(tmp_path):
    src = source()
    sfc, _ = mock_indices()
    respx.get("https://example.test/20261020/12z/f3-sfc.grib2").mock(
        return_value=httpx.Response(206, content=sfc))
    with httpx.Client() as client, PartialDownloader(client=client) as downloader:
        records = src.fetch_records(client, date="20261020", cycle=12, step=3)
        src.download_step(downloader, records=[r for r in records if r.levtype == "sfc"],
                          output_path=tmp_path / "surface.grib2", gap_tolerance=0,
                          date="20261020", cycle=12, step=3)
    assert (tmp_path / "surface.grib2").read_bytes() == sfc


@respx.mock
def test_second_file_failure_does_not_publish_partial_output(tmp_path):
    src = source()
    sfc, _ = mock_indices()
    respx.get("https://example.test/20261020/12z/f3-sfc.grib2").mock(
        return_value=httpx.Response(206, content=sfc))
    respx.get("https://example.test/20261020/12z/f3-pl.grib2").mock(return_value=httpx.Response(500))
    out = tmp_path / "f003.subset.grib2"
    out.write_bytes(b"existing product")
    with httpx.Client() as client, PartialDownloader(client=client, max_attempts=1) as downloader:
        records = src.fetch_records(client, date="20261020", cycle=12, step=3)
        with pytest.raises(PartialDownloadError):
            src.download_step(downloader, records=records, output_path=out, gap_tolerance=0,
                              date="20261020", cycle=12, step=3)
    assert out.read_bytes() == b"existing product"
    assert list(tmp_path.iterdir()) == [out]


@respx.mock
@pytest.mark.parametrize("statuses,expected", [((404, 404), False), ((200, 404), True), ((404, 200), True)])
def test_one_published_part_is_attempted_so_missing_part_becomes_failure(statuses, expected):
    src = source()
    for part, status in zip(("sfc", "pl"), statuses):
        respx.head(f"https://example.test/20261020/12z/f3-{part}.index").mock(
            return_value=httpx.Response(status))
    with httpx.Client() as client:
        assert src.probe_step(client, date="20261020", cycle=12, step=3) is expected


def test_listing_uses_union_to_expose_a_missing_part(monkeypatch):
    def list_steps(client, *, index_url_template, date, cycle):
        return [0, 3] if "-sfc." in index_url_template else [0, 6]
    monkeypatch.setattr("climate_download.sources.ifs_split.list_remote_steps", list_steps)
    with httpx.Client() as client:
        assert source().list_available_steps(client, date="20261020", cycle=12) == [0, 3, 6]


def test_shipped_speed_job_is_independent_and_bounded():
    cfg = load_job(Path(__file__).parents[1] / "config/jobs/ifs_0p1_renewables.yaml")
    assert cfg.source.name == "ifs-hres-0p1"
    assert "/ifs/0p1/" in cfg.source.url_template
    assert cfg.time.expanded_steps() == [3]
    assert cfg.download.output_dir == Path("output")
    assert any(group.levtype == "pl" and group.levels == ["850"] for group in cfg.variables)
