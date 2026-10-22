"""Probability bundles must be complete before manifest publication."""
import json
from pathlib import Path
import struct

import httpx
import pytest
import respx

from climate_download.config import load_job, load_source_dict


def source(model="ifs"):
    return load_source_dict({
        "type": "ecmwf-probabilities", "name": f"{model}-probabilities",
        "model": model, "url_template": "https://example.test/{date}/{cycle:02d}/{step}.{suffix}",
    })


def rows(model="ifs", bundle=240):
    from climate_download.sources.ecmwf_probabilities import expected_windows
    return [dict(param=param, levtype="sfc", type="ep", step=step,
                 _offset=i * 16, _length=16)
            for i, (param, step) in enumerate(
                (p, s) for p, steps in expected_windows(model, bundle).items() for s in sorted(steps))]


@respx.mock
@pytest.mark.parametrize("model,bundle,count", [("ifs",240,95),("ifs",360,50),
                                                ("aifs-ens",240,208),("aifs-ens",360,105)])
def test_probability_catalog(model, bundle, count):
    records = rows(model, bundle)
    records.append(dict(param="tpg50", levtype="sfc", type="em", step="24", _offset=99999, _length=16))
    respx.get(f"https://example.test/20261015/00/{bundle}.index").respond(
        text="\n".join(json.dumps(r) for r in records))
    with httpx.Client() as client:
        got = source(model).fetch_records(client, date="20261015", cycle=0, step=bundle)
    assert len(got) == count
    assert all(r.model_extra["type"] == "ep" for r in got)


@respx.mock
def test_missing_window_is_not_ready():
    respx.get("https://example.test/20261015/00/240.index").respond(
        text="\n".join(json.dumps(r) for r in rows()[:-1]))
    with httpx.Client() as client, pytest.raises(ValueError, match="incomplete"):
        source().fetch_records(client,date="20261015",cycle=0,step=240)


def test_both_bundles_always_attempted_even_before_publication():
    with httpx.Client() as client:
        assert source().list_available_steps(client,date="20261015",cycle=0) == [240,360]
        assert source().probe_step(client,date="20261015",cycle=0,step=360)
    with pytest.raises(ValueError, match="00/12"):
        source().build_index_url(date="20261015",cycle=6,step=240)


@pytest.mark.parametrize("name,model,params", [
    ("ifs_ens_probabilities","ifs",{"tpg20","tpg50","tpg100","10fgg15","10fgg25"}),
    ("aifs_ens_probabilities","aifs-ens",{"tpg1","tpg5","tpg10","tpg20","tpg25","tpg50","tpg100",
                                       "10spg10","10spg15","2tl273","tpl01","tprl1","tprg3","tprg5"}),
])
def test_shipped_jobs(name, model, params):
    root=Path(__file__).parents[1]
    for suffix in ("", "_ser"):
        job=load_job(root/f"config/jobs/{name}{suffix}.yaml")
        assert job.source.model == model
        assert {p for g in job.variables for p in g.params} == params
        assert job.time.expanded_steps() is None
        assert job.download.init_concurrency == 1


def test_missing_bundle_stays_a_manifest_failure(tmp_path, monkeypatch):
    from climate_download.jobs import JobResult, run_job
    from climate_download.config import JobConfig, DownloadConfig, TimeConfig, VariableGroup
    def one_step(src, variables, download, date, cycle, step, **kwargs):
        if step == 360:
            raise RuntimeError("second bundle not published")
        path=tmp_path/"f240.subset.grib2"
        path.write_bytes(b"GRIB"+b"0"*24+b"7777")
        return JobResult(date=date,cycle=cycle,step=step,output_path=path,
                         bytes_total=32,bytes_downloaded=32,records_total=95,records_selected=95,http_requests=1)
    monkeypatch.setattr("climate_download.jobs._run_one_step",one_step)
    config=JobConfig(source=source(),time=TimeConfig(date="20261015",cycle=0,steps="all"),
        variables=[VariableGroup(name="events",levtype="sfc",params=["tpg50"])],
        download=DownloadConfig(output_dir=tmp_path))
    outcome=run_job(config)
    assert len(outcome.failed)==1 and outcome.failed[0].step==360
    manifest=json.loads(next(tmp_path.rglob("*.manifest.json")).read_text())
    assert [f["step_hours"] for f in manifest["files"]]==[240]
    assert manifest["failures"][0]["step"]==360


def reset_index(model="aifs-ens", bundle=240):
    """Mean, probability and spread indexes restart at zero in one GRIB."""
    events = rows(model, bundle)
    for i, event in enumerate(events):
        event.update(_offset=i * 24, _length=24)
    mean = dict(param="tpg50", levtype="sfc", type="em", step="24", _offset=0, _length=24)
    spread = dict(mean, type="es")
    return [mean, *events, spread]


@respx.mock
@pytest.mark.parametrize("model", ["ifs", "aifs-ens"])
def test_reset_offsets_are_rebuilt_before_selecting_probability_messages(model):
    records = reset_index(model)
    url = "https://example.test/20261015/00/240"
    respx.get(url + ".index").respond(text="\n".join(json.dumps(r) for r in records))
    respx.head(url + ".grib2").respond(headers={"Content-Length": str(len(records) * 24)})
    with httpx.Client() as client:
        selected = source(model).fetch_records(client, date="20261015", cycle=0, step=240)
    assert [r.offset for r in selected] == list(range(24, (len(records) - 1) * 24, 24))
    assert all(r.model_extra["type"] == "ep" for r in selected)


@respx.mock
def test_reset_offsets_with_incomplete_object_coverage_fail_before_download():
    records = reset_index()
    url = "https://example.test/20261015/00/240"
    respx.get(url + ".index").respond(text="\n".join(json.dumps(r) for r in records))
    respx.head(url + ".grib2").respond(headers={"Content-Length": str(len(records) * 24 + 1)})
    with httpx.Client() as client, pytest.raises(ValueError, match="lengths"):
        source("aifs-ens").fetch_records(client, date="20261015", cycle=0, step=240)


@respx.mock
def test_reset_index_download_excludes_mean_and_spread_and_keeps_grib_boundaries(tmp_path):
    from climate_download.grib.partial import PartialDownloader
    from climate_download.probability_history import validate_subset

    records = reset_index()
    message = lambda payload: b"GRIB\0\0\0\2" + struct.pack(">Q", 24) + payload + b"7777"
    probability = message(b"prob") * (len(records) - 2)
    obj = message(b"mean") + probability + message(b"sprd")
    url = "https://example.test/20261015/00/240"
    respx.get(url + ".index").respond(text="\n".join(json.dumps(r) for r in records))
    respx.head(url + ".grib2").respond(headers={"Content-Length": str(len(obj))})
    requests = []
    def download(request):
        requests.append(request.headers["range"])
        start, end = map(int, request.headers["range"].removeprefix("bytes=").split("-"))
        return httpx.Response(206, content=obj[start:end + 1],
                              headers={"Content-Range": f"bytes {start}-{end}/{len(obj)}"})
    respx.get(url + ".grib2").mock(side_effect=download)
    path = tmp_path / "probability.grib2"
    with httpx.Client() as client, PartialDownloader(client, max_workers=1) as downloader:
        src = source("aifs-ens")
        selected = src.fetch_records(client, date="20261015", cycle=0, step=240)
        src.download_step(downloader, records=selected, output_path=path, gap_tolerance=0,
                          date="20261015", cycle=0, step=240)
    assert path.read_bytes() == probability
    assert requests == [f"bytes=24-{len(obj) - 25}"]
    validate_subset(path, selected)
