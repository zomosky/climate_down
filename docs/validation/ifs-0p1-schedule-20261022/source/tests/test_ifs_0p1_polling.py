"""Before-release polling must be harmless and repeated healthy runs idempotent."""
import json
from pathlib import Path

import httpx
import respx

from climate_download.config import TimeConfig, load_job
from climate_download.jobs import run_job


def polling_job(tmp_path):
    config = load_job(Path(__file__).parents[1] / 'config/jobs/ifs_0p1_renewables_ser.yaml')
    return config.model_copy(update={
        'time': TimeConfig(date='20261022', cycle=0, steps='all'),
        'download': config.download.model_copy(update={
            'output_dir': tmp_path, 'progress_bar': False,
        }),
    })


@respx.mock
def test_unpublished_init_creates_no_manifest_or_grib(tmp_path):
    route = respx.get('https://storage.googleapis.com/ecmwf-open-data/').mock(
        return_value=httpx.Response(200, text=(
            '<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>'
        )))
    result = run_job(polling_job(tmp_path))
    assert result.succeeded == result.failed == []
    assert route.call_count == 2  # surface and pressure listings only
    assert not list(tmp_path.rglob('*.manifest.json'))
    assert not list(tmp_path.rglob('*.grib2'))


@respx.mock
def test_repeated_poll_resumes_and_preserves_identical_manifest(tmp_path):
    entries = ''.join(
        '<Contents><Key>20261022/00z/ifs/0p1/oper/'
        f'20261022000000-0h-oper-fc-{part}.index</Key></Contents>'
        for part in ['sfc', 'pl']
    )
    route = respx.get('https://storage.googleapis.com/ecmwf-open-data/').mock(
        return_value=httpx.Response(200, text=(
            '<ListBucketResult><IsTruncated>false</IsTruncated>'
            + entries + '</ListBucketResult>'
        )))
    raw = tmp_path / 'ifs-hres-0p1/20261022/00z/f000.subset.grib2'
    raw.parent.mkdir(parents=True)
    raw.write_bytes(b'GRIBfixture7777')
    indices = []
    for part, param, levtype in [('sfc', '2t', 'sfc'), ('pl', 't', 'pl')]:
        entry = {'param': param, 'levtype': levtype, 'levelist': '850' if part == 'pl' else None,
                 '_offset': 0, '_length': len(raw.read_bytes()), 'step': '0'}
        indices.append(respx.get(
            f'https://storage.googleapis.com/ecmwf-open-data/20261022/00z/ifs/0p1/oper/'
            f'20261022000000-0h-oper-fc-{part}.index',
        ).mock(return_value=httpx.Response(200, text=json.dumps(entry))))
    config = polling_job(tmp_path)
    first = run_job(config)
    assert len(first.succeeded) == 1 and first.succeeded[0].resumed
    assert not first.failed
    manifest = next(tmp_path.rglob('*.manifest.json'))
    before = manifest.read_bytes(), manifest.stat().st_mtime_ns
    second = run_job(config)
    assert len(second.succeeded) == 1 and second.succeeded[0].resumed
    assert not second.failed
    assert (manifest.read_bytes(), manifest.stat().st_mtime_ns) == before
    assert json.loads(before[0])['files'][0]['step_hours'] == 0
    assert raw.read_bytes() == b'GRIBfixture7777'
    assert route.call_count == 4
    assert all(index.call_count == 2 for index in indices)
    # Existing downloader refreshes sidecars on resume, but never downloads the GRIB again.
