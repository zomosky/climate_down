import importlib.util
import json
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest


@pytest.fixture
def guard(monkeypatch):
    directory = Path(__file__).parents[1] / 'deploy'
    monkeypatch.syspath_prepend(str(directory))
    spec = importlib.util.spec_from_file_location('download_ifs_0p1', directory / 'download_ifs_0p1.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('date', ['2026-10-22', '2026-11-01', '2027-01-01'])
@pytest.mark.parametrize('cycle,hour,minute', [(12, 4, 5), (12, 7, 55), (0, 16, 5), (0, 19, 55)])
def test_early_and_last_retries_select_the_same_correct_utc_init(guard, tmp_path, date, cycle, hour, minute):
    local = datetime.fromisoformat(date).replace(hour=hour, minute=minute, tzinfo=ZoneInfo('Asia/Shanghai'))
    expected = local.date() - timedelta(days=1 if cycle == 12 else 0)
    calls = []
    def execute(argv, **kwargs):
        calls.append((argv, kwargs))
        return SimpleNamespace(returncode=0)
    assert guard.run_target(cycle=cycle, now=local, data_root=tmp_path,
                            runner=Path('/operation/download_run.sh'), execute=execute) == 0
    assert len(calls) == 1
    argv, kwargs = calls[0]
    assert argv == ['/operation/download_run.sh', 'config/jobs/ifs_0p1_renewables_ser.yaml']
    assert kwargs['env']['INIT_DATE'] == expected.strftime('%Y%m%d')
    assert kwargs['env']['CYCLE'] == str(cycle)
    assert kwargs['env']['LOOKBACK_DAYS'] == '0'
    assert 'LOCK' not in kwargs['env']  # retain the existing runner's job/cycle lock


def seed_manifest(guard, root, count=85, failures=None):
    policy = next(p for p in guard.POLICIES if p.source == 'ifs-hres-0p1' and p.cycle == 12)
    path = guard.manifest_path(root, policy, '20261021')
    path.parent.mkdir(parents=True)
    records = []
    for lead in policy.expected_steps[:count]:
        raw = path.parent / f'f{lead:03d}.subset.grib2'
        raw.write_bytes(b'GRIB7777')
        records.append({'step_hours': lead, 'path': str(raw), 'size_bytes': 8})
    path.write_text(json.dumps({'completed_at': '2026-10-21T23:30:00Z',
                                'failures': failures or [], 'files': records}))
    return path, records


def test_healthy_init_skips_runner_and_preserves_manifest(guard, tmp_path):
    path, _ = seed_manifest(guard, tmp_path)
    before = path.read_bytes(), path.stat().st_mtime_ns
    def execute(*args, **kwargs):
        pytest.fail('healthy init must not re-fetch indices or run downloader')
    assert guard.run_target(cycle=12, now=datetime.fromisoformat('2026-10-22T06:05:00+08:00'),
                            data_root=tmp_path, runner=Path('/runner'), execute=execute) == 0
    assert (path.read_bytes(), path.stat().st_mtime_ns) == before


@pytest.mark.parametrize('problem', ['partial', 'missing_raw', 'wrong_size', 'failure'])
def test_unhealthy_init_delegates_recovery_and_propagates_runner_failure(guard, tmp_path, problem):
    _, records = seed_manifest(guard, tmp_path, count=84 if problem == 'partial' else 85,
                               failures=[{'phase': 'download'}] if problem == 'failure' else None)
    if problem == 'missing_raw':
        Path(records[0]['path']).unlink()
    elif problem == 'wrong_size':
        Path(records[0]['path']).write_bytes(b'GRIB')
    calls = []
    def execute(argv, **kwargs):
        calls.append(argv)
        return SimpleNamespace(returncode=2)
    assert guard.run_target(cycle=12, now=datetime.fromisoformat('2026-10-22T06:05:00+08:00'),
                            data_root=tmp_path, runner=Path('/runner'), execute=execute) == 2
    assert len(calls) == 1


def test_prelaunch_and_dry_run_never_start_a_download(guard, tmp_path):
    def execute(*args, **kwargs):
        pytest.fail('no download expected')
    for now, dry_run in [('2026-10-20T04:05:00+08:00', False), ('2026-10-22T04:05:00+08:00', True)]:
        assert guard.run_target(cycle=12, now=datetime.fromisoformat(now), data_root=tmp_path,
                                runner=Path('/runner'), dry_run=dry_run, execute=execute) == 0


def test_fixed_clock_cannot_be_used_for_a_real_download(guard):
    with pytest.raises(SystemExit) as error:
        guard.main(['--cycle', '12', '--now', '2026-10-22T04:05:00+08:00'])
    assert error.value.code == 2
