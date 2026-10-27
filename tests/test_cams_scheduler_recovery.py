"""Terminal ADS failures must not starve later live inits or bounded recovery."""
import datetime as dt
import json
from types import SimpleNamespace

import pytest

from climate_download import cli
from climate_download.cams import worker
from climate_download.cams.config import CamsConfig
from climate_download.cams.state import save


@pytest.fixture
def config(tmp_path):
    return CamsConfig(output_dir=tmp_path, accounts=[{'name': 'a', 'credential_file': tmp_path/'credential'}],
                      cycle_accounts={0: 'a', 12: 'a'}, first_init='2026102100', max_run_seconds=0)


def marker(config, date, cycle=0):
    path = config.output_dir/'ec_cams'/date/f'{cycle:02d}z/cams.catchup.json'
    save(path, {'date': date, 'cycle': cycle, 'authorized_at': '2026-10-26T01:00:00+00:00'})
    return path


def test_catchup_survives_recent_window_without_expanding_history(config):
    p = marker(config, '20261024')
    marker(config, '20261020')
    marker(config, '20261029')
    marker(config, '20261024', cycle=12)
    before = p.read_bytes()
    keys = worker.scheduled_inits(config, 0, dt.datetime(2026, 10, 28, 1, tzinfo=dt.UTC))
    assert keys == ['2026102400', '2026102600', '2026102700']
    assert p.read_bytes() == before


def test_published_catchup_leaves_its_receipt_but_no_longer_adds_old_init(config):
    p = marker(config, '20261024')
    save(p.parent/'20261024_00z_ec_cams.manifest.json', {'completed_at': 'done'})
    assert worker.scheduled_inits(config, 0, dt.datetime(2026, 10, 28, 1, tzinfo=dt.UTC)) == ['2026102600', '2026102700']
    assert p.exists()


def test_catchup_rejects_cycle_or_directory_identity_drift(config):
    p = marker(config, '20261024')
    save(p, {'date': '20261025', 'cycle': 0, 'authorized_at': 'recorded'})
    with pytest.raises(ValueError, match='catchup.*identity'):
        worker.scheduled_inits(config, 0, dt.datetime(2026, 10, 28, 1, tzinfo=dt.UTC))


@pytest.mark.parametrize('phase', ['remote_failed', 'validation_failed', 'preflight_rejected'])
def test_terminal_failed_init_does_not_keep_history_waiting(config, phase):
    folder = config.output_dir/'ec_cams/20261024/00z'
    save(folder/'.cams/hourly/state.json', {'date': '20261024', 'cycle': 0, 'phase': phase, 'request_id': 'original'})
    marker(config, '20261024')
    p = folder/'.cams/hourly/state.json'
    before = p.read_bytes()
    assert worker.pending_inits(config, 0, dt.datetime(2026, 10, 26, 1, tzinfo=dt.UTC)) == ['2026102500']
    assert p.read_bytes() == before


def test_unresolved_submission_retains_history_priority_guard(config):
    folder = config.output_dir/'ec_cams/20261024/00z'
    save(folder/'.cams/hourly/state.json', {'date': '20261024', 'cycle': 0, 'phase': 'submission_uncertain'})
    assert worker.pending_inits(config, 0, dt.datetime(2026, 10, 26, 1, tzinfo=dt.UTC)) == ['2026102400', '2026102500']


def args(scheduled=True):
    return SimpleNamespace(config='unused', once=True, date=None if scheduled else '20261023',
                           cycle=0, scheduled=scheduled, preflight_only=False)


def test_scheduled_terminal_failure_preserves_error_and_continues_next_init(config, monkeypatch, capsys):
    from climate_download.cams import config as config_module
    monkeypatch.setattr(config_module, 'load_config', lambda p: config)
    monkeypatch.setattr(worker, 'scheduled_inits', lambda *a: ['2026102300', '2026102400'])
    seen = []
    def run(cfg, date, cycle):
        seen.append(date)
        if date == '20261023':
            raise worker.BlockedRequestError('CAMS remote_failed requires inspection')
        return {'status': 'pending', 'date': date, 'cycle': cycle}
    monkeypatch.setattr(worker, 'run_init', run)
    assert cli.cmd_cams(args()) == 1
    assert seen == ['20261023', '20261024']
    rows = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [r['status'] for r in rows] == ['blocked', 'pending']
    assert 'remote_failed' in rows[0]['error']['message']


def test_pending_init_does_not_submit_entire_backlog(config, monkeypatch):
    from climate_download.cams import config as config_module
    monkeypatch.setattr(config_module, 'load_config', lambda p: config)
    monkeypatch.setattr(worker, 'scheduled_inits', lambda *a: ['2026102300', '2026102400'])
    seen = []
    def run(cfg, date, cycle):
        seen.append(date)
        return {'status': 'pending', 'date': date, 'cycle': cycle}
    monkeypatch.setattr(worker, 'run_init', run)
    assert cli.cmd_cams(args()) == 0
    assert seen == ['20261023']


def test_explicit_failed_init_still_returns_failure(config, monkeypatch):
    from climate_download.cams import config as config_module
    monkeypatch.setattr(config_module, 'load_config', lambda p: config)
    def blocked(*a):
        raise worker.BlockedRequestError('original failure retained')
    monkeypatch.setattr(worker, 'run_init', blocked)
    assert cli.cmd_cams(args(scheduled=False)) == 1


def test_month_priority_uses_same_recovery_and_failure_rules(config, monkeypatch):
    import importlib.util
    from pathlib import Path
    path = Path(__file__).parents[1]/'docs/validation/ec-cams-backfill-month-20261022/queue_month.py'
    if not path.exists():
        pytest.skip('optional frozen month runner is not present')
    spec = importlib.util.spec_from_file_location('month_priority_recovery', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, 'pending_inits', lambda cfg, cycle, at: ['2026102400'])
    assert module.priority_inits(config, 0) == ['2026102400']
