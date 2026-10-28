"""A weekly queue must stay finite, preserve failed jobs and yield to live work."""
import pytest

from climate_download.cams import backfill
from climate_download.cams.config import CamsConfig
from climate_download.cams.state import digest, save, signature
from climate_download.cams.worker import BlockedRequestError


@pytest.fixture
def setup(tmp_path):
    config = CamsConfig(output_dir=tmp_path/'data', first_init='2026102100',
                        accounts=[{'name': 'a', 'credential_file': tmp_path/'credential'}],
                        cycle_accounts={0: 'a', 12: 'a'})
    plan = backfill.make_plan('20260101', '20260921', authorized_at='2026-10-27T01:00:00Z')
    root = tmp_path/'queue'
    root.mkdir()
    save(root/'plan.json', plan)
    return config, plan, root


def test_weeks_cover_exact_authorized_range_backwards_without_gaps(setup):
    _, plan, _ = setup
    assert len(plan['weeks']) == 38 and plan['inits_total'] == 528
    assert (plan['weeks'][0]['start'], plan['weeks'][0]['end']) == ('20260915', '20260921')
    assert (plan['weeks'][-1]['start'], plan['weeks'][-1]['end']) == ('20260101', '20260105')
    keys = [k for w in plan['weeks'] for k in w['inits']]
    assert len(keys) == len(set(keys)) == 528
    assert min(keys) == '2026010100' and max(keys) == '2026092112'
    assert all(len(w['inits']) == 14 for w in plan['weeks'][:-1])


@pytest.mark.parametrize('mutation', [
    lambda p: p['weeks'][0]['inits'].pop(),
    lambda p: p.update(variables=51),
    lambda p: p.update(start='20250101'),
    lambda p: p.update(authorized_at=''),
])
def test_plan_drift_is_rejected(setup, mutation):
    _, plan, root = setup
    mutation(plan)
    save(root/'plan.json', plan)
    with pytest.raises(ValueError):
        backfill.load_plan(root/'plan.json')


def test_raw_or_zarr_alone_never_advances_the_week(setup):
    _, plan, root = setup
    assert backfill.active_week(root, plan) == plan['weeks'][0]
    save(root/'weeks'/plan['weeks'][0]['id']/'assessment.json', {'passed': True})
    with pytest.raises(ValueError, match='assessment'):
        backfill.active_week(root, plan)


def test_live_priority_prevents_history_submission(setup, monkeypatch):
    config, plan, root = setup
    monkeypatch.setattr(backfill, 'pending_inits', lambda *a: ['2026102612'])
    monkeypatch.setattr(backfill, 'run_init', lambda *a, **k: pytest.fail('live work has priority'))
    result = backfill.tick(config, root, 12)
    assert result['status'] == 'waiting_for_realtime'
    assert result['priority_inits'] == ['2026102612']


def test_tick_drives_one_init_with_shared_locks_and_original_account(setup, monkeypatch):
    config, plan, root = setup
    monkeypatch.setattr(backfill, 'pending_inits', lambda *a: [])
    seen = []
    def run(cfg, date, cycle):
        seen.append((cfg, date, cycle))
        return {'status': 'pending', 'date': date, 'cycle': cycle}
    monkeypatch.setattr(backfill, 'run_init', run)
    monkeypatch.setattr(backfill.shutil, 'disk_usage', lambda p: (0, 0, 10**13))
    result = backfill.tick(config, root, 0)
    assert result['status'] == 'pending' and len(seen) == 1
    cfg, date, cycle = seen[0]
    assert date == '20260915' and cycle == 0
    assert cfg.output_dir == config.output_dir and cfg.accounts == config.accounts
    assert cfg.max_run_seconds == 0 and cfg.first_init == '2026010100'
    assert config.first_init == '2026102100'


def test_transport_timeout_keeps_week_pending_and_reuses_its_order(setup, monkeypatch):
    import json
    import requests
    from climate_download.cams import worker
    from test_cams import fake_client
    config,_,root=setup
    monkeypatch.setattr(backfill,'pending_inits',lambda *a:[])
    monkeypatch.setattr(backfill.shutil,'disk_usage',lambda p:(0,0,10**13))
    client=fake_client();get=client.get_remote
    def successful(rid):
        remote=get(rid);remote.json['status']='successful';return remote
    client.get_remote=successful
    monkeypatch.setattr(worker,'create_client',lambda account:(client,''))
    monkeypatch.setattr(worker,'preflight',lambda *a:{'passed':True})
    def timeout(*a,**k):raise requests.ReadTimeout('temporary object-store timeout')
    monkeypatch.setattr(worker,'transfer',timeout)
    for _ in range(2):
        result=backfill.tick(config,root,0)
        assert result['status']=='pending' and result['init']=='2026091500'
    state=json.loads((config.output_dir/'ec_cams/20260915/00z/.cams/hourly/state.json').read_text())
    assert client.submits==1 and state['request_id']=='original-job'
    assert state['phase']=='transfer_deferred' and state['transfer_deferred_count']==2


def test_terminal_history_failure_is_retained_and_other_inits_can_run(setup, monkeypatch):
    config, plan, root = setup
    monkeypatch.setattr(backfill, 'pending_inits', lambda *a: [])
    monkeypatch.setattr(backfill.shutil, 'disk_usage', lambda p: (0, 0, 10**13))
    path = config.output_dir/'ec_cams/20260915/00z/.cams/hourly/state.json'
    save(path, {'phase': 'remote_failed', 'request_id': 'original'})
    before = path.read_bytes()
    seen = []
    def run(cfg, date, cycle):
        seen.append(date)
        return {'status': 'pending', 'date': date, 'cycle': cycle}
    monkeypatch.setattr(backfill, 'run_init', run)
    result = backfill.tick(config, root, 0)
    assert seen == ['20260916'] and result['blocked'][0]['init'] == '2026091500'
    assert path.read_bytes() == before


def test_new_blocked_request_cannot_be_automatically_resubmitted(setup, monkeypatch):
    config, _, root = setup
    monkeypatch.setattr(backfill, 'pending_inits', lambda *a: [])
    monkeypatch.setattr(backfill.shutil, 'disk_usage', lambda p: (0, 0, 10**13))
    def run(*a, **k):
        raise BlockedRequestError('submit uncertain; inspect original ID')
    monkeypatch.setattr(backfill, 'run_init', run)
    result = backfill.tick(config, root, 0)
    assert result['status'] == 'blocked' and result['init'] == '2026091500'


def test_low_disk_space_stops_new_work(setup, monkeypatch):
    config, _, root = setup
    monkeypatch.setattr(backfill, 'pending_inits', lambda *a: [])
    monkeypatch.setattr(backfill.shutil, 'disk_usage', lambda p: (0, 0, 1024))
    monkeypatch.setattr(backfill, 'run_init', lambda *a, **k: pytest.fail('no space'))
    assert backfill.tick(config, root, 0)['status'] == 'waiting_for_disk_space'


def test_only_full_accepted_week_opens_previous_week_and_evidence_is_pinned(setup):
    _, plan, root = setup
    week = plan['weeks'][0]
    cases = []
    for key in week['inits']:
        folder = root/'weeks'/week['id']/'acceptance'/key
        case = {'passed': True, 'init': key}
        for stage in ('raw', 'http'):
            path = folder/(stage+'_acceptance.json')
            save(path, {'passed': True})
            case[stage+'_sha256'] = digest(path)
        cases.append(case)
    save(root/'weeks'/week['id']/'assessment.json',
         {'passed': True, 'week': week, 'plan_signature': signature(plan), 'cases': cases})
    assert backfill.active_week(root, plan) == plan['weeks'][1]
    (root/'weeks'/week['id']/'acceptance'/week['inits'][0]/'http_acceptance.json').write_text('corrupted')
    with pytest.raises(ValueError, match='evidence'):
        backfill.active_week(root, plan)
