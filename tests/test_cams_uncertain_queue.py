"""Quarantine ambiguous submissions without reposting or accepting their gaps."""
import datetime as dt
import json
from types import SimpleNamespace

import pytest

from climate_download.cams import backfill, state, worker
from climate_download.cams.catalogue import request_for
from climate_download.cams.queue_policy import make_policy
from test_cams_backfill import setup
from test_cams_staged_queue import prepare
from test_cams_staged_e2e import report, system


def enable(config, root, plan):
    config.mars_count_retry_limit = 48
    config.mars_count_retry_delays_seconds = [1800] * 48
    config.mars_count_retry_window_seconds = 86400
    config.deleted_order_retry_limit = 1
    state.save(root/'recovery_policy.json', make_policy(
        root, plan, [1800]*48, authorized_at='2026-11-02T01:07:00+00:00',
        retry_window_seconds=86400, deleted_order_retry_limit=1,
        continue_other_weeks_when_blocked=True, quarantine_uncertain_submissions=True))


def uncertain(config, key):
    folder = config.output_dir/'ec_cams'/key[:8]/(key[8:]+'z')/'.cams/gust'
    request = request_for(key[:8], int(key[8:]), 'gust')
    state.save(folder/'request.json', request)
    state.save(folder/'state.json', {
        'phase': 'submission_uncertain', 'date': key[:8], 'cycle': int(key[8:]),
        'group': 'gust', 'account': config.account_for(int(key[8:])).name,
        'dataset': 'cams-global-atmospheric-composition-forecasts',
        'request_sha256': state.signature(request),
        'submission_started_at': state.now(),
        'error': {'type': 'HTTPError', 'message': '502 Bad Gateway'}})
    return folder, {p.name: state.digest(p) for p in folder.glob('*.json')}


@pytest.mark.parametrize('phase', ['submitting', 'submission_uncertain'])
def test_same_lane_skips_only_the_uncertain_init_and_keeps_original_bytes(setup, monkeypatch, phase):
    config, plan, root = prepare(setup, monkeypatch)
    enable(config, root, plan)
    folder, _ = uncertain(config, '2026091500')
    saved = json.loads((folder/'state.json').read_text()); saved['phase'] = phase
    state.save(folder/'state.json', saved)
    original = {p.name: state.digest(p) for p in folder.glob('*.json')}
    calls = []
    def run(cfg, date, cycle, **kwargs):
        calls.append((date, cycle, kwargs))
        return {'status': 'pending', 'date': date, 'cycle': cycle}
    monkeypatch.setattr(backfill, 'run_init', run)
    result = backfill.tick(config, root, 0)
    assert calls == [('20260916', 0, {'require_idle_account': True})]
    assert result['init'] == '2026091600' and result['quarantined'][0]['init'] == '2026091500'
    assert {p.name: state.digest(p) for p in folder.glob('*.json')} == original
    assert not (root/'weeks'/plan['weeks'][0]['id']/'assessment.json').exists()


def test_finished_other_lane_can_start_previous_week_without_accepting_the_gap(setup, monkeypatch):
    config, plan, root = prepare(setup, monkeypatch); enable(config, root, plan)
    folder, original = uncertain(config, '2026091500')
    for key in plan['weeks'][0]['inits']:
        if key.endswith('12'):
            state.save(config.output_dir/'ec_cams'/key[:8]/'12z'/f'{key[:8]}_12z_ec_cams.manifest.json', {})
    calls = []
    def run(cfg, date, cycle, **kwargs):
        calls.append((date, cycle, kwargs)); return {'status': 'pending'}
    monkeypatch.setattr(backfill, 'run_init', run)
    result = backfill.tick(config, root, 12)
    assert calls == [('20260908', 12, {'require_idle_account': True})]
    assert result['week'] == plan['weeks'][1]['id']
    assert plan['weeks'][0]['id'] in result['deferred_weeks']
    assert {p.name: state.digest(p) for p in folder.glob('*.json')} == original
    assert not (root/'weeks'/plan['weeks'][0]['id']/'assessment.json').exists()


def test_policy_refusal_is_not_quarantined_as_a_submission_failure(setup, monkeypatch):
    config, plan, root = prepare(setup, monkeypatch); enable(config, root, plan)
    folder, _ = uncertain(config, '2026091500')
    value = json.loads((folder/'state.json').read_text()); value['phase'] = 'preflight_rejected'
    state.save(folder/'state.json', value)
    monkeypatch.setattr(backfill, 'run_init', lambda *a, **k: pytest.fail('policy refusal must stop'))
    assert backfill.tick(config, root, 0)['status'] == 'blocked'


@pytest.mark.parametrize('kind', ['active', 'metadata_error'])
def test_guarded_history_checks_account_under_lock_before_any_post(system, monkeypatch, kind):
    config, _, client, _ = system; client.failures = 0
    def jobs(**kwargs):
        # Taking this same account lock via another descriptor must fail here.
        import fcntl
        lock = config.output_dir/'ec_cams/.locks/account-mock-account.lock'
        with lock.open('a') as stream:
            with pytest.raises(BlockingIOError):
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if kind == 'metadata_error': raise TimeoutError('temporary metadata failure')
        return SimpleNamespace(json={'jobs': [{'jobID': 'unknown-original', 'status': 'running'}]})
    client.get_jobs = jobs
    result = worker.run_init(config, '20261027', 0, require_idle_account=True)
    assert result['status'] == 'waiting_for_account' and client.posts == []
    assert not list(config.output_dir.glob('ec_cams/20261027/00z/.cams/*/state.json'))


def test_account_guard_resumes_a_known_order_without_reposting_it(system):
    config, _, client, _ = system; client.failures = 0
    request = request_for('20261027', 0, 'hourly')
    remote = client.submit('cams-global-atmospheric-composition-forecasts', request)
    folder = config.output_dir/'ec_cams/20261027/00z/.cams/hourly'
    state.save(folder/'request.json', request)
    state.save(folder/'state.json', {'phase': 'waiting_for_remote', 'date': '20261027', 'cycle': 0,
        'group': 'hourly', 'account': 'mock-account', 'dataset': 'cams-global-atmospheric-composition-forecasts',
        'request_sha256': state.signature(request), 'request_id': remote.request_id})
    client.get_jobs = lambda **k: SimpleNamespace(json={'jobs': [{'jobID': remote.request_id, 'status': 'running'}]})
    result = worker.run_init(config, '20261027', 0, require_idle_account=True)
    assert result['status'] == 'complete'
    assert len([p for p in client.posts if p['request_sha256'] == state.signature(request)]) == 1


def test_uncertain_gap_remains_unsubmitted_while_both_lanes_and_realtime_continue(system):
    from test_cams_partition import constraint_rows
    config, clock, client, tmp = system; client.failures = 0
    root = tmp/'weekly'; plan = backfill.make_plan('20260901', '20260914', authorized_at=clock[0].isoformat())
    state.save(root/'plan.json', plan); enable(config, root, plan)
    folder, original = uncertain(config, '2026090800')
    trace = [worker.run_init(config, '20261027', 0)]
    clock[0] += dt.timedelta(hours=12)
    client.get_collection = lambda *a: SimpleNamespace(constraints=constraint_rows(0)+constraint_rows(12))
    trace.append(worker.run_init(config, '20261027', 12))
    for _ in range(7): trace.append(backfill.tick(config, root, 12))
    trace.append(backfill.tick(config, root, 0))
    trace.append(backfill.tick(config, root, 12))
    assert trace[-2]['init'] == '2026090900' and trace[-2]['status'] == 'complete'
    assert trace[-1]['init'] == '2026090112' and trace[-1]['status'] == 'complete'
    assert not any(p['request_sha256'] == json.loads((folder/'state.json').read_text())['request_sha256'] for p in client.posts)
    assert {p.name: state.digest(p) for p in folder.glob('*.json')} == original
    assert not list((root/'weeks').glob('*/assessment.json'))
    clock[0] += dt.timedelta(days=1)
    trace.append(worker.run_init(config, '20261028', 0))
    assert trace[-1]['status'] == 'complete'
    report('uncertain-quarantine', config, client, trace, {
        'uncertain_original_bytes_unchanged': True, 'uncertain_init_additional_posts': 0,
        'same_week_other_inits_continued': True, 'other_lane_previous_week_continued': True,
        'realtime_completed': True, 'gap_not_accepted': True})


def test_a_new_ambiguous_post_ends_the_tick_before_other_history_is_considered(system):
    config, clock, client, tmp = system; client.failures = 0
    assert worker.run_init(config, '20261027', 0)['status'] == 'complete'
    root = tmp/'weekly'; plan = backfill.make_plan('20260908', '20260914', authorized_at=clock[0].isoformat())
    state.save(root/'plan.json', plan); enable(config, root, plan)
    submit = client.submit
    def lost_reply(dataset, request):
        submit(dataset, request)
        raise TimeoutError('response lost after ADS accepted the original POST')
    client.submit = lost_reply
    first = backfill.tick(config, root, 0)
    assert first['status'] == 'waiting_for_account' and first['reason'] == 'new_uncertain_submission'
    count = len(client.posts); original = client.posts[-1]['request_id']
    client.get_jobs = lambda **k: SimpleNamespace(json={'jobs': [{'jobID': original, 'status': 'running'}]})
    second = backfill.tick(config, root, 0)
    assert second['init'] == '2026090900' and second['status'] == 'waiting_for_account'
    assert len(client.posts) == count


def test_new_realtime_due_during_account_metadata_preempts_guarded_history(system):
    config, clock, client, _ = system; client.failures = 0
    assert worker.run_init(config, '20261027', 0)['status'] == 'complete'
    count = len(client.posts)
    def jobs(**kwargs):
        clock[0] += dt.timedelta(days=1)
        return SimpleNamespace(json={'jobs': []})
    client.get_jobs = jobs
    history = config.model_copy(update={'first_init': '2026010100'})
    result = worker.run_init(history, '20260909', 0, require_idle_account=True)
    assert result['status'] == 'yielded_to_realtime' and result['priority_init'] == '2026102800'
    assert len(client.posts) == count


def test_realtime_does_not_gain_a_global_idle_account_gate(system):
    config, _, client, _ = system; client.failures = 0
    client.get_jobs = lambda **k: pytest.fail('normal realtime admission must not use the history guard')
    assert worker.run_init(config, '20261027', 0)['status'] == 'complete'


def test_uncertainty_isolation_cannot_be_enabled_without_cross_week_authorization(setup):
    _, plan, root = setup
    with pytest.raises(ValueError):
        make_policy(root, plan, [1800]*48, authorized_at='2026-11-02T01:07:00+00:00',
                    retry_window_seconds=86400, quarantine_uncertain_submissions=True)
