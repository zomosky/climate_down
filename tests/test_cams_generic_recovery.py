"""Confirmed generic MARS failures share the existing half-hour/day recovery bound."""
import datetime as dt
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from climate_download.cams import backfill, recovery, state as store, worker
from climate_download.cams.config import CamsConfig
from climate_download.cams.queue_policy import make_policy
from test_cams_daily_recovery import daily
from test_cams_mars_recovery import setup, state
from test_cams_staged_e2e import system, report


ERROR = ('400 Client Error: Bad Request for url: https://signed.invalid/?token=token-value\n'
         'The job has failed\nThe job failed with: MarsRuntimeError\n'
         'MARS has returned an error, please check your selection.\n'
         'Request submitted to the MARS server:\n' + '.'*2000 + '\nFull error message:\n\n')


def enable(config):
    daily(config)
    config.generic_mars_retry_enabled = True


def ready(setup, *, enabled=True):
    config, folder, client, clock, tick = setup
    daily(config)
    saved = state(folder); saved['mars_retry_policy'] = recovery.policy_for(config)
    store.save(folder/'state.json', saved)
    if enabled: config.generic_mars_retry_enabled = True
    client.remotes['job-1'].json['status'] = 'failed'
    client.remotes['job-1'].error = RuntimeError(ERROR)
    original = client.get_jobs
    client.get_jobs = lambda **kwargs: (original(**kwargs) if 'status' in kwargs else
                                      SimpleNamespace(json={'jobs': []}, next=None))
    return config, folder, client, clock, tick


def original_files(folder):
    return {str(p): store.digest(p) for p in folder.glob('failures/*/*.json')}


def unchanged(proof):
    assert all(store.digest(Path(p)) == digest for p, digest in proof.items())


def test_generic_failure_is_opt_in_and_keeps_the_original_without_post(setup):
    _, folder, client, _, tick = ready(setup, enabled=False)
    with pytest.raises(worker.BlockedRequestError): tick()
    assert state(folder)['phase'] == 'remote_failed' and len(client.posts) == 1
    assert json.loads(next(folder.glob('failures/*/failure.json')).read_text())['diagnostic'] is None


def test_new_generic_failure_uses_half_hour_delay_and_identical_request(setup):
    _, folder, client, clock, tick = ready(setup)
    assert tick() is False
    assert state(folder)['phase'] == 'retry_wait'
    assert state(folder)['failure_diagnostic']['kind'] == 'mars_runtime_error'
    proof = original_files(folder); request = (folder/'request.json').read_bytes()
    clock[0] += dt.timedelta(seconds=1799)
    assert tick() is False and len(client.posts) == 1
    clock[0] += dt.timedelta(seconds=1)
    assert tick() is False and len(client.posts) == 2
    assert client.posts[0] == client.posts[1] and (folder/'request.json').read_bytes() == request
    assert state(folder)['retry_count'] == 1
    unchanged(proof)


def test_retained_generic_failure_enrolls_from_full_archive_without_resetting_clock(setup):
    config, folder, client, clock, tick = ready(setup, enabled=False)
    with pytest.raises(worker.BlockedRequestError): tick()
    proof = original_files(folder); observed = clock[0]
    assert 'Full error message' not in state(folder)['error']['message']
    clock[0] += dt.timedelta(hours=23); config.generic_mars_retry_enabled = True
    client.active = [{'jobID': 'different-history', 'status': 'running'}]
    assert tick() is False and len(client.posts) == 1
    current = state(folder); assert current['phase'] == 'retry_wait'
    assert recovery._first_failure_at(config, folder, current) == observed
    enrollment = current['failed_attempt']['generic_mars_enrollment']
    receipt = json.loads((folder/enrollment['directory']).read_text())
    assert receipt['observed_at'] == observed.isoformat()
    assert receipt['request_id'] == 'job-1' and receipt['remote_failed_verified']
    client.active = []
    assert tick() is False and len(client.posts) == 2
    assert state(folder)['previous_attempts'][0]['evidence']['generic_mars_enrollment'] == enrollment
    unchanged(proof)


def test_expired_retained_generic_failure_does_not_gain_a_new_day(setup):
    config, folder, client, clock, tick = ready(setup, enabled=False)
    with pytest.raises(worker.BlockedRequestError): tick()
    proof = original_files(folder); clock[0] += dt.timedelta(days=1, seconds=1)
    config.generic_mars_retry_enabled = True
    with pytest.raises(worker.BlockedRequestError): tick()
    assert len(client.posts) == 1
    unchanged(proof)


@pytest.mark.parametrize('problem', ['request', 'raw', 'state_archive', 'failure_archive'])
def test_old_generic_enrollment_keeps_existing_evidence_guards(setup, problem):
    config, folder, client, _, tick = ready(setup, enabled=False)
    with pytest.raises(worker.BlockedRequestError): tick()
    archive = folder/state(folder)['failed_attempt']['directory']
    if problem == 'raw': (folder/'download.grib.part').write_bytes(b'original partial')
    elif problem == 'request': (folder/'request.json').write_text('{}')
    else: (archive/('state.json' if problem == 'state_archive' else 'failure.json')).write_text('{}')
    config.generic_mars_retry_enabled = True
    with pytest.raises((worker.BlockedRequestError, ValueError)): tick()
    assert len(client.posts) == 1


@pytest.mark.parametrize('detail', [
    'MarsRuntimeError only',
    'MARS has returned an error\nFull error message:\n',
    'MarsRuntimeError\nMARS has returned an error\nFull error message:\nInvalid parameter',
    '403 Client Error\nMarsRuntimeError\nMARS has returned an error\nFull error message:\n',
    'MarsRuntimeError licence not accepted\nMARS has returned an error\nFull error message:\n',
])
def test_unverified_or_permanent_errors_are_not_generic_retry_candidates(setup, detail):
    _, folder, client, _, tick = ready(setup)
    client.remotes['job-1'].error = RuntimeError(detail)
    with pytest.raises(worker.BlockedRequestError): tick()
    assert len(client.posts) == 1 and state(folder)['phase'] == 'remote_failed'


def test_original_reactivated_job_is_polled_without_an_extra_submission(setup):
    config, folder, client, _, tick = ready(setup, enabled=False)
    with pytest.raises(worker.BlockedRequestError): tick()
    proof = original_files(folder); config.generic_mars_retry_enabled = True
    client.remotes['job-1'].json['status'] = 'running'
    assert tick() is False and state(folder)['request_id'] == 'job-1'
    assert state(folder)['phase'] == 'waiting_for_remote' and len(client.posts) == 1
    unchanged(proof)


def test_matching_successful_order_prevents_a_duplicate_post(setup):
    _, folder, client, clock, tick = ready(setup)
    assert tick() is False; clock[0] += dt.timedelta(minutes=30)
    original = client.remotes['job-1']; original.json['created'] = clock[0].isoformat()
    match = SimpleNamespace(json={**original.json, 'status': 'successful'})
    get = client.get_remote
    client.get_remote = lambda rid: match if rid == 'existing-success' else get(rid)
    client.get_jobs = lambda **kwargs: SimpleNamespace(json={'jobs': [] if 'status' in kwargs else
        [{'jobID': 'existing-success', 'status': 'successful', 'created': clock[0].isoformat()}]}, next=None)
    with pytest.raises(worker.BlockedRequestError): tick()
    assert len(client.posts) == 1 and state(folder).get('retry_count', 0) == 0


def test_unknown_submission_spends_one_attempt_and_never_posts_again(setup):
    _, folder, client, clock, tick = ready(setup)
    assert tick() is False; clock[0] += dt.timedelta(minutes=30)
    client.on_submit = lambda: (_ for _ in ()).throw(TimeoutError('lost response'))
    with pytest.raises(worker.BlockedRequestError): tick()
    assert state(folder)['phase'] == 'submission_uncertain' and state(folder)['retry_count'] == 1
    for _ in range(2):
        with pytest.raises(worker.BlockedRequestError): tick()
    assert len(client.posts) == 2


def test_generic_recovery_stops_after_the_shared_day(setup):
    config, folder, client, clock, tick = ready(setup)
    assert tick() is False; observed = clock[0]
    clock[0] += dt.timedelta(minutes=30); tick()
    client.remotes['job-2'].json['status'] = 'failed'; client.remotes['job-2'].error = RuntimeError(ERROR)
    clock[0] = observed + dt.timedelta(hours=23)
    assert tick() is False and recovery._first_failure_at(config, folder, state(folder)) == observed
    clock[0] = observed + dt.timedelta(days=1, seconds=1)
    with pytest.raises(worker.BlockedRequestError): tick()
    assert len(client.posts) == 2


def test_new_realtime_due_after_preflight_prevents_old_replacement(setup, monkeypatch):
    _, folder, client, clock, tick = ready(setup)
    assert tick() is False; clock[0] += dt.timedelta(minutes=30)
    monkeypatch.setattr(worker, 'newer_live_due', lambda *a: '2026102800')
    assert tick() is False and len(client.posts) == 1
    assert state(folder)['phase'] == 'retry_wait' and state(folder).get('retry_count', 0) == 0


def test_generic_config_requires_the_bounded_daily_policy(setup):
    config, *_ = setup
    values = config.model_dump(); values['generic_mars_retry_enabled'] = True
    with pytest.raises(ValueError): CamsConfig.model_validate(values)


def test_generic_failure_does_not_reuse_an_old_non_half_hourly_policy(setup):
    _, folder, client, clock, tick = ready(setup)
    saved = state(folder)
    saved['mars_retry_policy'] = {'schema_version': 2, 'max_retries': 3, 'per_init_limit': 3,
        'delays_seconds': [1800, 7200, 21600], 'captured_at': clock[0].isoformat()}
    store.save(folder/'state.json', saved)
    with pytest.raises(worker.BlockedRequestError): tick()
    assert len(client.posts) == 1 and state(folder)['phase'] == 'remote_failed'


def test_preflight_cannot_cross_the_original_day_and_still_post(setup, monkeypatch):
    _, folder, client, clock, tick = ready(setup)
    assert tick() is False; clock[0] += dt.timedelta(days=1, seconds=-1)
    def slow(*args):
        clock[0] += dt.timedelta(seconds=2)
        return {'policy_gate': 'passed'}
    monkeypatch.setattr(worker, 'preflight', slow)
    monkeypatch.setattr(worker, 'newer_live_due', lambda *a: None)
    with pytest.raises(worker.BlockedRequestError): tick()
    assert len(client.posts) == 1 and state(folder).get('retry_count', 0) == 0


def test_48_generic_replacements_share_one_durable_cap(setup, monkeypatch):
    _, folder, client, clock, tick = ready(setup)
    monkeypatch.setattr(worker, 'newer_live_due', lambda *a: None)
    assert tick() is False; proof = original_files(folder)
    for attempt in range(1, 49):
        clock[0] += dt.timedelta(minutes=30)
        assert tick() is False and len(client.posts) == attempt+1
        client.remotes[f'job-{attempt+1}'].json['status'] = 'failed'
        client.remotes[f'job-{attempt+1}'].error = RuntimeError(ERROR)
        if attempt < 48: assert tick() is False
        else:
            with pytest.raises(worker.BlockedRequestError): tick()
    assert state(folder)['retry_count'] == 48 and len(client.posts) == 49
    clock[0] += dt.timedelta(days=2)
    with pytest.raises(worker.BlockedRequestError): tick()
    assert len(client.posts) == 49 and all(p == client.posts[0] for p in client.posts)
    unchanged(proof)


def test_a_failure_in_another_group_keeps_the_shared_earliest_window(setup):
    config, folder, client, clock, tick = ready(setup)
    assert tick() is False
    other = config.output_dir/'ec_cams/20261027/00z/.cams/gust/failures/other/failure.json'
    store.save(other, {'captured_at': (clock[0]-dt.timedelta(days=1, seconds=1)).isoformat(),
        'diagnostic': {'kind': 'mars_runtime_error'}, 'local_request_verified': True,
        'remote_request_verified': True})
    clock[0] += dt.timedelta(minutes=30)
    with pytest.raises(worker.BlockedRequestError): tick()
    assert len(client.posts) == 1


def test_generic_enrollment_cannot_move_the_original_failure_time(setup):
    config, folder, client, clock, tick = ready(setup, enabled=False)
    with pytest.raises(worker.BlockedRequestError): tick()
    config.generic_mars_retry_enabled = True; client.active = [{'jobID': 'busy'}]
    assert tick() is False
    proof = state(folder)['failed_attempt']['generic_mars_enrollment']; path = folder/proof['directory']
    receipt = json.loads(path.read_text()); receipt['observed_at'] = (clock[0]+dt.timedelta(hours=1)).isoformat()
    store.save(path, receipt); client.active = []; clock[0] += dt.timedelta(minutes=30)
    with pytest.raises(worker.BlockedRequestError): tick()
    assert len(client.posts) == 1


def test_retained_generic_enrollment_defers_on_metadata_authentication_failure(setup):
    import requests
    config, folder, client, _, tick = ready(setup, enabled=False)
    with pytest.raises(worker.BlockedRequestError): tick()
    proof = original_files(folder); config.generic_mars_retry_enabled = True
    response = requests.Response(); response.status_code = 403
    def denied(rid): raise requests.HTTPError('403 forbidden', response=response)
    client.get_remote = denied
    with pytest.raises(worker.BlockedRequestError): tick()
    assert len(client.posts) == 1 and state(folder)['phase'] == 'remote_failed'
    unchanged(proof)


def test_generic_recovery_full_target_publishes_without_blocking_history_or_realtime(system):
    config, clock, client, tmp = system; client.failures = 0; enable(config)
    assert worker.run_init(config, '20261027', 0)['status'] == 'complete'
    submit = client.submit; target_posts = []
    def fail_target(dataset, request):
        remote = submit(dataset, request)
        if request['date'] == '2026-08-30' and request.get('pressure_level'):
            target_posts.append(remote.request_id)
            if len(target_posts) <= 2:
                remote.json['status'] = 'failed'
                remote.get_results = lambda: (_ for _ in ()).throw(RuntimeError(ERROR))
        return remote
    client.submit = fail_target
    historical = config.model_copy(update={'first_init': '2026010100', 'max_run_seconds': 0,
                                          'hourly_request_mode': 'bundle'})
    first = worker.run_init(historical, '20260830', 0)
    assert first['status'] == 'recovery_deferred'
    root = tmp/'weekly'; plan = backfill.make_plan('20260825', '20260831', authorized_at=clock[0].isoformat())
    store.save(root/'plan.json', plan)
    store.save(root/'recovery_policy.json', make_policy(root, plan, [1800]*48,
        authorized_at=clock[0].isoformat(), retry_window_seconds=86400, deleted_order_retry_limit=0,
        continue_other_weeks_when_blocked=True, generic_mars_retry_enabled=True))
    history = backfill.tick(config, root, 0)
    assert history['status'] == 'complete' and history['init'] == '2026082500'
    proof = original_files(config.output_dir/'ec_cams/20260830/00z/.cams/pressure850')
    trace = [first, history]
    for _ in range(2):
        clock[0] += dt.timedelta(minutes=30)
        historical = CamsConfig.model_validate(historical.model_dump())
        trace.append(worker.run_init(historical, '20260830', 0))
    assert trace[-1]['status'] == 'complete' and len(target_posts) == 3
    manifest = json.loads(Path(trace[-1]['manifest']).read_text())
    assert len(manifest['variables']) == 52 and len(manifest['files']) == 121
    assert sum(f['records_selected'] for f in manifest['files']) == 5732
    unchanged(proof)
    clock[0] += dt.timedelta(days=1)
    live = worker.run_init(config, '20261028', 0); assert live['status'] == 'complete'
    report('generic-mars-success', config, client, trace+[live], {
        'original_failures_unchanged': True, 'target_pressure_messages': 246,
        'full_init_messages': 5732, 'variables': 52, 'extra_target_submissions': 2,
        'history_completed_while_waiting': True, 'new_realtime_completed': True})
