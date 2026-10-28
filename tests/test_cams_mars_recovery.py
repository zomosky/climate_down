"""Recover only confirmed MARS count failures, with durable one-attempt bounds."""
import copy
import datetime as dt
import json
from types import SimpleNamespace

import pytest

from climate_download.cams import recovery, worker
from climate_download.cams.catalogue import request_for
from climate_download.cams.client import PreflightRejected
from climate_download.cams.config import CamsConfig
from climate_download.cams.state import digest, save, safe_error


ERROR = ('ADS request expansion '+'.'*2000+' token-value https://signed.invalid/?token=token-value\n'
         'The job failed with: MarsRuntimeError\n'
         'mars - ERROR - Last error is -43\nmars - ERROR - Expected 420, got 494.\n')


@pytest.fixture
def setup(tmp_path, monkeypatch):
    config = CamsConfig(output_dir=tmp_path, first_init='2026102100', max_run_seconds=0,
                        accounts=[{'name': 'a', 'credential_file': tmp_path/'credential'}],
                        cycle_accounts={0: 'a', 12: 'a'}, mars_count_retry_limit=1,
                        mars_count_retry_delay_seconds=1800)
    clock = [dt.datetime(2026, 10, 27, 10, tzinfo=dt.timezone.utc)]
    stamp = lambda: clock[0].isoformat()
    monkeypatch.setattr(worker, 'now', stamp)
    monkeypatch.setattr(recovery, 'now', stamp)
    monkeypatch.setattr(worker, 'preflight', lambda *a: {'cost': 5445, 'policy_gate': 'passed'})
    request = request_for('20261027', 0, 'hourly')
    class Remote:
        def __init__(self, job):
            self.request_id = job
            self.json = {'status': 'accepted', 'metadata': {'request': {'ids': copy.deepcopy(request)}}}
            self.error = RuntimeError(ERROR)
        def get_results(self):
            raise self.error
    class Client:
        def __init__(self):
            self.posts = []
            self.remotes = {}
            self.active = []
            self.on_submit = None
        def submit(self, dataset, payload):
            self.posts.append(copy.deepcopy(payload))
            if self.on_submit:
                self.on_submit()
            remote = Remote('job-'+str(len(self.posts)))
            self.remotes[remote.request_id] = remote
            return remote
        def get_remote(self, job):
            return self.remotes[job]
        def get_jobs(self, **kwargs):
            assert kwargs['status'] == ['accepted', 'running']
            return SimpleNamespace(json={'jobs': self.active})
    client = Client()
    folder = tmp_path/'group'
    def tick():
        return worker.group_tick(config, folder, '20261027', 0, 'hourly', client, 'token-value')
    assert tick() is False
    return config, folder, client, clock, tick


def failed(setup):
    config, folder, client, clock, tick = setup
    client.remotes['job-1'].json['status'] = 'failed'
    return tick


def state(folder):
    return json.loads((folder/'state.json').read_text())


def test_long_failure_is_classified_before_log_truncation_and_archived(setup):
    _, folder, client, _, _ = setup
    assert failed(setup)() is False
    current = state(folder)
    assert current['phase'] == 'retry_wait' and current['request_id'] == 'job-1'
    assert current['failure_diagnostic'] == {'kind': 'mars_field_count_mismatch', 'mars_error_code': -43,
                                            'expected_fields': 420, 'actual_fields': 494}
    archive = folder/current['failed_attempt']['directory']
    assert digest(archive/'state.json') == current['failed_attempt']['state_sha256']
    assert json.loads((archive/'state.json').read_text())['phase'] == 'remote_failed'
    full = (archive/'failure.json').read_text()
    assert 'Expected 420, got 494' in full and 'token-value' not in full and 'https://' not in full
    assert len(client.posts) == 1


def test_cooldown_then_one_identical_retry_preserves_original_request(setup):
    _, folder, client, clock, tick = setup
    request_bytes = (folder/'request.json').read_bytes()
    failed(setup)()
    clock[0] += dt.timedelta(seconds=1799)
    assert tick() is False and len(client.posts) == 1
    clock[0] += dt.timedelta(seconds=1)
    assert tick() is False and len(client.posts) == 2
    current = state(folder)
    assert current['request_id'] == 'job-2' and current['retry_count'] == 1
    assert current['previous_attempts'][-1]['request_id'] == 'job-1'
    assert client.posts[0] == client.posts[1] == request_for('20261027', 0, 'hourly')
    assert (folder/'request.json').read_bytes() == request_bytes
    assert tick() is False and len(client.posts) == 2


def test_second_failure_never_retries_again(setup):
    _, folder, client, clock, tick = setup
    failed(setup)()
    clock[0] += dt.timedelta(seconds=1800)
    tick()
    client.remotes['job-2'].json['status'] = 'failed'
    with pytest.raises(worker.BlockedRequestError):
        tick()
    assert state(folder)['phase'] == 'remote_failed' and len(client.posts) == 2
    with pytest.raises(worker.BlockedRequestError):
        tick()
    assert len(list((folder/'failures').glob('*/failure.json'))) == 2


def test_lost_retry_response_stays_uncertain_across_restart(setup):
    _, folder, client, clock, tick = setup
    failed(setup)()
    clock[0] += dt.timedelta(seconds=1800)
    def timeout():
        raise TimeoutError('token-value https://signed.invalid/?token=token-value')
    client.on_submit = timeout
    with pytest.raises(worker.BlockedRequestError, match='uncertain'):
        tick()
    current = state(folder)
    assert current['phase'] == 'submission_uncertain' and current['retry_count'] == 1
    assert 'token-value' not in (folder/'state.json').read_text()
    with pytest.raises(worker.BlockedRequestError):
        tick()
    assert len(client.posts) == 2


def test_existing_request_without_captured_policy_is_not_retroactively_retried(setup):
    _, folder, client, _, _ = setup
    current = state(folder)
    current.pop('mars_retry_policy')
    save(folder/'state.json', current)
    with pytest.raises(worker.BlockedRequestError):
        failed(setup)()
    assert state(folder)['phase'] == 'remote_failed' and len(client.posts) == 1


@pytest.mark.parametrize('message', [
    '403 licence not accepted', 'Expected 420, got 494.',
    'MarsRuntimeError Last error is -42 Expected 420, got 494.',
    'MarsRuntimeError Last error is -43 Expected 420, got 420.',
    'MarsRuntimeError Last error is -43 data not found',
])
def test_other_failures_are_not_given_automatic_replacements(setup, message):
    _, folder, client, _, _ = setup
    client.remotes['job-1'].error = RuntimeError(message)
    with pytest.raises(worker.BlockedRequestError):
        failed(setup)()
    assert state(folder)['phase'] == 'remote_failed' and len(client.posts) == 1


@pytest.mark.parametrize('filename', ['download.grib', 'download.grib.part'])
def test_existing_raw_bytes_prevent_switching_to_a_different_order(setup, filename):
    _, folder, client, _, _ = setup
    path = folder/filename
    path.write_bytes(b'original bytes')
    with pytest.raises(worker.BlockedRequestError):
        failed(setup)()
    assert path.read_bytes() == b'original bytes' and len(client.posts) == 1


def test_cached_ranges_prevent_replacement_even_before_the_first_contiguous_append(setup):
    _,folder,client,_,_=setup
    cache=folder/'download.grib.part.ranges/8-15.part';cache.parent.mkdir();cache.write_bytes(b'original')
    with pytest.raises(worker.BlockedRequestError):failed(setup)()
    assert cache.read_bytes()==b'original' and len(client.posts)==1


def test_range_identity_failure_is_terminal_and_keeps_original_bytes(setup,monkeypatch):
    from climate_download.cams.ranges import RangeIdentityError
    _,folder,client,_,tick=setup;client.remotes['job-1'].json['status']='successful'
    raw=folder/'download.grib.part';raw.write_bytes(b'original bytes')
    def changed(*a,**kw):raise RangeIdentityError('Object identity changed')
    monkeypatch.setattr(worker,'transfer',changed)
    with pytest.raises(worker.BlockedRequestError,match='range'):tick()
    with pytest.raises(worker.BlockedRequestError):tick()
    assert state(folder)['phase']=='validation_failed'
    assert raw.read_bytes()==b'original bytes' and len(client.posts)==1


def test_remote_payload_drift_prevents_replacement(setup):
    _, folder, client, _, _ = setup
    client.remotes['job-1'].json['metadata']['request']['ids']['time'] = ['12:00']
    with pytest.raises(worker.BlockedRequestError):
        failed(setup)()
    assert state(folder)['phase'] == 'remote_failed' and len(client.posts) == 1


def test_other_active_order_defers_retry_without_spending_the_attempt(setup):
    _, folder, client, clock, tick = setup
    failed(setup)()
    clock[0] += dt.timedelta(seconds=1800)
    client.active = [{'jobID': 'manual-order', 'status': 'running'}]
    assert tick() is False and len(client.posts) == 1
    assert state(folder).get('retry_count', 0) == 0
    client.active = []
    assert tick() is False and len(client.posts) == 2


def test_retry_rechecks_preflight_and_does_not_bypass_licence_or_cost(setup, monkeypatch):
    _, folder, client, clock, tick = setup
    failed(setup)()
    clock[0] += dt.timedelta(seconds=1800)
    def rejected(*a):
        raise PreflightRejected('cost differs')
    monkeypatch.setattr(worker, 'preflight', rejected)
    with pytest.raises(worker.BlockedRequestError):
        tick()
    assert state(folder)['phase'] == 'preflight_rejected' and len(client.posts) == 1


def test_crash_during_retry_cannot_create_another_order(setup):
    _, folder, client, clock, tick = setup
    failed(setup)()
    clock[0] += dt.timedelta(seconds=1800)
    def crash():
        current = state(folder)
        assert current['phase'] == 'submitting' and current['retry_count'] == 1
        assert not current.get('request_id')
        raise SystemExit('process crashed')
    client.on_submit = crash
    with pytest.raises(SystemExit):
        tick()
    with pytest.raises(worker.BlockedRequestError):
        tick()
    assert len(client.posts) == 2


def test_tampering_with_failed_evidence_blocks_retry(setup):
    _, folder, client, clock, tick = setup
    failed(setup)()
    archive = folder/state(folder)['failed_attempt']['directory']
    (archive/'state.json').write_text('changed evidence')
    clock[0] += dt.timedelta(seconds=1800)
    with pytest.raises(worker.BlockedRequestError):
        tick()
    assert len(client.posts) == 1


def test_full_error_redaction_keeps_the_end_without_weakening_default_limit():
    error = RuntimeError(ERROR)
    assert len(safe_error(error, 'token-value')['message']) == 1200
    full = safe_error(error, 'token-value', max_chars=None)['message']
    assert 'Expected 420, got 494' in full and 'token-value' not in full
    assert recovery.classify_mars_failure(error)['actual_fields'] == 494


def test_transient_metadata_failure_does_not_spend_retry_budget(setup, monkeypatch):
    _, folder, client, clock, tick = setup
    failed(setup)()
    clock[0] += dt.timedelta(seconds=1800)
    original = client.get_remote
    def unavailable(*a):
        raise TimeoutError('metadata token-value https://signed.invalid/?token=token-value')
    monkeypatch.setattr(client, 'get_remote', unavailable)
    assert tick() is False and len(client.posts) == 1
    assert state(folder)['phase'] == 'retry_wait' and state(folder).get('retry_count', 0) == 0
    assert 'token-value' not in (folder/'state.json').read_text()
    monkeypatch.setattr(client, 'get_remote', original)
    assert tick() is False and len(client.posts) == 2


def test_disabling_policy_makes_pending_retry_terminal_without_new_post(setup):
    config, folder, client, clock, tick = setup
    failed(setup)()
    config.mars_count_retry_limit = 0
    clock[0] += dt.timedelta(seconds=1800)
    with pytest.raises(worker.BlockedRequestError):
        tick()
    assert state(folder)['phase'] == 'remote_failed' and len(client.posts) == 1


def test_later_config_change_cannot_shorten_captured_cooldown(setup):
    config, folder, client, clock, tick = setup
    failed(setup)()
    config.mars_count_retry_delay_seconds = 300
    clock[0] += dt.timedelta(seconds=900)
    assert tick() is False and len(client.posts) == 1
    clock[0] += dt.timedelta(seconds=900)
    assert tick() is False and len(client.posts) == 2


def test_original_order_becoming_successful_keeps_its_original_id(setup, monkeypatch):
    _, folder, client, clock, tick = setup
    failed(setup)()
    clock[0] += dt.timedelta(seconds=1800)
    client.remotes['job-1'].json['status'] = 'successful'
    monkeypatch.setattr(worker, 'transfer', lambda *a, **k: ({'expected_bytes': 1, 'partial_bytes': 0}, False))
    assert tick() is False
    assert state(folder)['request_id'] == 'job-1' and len(client.posts) == 1


def test_uncertain_retry_stays_visible_after_recent_window_moves(setup):
    config, _, _, _, _ = setup
    folder = config.output_dir/'ec_cams/20261022/00z/.cams/hourly'
    save(folder/'state.json', {'date': '20261022', 'cycle': 0, 'phase': 'submission_uncertain'})
    keys = worker.scheduled_inits(config, 0, dt.datetime(2026, 10, 28, 1, tzinfo=dt.timezone.utc))
    assert keys == ['2026102200', '2026102600', '2026102700']
    assert worker.pending_inits(config, 0, dt.datetime(2026, 10, 28, 1, tzinfo=dt.timezone.utc)) == keys


def test_older_loaded_config_cannot_enroll_requests_during_code_update():
    assert recovery.policy_for(SimpleNamespace())['max_retries'] == 0
