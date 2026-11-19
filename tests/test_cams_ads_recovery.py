"""The two production ADS failure forms enter bounded recovery."""
from datetime import timedelta
import json
from types import SimpleNamespace

import requests

import pytest

from climate_download.cams import backfill, recovery, retry_activation, submission, worker
from climate_download.cams.queue_policy import make_policy
from climate_download.cams.state import digest, save
from test_cams_generic_recovery import ready, original_files, unchanged
from test_cams_mars_recovery import setup, state
from test_cams_submission_recovery import case, config, candidate
from test_cams_staged_e2e import system, report


ADS_FAILURE = '400 Client Error: Bad Request for url: [URL_REDACTED]\nThe job has failed\n'
GATEWAY = {'type': 'HTTPError', 'message': '502 Server Error: Bad Gateway for url: [URL_REDACTED]'}


def test_confirmed_ads_failure_without_mars_detail_retries_the_same_request(setup):
    cfg, folder, client, clock, tick = ready(setup)
    cfg.ads_retry_enabled = True
    client.remotes['job-1'].error = RuntimeError(ADS_FAILURE)
    assert tick() is False
    initial = state(folder)
    assert initial['phase'] == 'retry_wait'
    assert initial['failure_diagnostic']['kind'] == 'ads_job_failed'
    saved = original_files(folder)
    clock[0] += timedelta(seconds=1799)
    assert tick() is False and len(client.posts) == 1
    clock[0] += timedelta(seconds=1)
    assert tick() is False and len(client.posts) == 2
    assert client.posts[0] == client.posts[1]
    unchanged(saved)


def test_ads_title_with_permanent_error_does_not_repost(setup):
    cfg, folder, client, _, tick = ready(setup)
    cfg.ads_retry_enabled = True
    client.remotes['job-1'].error = RuntimeError(ADS_FAILURE + 'Invalid variable')
    with pytest.raises(worker.BlockedRequestError):
        tick()
    assert state(folder)['phase'] == 'remote_failed' and len(client.posts) == 1


def test_gateway_recovers_original_if_it_exists(case):
    cfg, _, folder, request, _, client = case
    cfg.ads_retry_enabled = True
    saved = state(folder)
    saved['error'] = GATEWAY
    save(folder/'state.json', saved)
    candidate(client, request, status='accepted')
    outcome = submission.recover_submission(cfg, '20261024', 0, 'gust', automatic=True, client=client)
    assert outcome['status'] == 'original_recovered'
    assert state(folder)['request_id'] == 'original' and not client.posts


def test_gateway_with_no_matching_order_reserves_at_most_one_replacement(case):
    cfg, clock, folder, request, _, client = case
    cfg.ads_retry_enabled = True
    saved = state(folder)
    saved['error'] = GATEWAY
    save(folder/'state.json', saved)
    client.lose_reply = True
    # A lost replacement receipt still consumes its durable reservation.
    with pytest.raises(worker.BlockedRequestError):
        submission.recover_submission(cfg, '20261024', 0, 'gust', automatic=True, client=client)
    clock[0] += timedelta(minutes=30)
    outcome = submission.recover_submission(cfg, '20261024', 0, 'gust', automatic=True, client=client)
    assert outcome['status'] == 'replacement_spent'
    assert len(client.posts) == 1 and client.posts[0]['body'] == request


def activate(cfg, folder, saved, stamp, fingerprint):
    key = folder.relative_to(cfg.output_dir/'ec_cams').as_posix()
    row = {'account': saved['account'], 'request_sha256': saved['request_sha256'],
           'state_sha256': fingerprint, 'started_at': stamp.isoformat()}
    cfg._retry_activations = {key: row}
    return key, row


def test_existing_terminal_failure_is_reopened_only_by_its_bound_activation(setup, monkeypatch):
    cfg, folder, client, clock, tick = ready(setup)
    client.remotes['job-1'].error = RuntimeError(ADS_FAILURE)
    with pytest.raises(worker.BlockedRequestError):
        tick()
    pinned = cfg.output_dir/'ec_cams/20261027/00z/.cams/hourly'
    pinned.parent.mkdir(parents=True)
    folder.rename(pinned)
    folder = pinned
    def tick():
        return worker.group_tick(cfg, folder, '20261027', 0, 'hourly', client, '')
    saved = state(folder)
    proof = original_files(folder)
    clock[0] += timedelta(days=4)
    monkeypatch.setattr(retry_activation, 'now', lambda: clock[0].isoformat())
    monkeypatch.setattr(worker, 'newer_live_due', lambda *args: None)
    cfg.ads_retry_enabled = True
    with pytest.raises(worker.BlockedRequestError):
        tick()
    assert len(client.posts) == 1
    activate(cfg, folder, saved, clock[0], saved['failed_attempt']['state_sha256'])
    first_activation = clock[0]
    assert tick() is False and len(client.posts) == 2
    assert recovery._first_failure_at(cfg, folder, state(folder)) == first_activation
    unchanged(proof)


def test_expired_gateway_activation_checks_bound_identity_and_survives_restart(case, monkeypatch):
    cfg, clock, folder, request, _, client = case
    cfg.ads_retry_enabled = True
    saved = state(folder)
    saved['error'] = GATEWAY
    save(folder/'state.json', saved)
    original = (folder/'state.json').read_bytes()
    clock[0] += timedelta(days=4)
    monkeypatch.setattr(retry_activation, 'now', lambda: clock[0].isoformat())
    monkeypatch.setattr(worker, 'newer_live_due', lambda *args: None)
    assert submission.recover_submission(cfg, '20261024', 0, 'gust', automatic=True,
                                         client=client)['status'] == 'window_expired'
    activate(cfg, folder, saved, clock[0], digest(folder/'state.json'))
    result = submission.recover_submission(cfg, '20261024', 0, 'gust', automatic=True, client=client)
    assert result['status'] == 'replacement_submitted'
    relay = state(folder)
    archive = folder/relay['submission_recovery']['directory']
    assert (archive/'state.json').read_bytes() == original
    assert client.posts[0]['body'] == request and len(client.posts) == 1
    cfg._retry_activations = {}
    assert recovery._first_failure_at(cfg, folder, relay) == clock[0]
    assert submission.recover_submission(cfg, '20261024', 0, 'gust', automatic=True,
                                         client=client)['status'] == 'already_resumed'


def test_recovery_lookup_waits_for_live_and_is_rate_limited(case, monkeypatch):
    cfg, clock, folder, _, _, client = case
    cfg.ads_retry_enabled = True
    saved = state(folder)
    saved['error'] = GATEWAY
    save(folder/'state.json', saved)
    monkeypatch.setattr(worker, 'newer_live_due', lambda *args: '2026102500')
    assert submission.recover_submission(cfg, '20261024', 0, 'gust', automatic=True,
                                         client=client)['status'] == 'waiting_for_account_or_realtime'
    monkeypatch.setattr(worker, 'newer_live_due', lambda *args: None)
    assert submission.recover_submission(cfg, '20261024', 0, 'gust', automatic=True,
                                         client=client)['status'] == 'not_due'
    assert not client.posts
    clock[0] += timedelta(minutes=30)
    assert submission.recover_submission(cfg, '20261024', 0, 'gust', automatic=True,
                                         client=client)['status'] == 'replacement_submitted'


def test_weekly_recovers_accepted_order_before_rejecting_unknown_active_account(case, monkeypatch, tmp_path):
    cfg, clock, folder, request, _, client = case
    cfg.ads_retry_enabled = True
    cfg.history_accounts = ['a']
    cfg.max_active_orders_per_account = 1
    cfg.first_init = '2026120100'
    saved = state(folder)
    saved['error'] = GATEWAY
    save(folder/'state.json', saved)
    candidate(client, request, status='accepted')
    queue = tmp_path/'week'
    plan = backfill.make_plan('20261024', '20261024', authorized_at=clock[0].isoformat())
    save(queue/'plan.json', plan)
    save(queue/'recovery_policy.json', make_policy(queue, plan, cfg.mars_count_retry_delays_seconds,
        retry_window_seconds=86400, continue_other_weeks_when_blocked=True,
        quarantine_uncertain_submissions=True, ads_retry_enabled=True, authorized_at=clock[0].isoformat()))
    monkeypatch.setattr(backfill, 'create_client', lambda account: (client, ''))
    monkeypatch.setattr(backfill.shutil, 'disk_usage', lambda *args: (0, 0, 100 << 30))
    outcome = backfill.tick(cfg, queue, account='a')
    assert outcome['status'] == 'original_recovered' and outcome['init'] == '2026102400'
    assert state(folder)['request_id'] == 'original' and not client.posts


def test_weekly_retry_and_ambiguous_gateway_publish_one_complete_init(system, monkeypatch):
    base, clock, client, tmp = system
    cfg = base.model_copy(update={'first_init': '2026120100', 'history_accounts': ['mock-account'],
        'max_active_orders_per_account': 1, 'quarantine_uncertain_submissions': True,
        'ads_retry_enabled': True, 'mars_count_retry_delays_seconds': [1800]*48,
        'mars_count_retry_limit': 48, 'mars_count_retry_window_seconds': 86400})
    monkeypatch.setattr(submission, 'now', lambda: clock[0].isoformat())
    monkeypatch.setattr(submission, 'preflight', worker.preflight)
    monkeypatch.setattr(backfill, 'create_client', lambda account: (client, ''))
    client.failures = 0
    calls, rows = [], []
    submit = client.submit

    def post(dataset, request):
        calls.append(request)
        if len(calls) == 3:
            response = requests.Response()
            response.status_code = 502
            raise requests.HTTPError('502 Server Error: Bad Gateway for url: [URL_REDACTED]', response=response)
        remote = submit(dataset, request)
        remote.json.update(created=clock[0].isoformat(), processID=worker.DATASET)
        if len(calls) == 1:
            remote.json['status'] = 'failed'
            def fail_results():
                raise RuntimeError(ADS_FAILURE)
            remote.get_results = fail_results
        rows.insert(0, {'jobID': remote.request_id, **{k: remote.json[k] for k in ('status','created','processID')}})
        return remote

    def jobs(**kwargs):
        status = kwargs.get('status')
        selected = rows if status is None else [row for row in rows if row['status'] in status]
        return SimpleNamespace(json={'jobs': selected}, next=None)

    client.submit, client.get_jobs = post, jobs
    root = tmp/'ads-queue'
    plan = backfill.make_plan('20260704', '20260704', authorized_at=clock[0].isoformat())
    save(root/'plan.json', plan)
    save(root/'recovery_policy.json', make_policy(root, plan, [1800]*48, retry_window_seconds=86400,
        continue_other_weeks_when_blocked=True, quarantine_uncertain_submissions=True,
        ads_retry_enabled=True, authorized_at=clock[0].isoformat()))
    folder = cfg.output_dir/'ec_cams/20260704/00z'
    def tick():
        return backfill.tick(cfg, root, cycle=0)
    # Only the 00 UTC test lane is present. Its retry defers without a POST.
    trace = [tick()]
    assert len(calls) == 1 and trace[0]['deferred'][0]['init'] == '2026070400'
    evidence = original_files(folder/'.cams/hourly')
    clock[0] += timedelta(minutes=30)
    trace.append(tick())
    assert trace[-1]['reason'] == 'new_uncertain_submission' and len(calls) == 3
    assert calls[0] == calls[1]
    old_gust = (folder/'.cams/gust/state.json').read_bytes()
    clock[0] += timedelta(minutes=29, seconds=59)
    trace.append(tick())
    assert len(calls) == 3
    clock[0] += timedelta(seconds=1)
    trace.append(tick())
    assert trace[-1]['status'] == 'replacement_submitted' and calls[2] == calls[3]
    trace.append(tick())
    assert trace[-1]['status'] == 'complete' and len(calls) == 5
    raw = json.loads((folder/'20260704_00z_ec_cams.manifest.json').read_text())
    assert len(raw['files']) == 121 and len(raw['variables']) == 52
    assert sum(row['records_selected'] for row in raw['files']) == 5732
    assert backfill.active_week(root, plan) == plan['weeks'][0]
    assert recovery.init_used_count(cfg, folder/'.cams/gust', state(folder/'.cams/gust')) == 2
    assert next((folder/'.cams/gust/uncertain').glob('*/state.json')).read_bytes() == old_gust
    unchanged(evidence)
    report('ads-verified-recovery', cfg, client, trace, {'messages': 5732, 'variables': 52,
        'retained_originals': True, 'confirmed_failure_retries': 1, 'gateway_replacements': 1})
