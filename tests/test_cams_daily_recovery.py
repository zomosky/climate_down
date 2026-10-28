"""Half-hourly recovery has one absolute day per init, across restarts/pieces."""
import datetime as dt
import json
from pathlib import Path

import pytest

from climate_download.cams import recovery, worker
from climate_download.cams.config import CamsConfig, load_config
from climate_download.cams.queue_policy import make_policy, load_policy
from climate_download.cams.state import digest, save
from test_cams_mars_recovery import setup, failed, state
from test_cams_staged_recovery import staged


def daily(config):
    config.mars_count_retry_limit = 48
    config.mars_count_retry_delays_seconds = [1800]*48
    config.mars_count_retry_window_seconds = 86400


def test_production_config_has_half_hourly_one_day_bounds():
    config = load_config(Path('config/jobs/cams_operational.yaml'))
    assert config.mars_count_retry_limit == 48
    assert config.mars_count_retry_delays_seconds == [1800]*48
    assert config.mars_count_retry_window_seconds == 86400


def test_48_replacements_retain_49_failures_and_never_reset_on_restart(setup):
    config, folder, client, clock, tick = setup
    daily(config)
    assert failed(setup)() is False
    original = {str(p): digest(p) for p in folder.glob('failures/*/*.json')}
    for attempt in range(1,49):
        clock[0] += dt.timedelta(seconds=1799)
        assert tick() is False and len(client.posts) == attempt
        clock[0] += dt.timedelta(seconds=1)
        assert tick() is False and len(client.posts) == attempt+1
        client.remotes[f'job-{attempt+1}'].json['status'] = 'failed'
        if attempt < 48:
            assert tick() is False
        else:
            with pytest.raises(worker.BlockedRequestError):tick()
    assert state(folder)['retry_count'] == 48
    assert len(list(folder.glob('failures/*/failure.json'))) == 49
    clock[0] += dt.timedelta(days=3)
    with pytest.raises(worker.BlockedRequestError):tick()
    assert len(client.posts) == 49 and all(p == client.posts[0] for p in client.posts)
    assert all(digest(Path(p)) == value for p,value in original.items())


def test_remote_extraction_time_does_not_restart_the_24_hour_window(setup):
    config,folder,client,clock,tick = setup
    daily(config);failed(setup)()
    started = clock[0]
    clock[0] += dt.timedelta(minutes=30);tick()
    clock[0] += dt.timedelta(hours=23)
    client.remotes['job-2'].json['status'] = 'failed'
    assert tick() is False
    assert recovery._first_failure_at(config,folder,state(folder)) == started
    clock[0] += dt.timedelta(minutes=31)
    with pytest.raises(worker.BlockedRequestError):tick()
    assert len(client.posts) == 2 and state(folder)['retry_count'] == 1


def test_preflight_crossing_the_deadline_does_not_post(setup,monkeypatch):
    config,folder,client,clock,tick = setup
    daily(config);failed(setup)()
    clock[0] += dt.timedelta(hours=24)
    def slow(*args):
        clock[0] += dt.timedelta(seconds=1)
        return {'cost':1,'policy_gate':'passed'}
    monkeypatch.setattr(worker,'preflight',slow)
    with pytest.raises(worker.BlockedRequestError):tick()
    assert len(client.posts) == 1 and state(folder).get('retry_count',0) == 0


def test_existing_staged_policy_can_upgrade_without_resetting_used_count(setup):
    config,folder,client,clock,tick = setup
    staged(config);failed(setup)()
    clock[0] += dt.timedelta(minutes=30);tick()
    client.remotes['job-2'].json['status'] = 'failed';tick()
    original = {str(p):digest(p) for p in folder.glob('failures/*/*.json')}
    daily(config)
    assert tick() is False and state(folder)['retry_count'] == 1
    assert state(folder)['mars_retry_policy']['schema_version'] == 3
    clock[0] += dt.timedelta(minutes=30);tick()
    assert len(client.posts) == 3 and state(folder)['retry_count'] == 2
    assert all(digest(Path(p)) == value for p,value in original.items())


def test_two_policy_enrollments_before_replacement_keep_the_original_chain(setup):
    config,folder,client,clock,tick = setup
    failed(setup)();staged(config);tick();daily(config);tick()
    clock[0] += dt.timedelta(minutes=30)
    assert tick() is False and len(client.posts) == 2


def test_expired_staged_failure_cannot_gain_a_fresh_day(setup):
    config,folder,client,clock,tick = setup
    staged(config);failed(setup)()
    clock[0] += dt.timedelta(days=2);daily(config)
    with pytest.raises(worker.BlockedRequestError):tick()
    assert state(folder)['mars_retry_policy']['schema_version'] == 2 and len(client.posts) == 1


def test_all_groups_share_the_first_failure_window(setup):
    config,folder,client,clock,tick = setup
    daily(config);failed(setup)()
    earliest = clock[0]-dt.timedelta(hours=24,seconds=1)
    sibling = config.output_dir/'ec_cams/20261027/00z/.cams/gust/failures/old/failure.json'
    save(sibling,{'captured_at':earliest.isoformat(),'diagnostic':{'kind':'mars_field_count_mismatch'},
                  'local_request_verified':True,'remote_request_verified':True})
    with pytest.raises(worker.BlockedRequestError):tick()
    assert len(client.posts) == 1


@pytest.mark.parametrize('window,limit,delays',[(86401,48,[1800]*48),(86400,49,[1800]*49),
    (86400,48,[900]*48),(3600,3,[1800]*3),(86400,1,None)])
def test_daily_config_rejects_unbounded_or_non_half_hourly_values(setup,window,limit,delays):
    config,*_ = setup
    with pytest.raises(ValueError):
        CamsConfig.model_validate({**config.model_dump(),'mars_count_retry_limit':limit,
            'mars_count_retry_delays_seconds':delays,'mars_count_retry_window_seconds':window})


def test_daily_queue_authorization_is_bound_to_the_same_day_and_original_plan(tmp_path):
    plan={'source':'ec_cams','dataset':'cams-global-atmospheric-composition-forecasts'}
    save(tmp_path/'plan.json',plan);before=digest(tmp_path/'plan.json')
    policy=make_policy(tmp_path,plan,[1800]*48,authorized_at='2026-10-28T08:00:00+00:00',retry_window_seconds=86400)
    save(tmp_path/'recovery_policy.json',policy)
    assert load_policy(tmp_path,plan)==policy and digest(tmp_path/'plan.json')==before
    policy['retry_window_seconds']=172800;save(tmp_path/'recovery_policy.json',policy)
    with pytest.raises(ValueError):load_policy(tmp_path,plan)
