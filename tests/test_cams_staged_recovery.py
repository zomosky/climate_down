"""Multi-hour recovery budgets must survive restarts without starving live/history."""
import datetime as dt
import fcntl
import json
from pathlib import Path
import shutil
from types import SimpleNamespace

import pytest

from climate_download.cams import recovery, worker
from climate_download.cams.config import CamsConfig
from climate_download.cams.state import digest, save
from test_cams_mars_recovery import setup, failed, state
from test_cams_partition import partition_setup


DELAYS=[1800,7200,21600]


def staged(config):
    config.mars_count_retry_limit=3
    config.mars_count_retry_delays_seconds=DELAYS.copy()
    return config


def test_three_stages_preserve_every_failed_order_and_stop_after_three_replacements(setup):
    config,folder,client,clock,tick=setup;staged(config)
    payload=(folder/'request.json').read_bytes();archives={}
    for index,delay in enumerate(DELAYS):
        client.remotes[f'job-{index+1}'].json['status']='failed'
        assert tick() is False
        current=state(folder)
        assert current['phase']=='retry_wait'
        assert current['mars_retry_policy']['delays_seconds']==DELAYS
        assert dt.datetime.fromisoformat(current['retry_due_at'])==clock[0]+dt.timedelta(seconds=delay)
        archives.update({str(p):digest(p) for p in (folder/'failures').glob('*/*.json')})
        clock[0]+=dt.timedelta(seconds=delay-1)
        assert tick() is False and len(client.posts)==index+1
        clock[0]+=dt.timedelta(seconds=1)
        assert tick() is False and len(client.posts)==index+2
        assert state(folder)['retry_count']==index+1
        assert all(digest(Path(p))==checksum for p,checksum in archives.items())
    client.remotes['job-4'].json['status']='failed'
    with pytest.raises(worker.BlockedRequestError):tick()
    clock[0]+=dt.timedelta(days=3)
    with pytest.raises(worker.BlockedRequestError):tick()
    assert len(client.posts)==4 and len(list((folder/'failures').glob('*/failure.json')))==4
    assert (folder/'request.json').read_bytes()==payload
    assert all(request==client.posts[0] for request in client.posts)


def test_staged_policy_is_captured_for_new_requests(setup):
    config,*_=setup;staged(config)
    policy=recovery.policy_for(config)
    assert policy['schema_version']==2 and policy['max_retries']==3
    assert policy['delays_seconds']==DELAYS and policy['per_init_limit']==3


def test_legacy_exhausted_failure_is_enrolled_once_without_resetting_used_budget(setup):
    config,folder,client,clock,tick=setup
    failed(setup)();clock[0]+=dt.timedelta(seconds=1800);tick()
    client.remotes['job-2'].json['status']='failed'
    with pytest.raises(worker.BlockedRequestError):tick()
    archived={str(p):digest(p) for p in (folder/'failures').glob('*/*.json')}
    staged(config)
    assert tick() is False and state(folder)['retry_count']==1
    assert dt.datetime.fromisoformat(state(folder)['retry_due_at'])==clock[0]+dt.timedelta(seconds=7200)
    assert state(folder)['staged_policy_enrollment']['previous_max_retries']==1
    clock[0]+=dt.timedelta(seconds=7200);tick()
    assert len(client.posts)==3 and state(folder)['retry_count']==2
    assert all(digest(Path(p))==checksum for p,checksum in archived.items())


def test_restart_and_config_change_cannot_shorten_stage_or_reset_budget(setup):
    config,folder,client,clock,tick=setup;staged(config)
    failed(setup)();clock[0]+=dt.timedelta(seconds=1800);tick()
    client.remotes['job-2'].json['status']='failed';tick()
    due=state(folder)['retry_due_at']
    config.mars_count_retry_delays_seconds=[300,600,900]
    clock[0]+=dt.timedelta(seconds=1800)
    assert tick() is False and len(client.posts)==2 and state(folder)['retry_due_at']==due
    clock[0]=dt.datetime.fromisoformat(due);tick()
    assert len(client.posts)==3 and state(folder)['retry_count']==2


def test_per_init_budget_is_shared_across_native_pieces_and_other_groups(setup):
    config,folder,client,clock,tick=setup;staged(config)
    sibling=config.output_dir/'ec_cams/20261027/00z/.cams/hourly/parts/sibling/state.json'
    save(sibling,{'date':'20261027','cycle':0,'retry_count':2})
    failed(setup)();clock[0]+=dt.timedelta(seconds=1800);tick()
    client.remotes['job-2'].json['status']='failed'
    with pytest.raises(worker.BlockedRequestError):tick()
    assert len(client.posts)==2 and state(folder)['retry_count']==1


def test_waiting_recovery_releases_account_and_does_not_sleep_inside_run(setup,monkeypatch):
    config,folder,client,_,_=setup;staged(config);failed(setup)()
    actual=config.output_dir/'ec_cams/20261027/00z/.cams/hourly'
    shutil.copytree(folder,actual)
    config.max_run_seconds=3600
    monkeypatch.setattr(worker.time,'sleep',lambda *a:pytest.fail('recovery must release its lock'))
    result=worker.run_init(config,'20261027',0,client=client)
    assert result['status']=='recovery_deferred' and result['retry_due_at']==state(actual)['retry_due_at']
    with (config.output_dir/'ec_cams/.locks/account-a.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    assert len(client.posts)==1


def test_waiting_recovery_remains_scheduled_but_does_not_hold_history_priority(setup):
    config,folder,_,_,_=setup;staged(config);failed(setup)()
    actual=config.output_dir/'ec_cams/20261027/00z/.cams/hourly';shutil.copytree(folder,actual)
    config.first_init='2026102700'
    at=dt.datetime(2026,10,28,6,tzinfo=dt.timezone.utc)
    assert '2026102700' in worker.scheduled_inits(config,0,at)
    assert '2026102700' not in worker.pending_inits(config,0,at)
    later=at+dt.timedelta(days=5)
    assert '2026102700' in worker.scheduled_inits(config,0,later)


@pytest.mark.parametrize('mode',['native_constraint_cubes','bundle'])
def test_cli_continues_other_inits_after_a_deferred_recovery(setup,monkeypatch,mode):
    from climate_download import cli
    from climate_download.cams import config as module
    config,*_=setup;staged(config);config.hourly_request_mode=mode
    monkeypatch.setattr(module,'load_config',lambda p:config)
    monkeypatch.setattr(worker,'scheduled_inits',lambda *a:['2026102700','2026102800'])
    calls=[]
    def run(cfg,date,cycle):
        calls.append(date)
        return {'status':'recovery_deferred' if date=='20261028' else 'pending','date':date,'cycle':cycle}
    monkeypatch.setattr(worker,'run_init',run)
    args=SimpleNamespace(config='unused',once=True,date=None,cycle=0,scheduled=True,preflight_only=False)
    assert cli.cmd_cams(args)==0 and calls==['20261028','20261027']


@pytest.mark.parametrize('problem',['raw','ranges','tampered_archive'])
def test_staged_enrollment_does_not_bypass_existing_evidence_guards(setup,problem):
    config,folder,client,clock,tick=setup;staged(config)
    if problem=='raw':(folder/'download.grib.part').write_bytes(b'original')
    if problem=='ranges':(folder/'download.grib.part.ranges').mkdir()
    if problem=='tampered_archive':
        failed(setup)()
        (folder/state(folder)['failed_attempt']['directory']/'failure.json').write_text('changed')
        clock[0]+=dt.timedelta(seconds=1800)
    with pytest.raises(worker.BlockedRequestError):
        (tick if problem=='tampered_archive' else failed(setup))()
    assert len(client.posts)==1


@pytest.mark.parametrize('values', [[1800,300], [1800,1800], [0,7200,21600], [1800,7200],
                                   [1800,7200,21600,43200]])
def test_invalid_staged_config_is_rejected(setup,values):
    config,*_=setup
    with pytest.raises(ValueError):
        CamsConfig.model_validate({**config.model_dump(),'mars_count_retry_limit':3,
                                   'mars_count_retry_delays_seconds':values})


@pytest.mark.parametrize('damage',['deadline','policy','enrollment'])
def test_retained_stage_cannot_be_shortened_by_state_or_enrollment_drift(setup,damage):
    config,folder,client,clock,tick=setup;staged(config);failed(setup)()
    current=state(folder)
    if damage=='deadline':current['retry_due_at']=clock[0].isoformat()
    elif damage=='policy':current['mars_retry_policy']['delays_seconds']=[300,600,900]
    else:
        path=folder/current['staged_policy_enrollment']['directory']
        path.write_text('changed authorization evidence')
    save(folder/'state.json',current)
    clock[0]+=dt.timedelta(seconds=1800)
    with pytest.raises(worker.BlockedRequestError):tick()
    assert len(client.posts)==1


def test_staged_unknown_submit_keeps_reserved_count_and_never_enrolls_again(setup):
    config,folder,client,clock,tick=setup;staged(config);failed(setup)()
    clock[0]+=dt.timedelta(seconds=1800)
    def lost():raise TimeoutError('token-value https://signed.invalid/?token=token-value')
    client.on_submit=lost
    with pytest.raises(worker.BlockedRequestError):tick()
    assert state(folder)['phase']=='submission_uncertain' and state(folder)['retry_count']==1
    clock[0]+=dt.timedelta(days=5)
    with pytest.raises(worker.BlockedRequestError):tick()
    assert len(client.posts)==2 and 'token-value' not in (folder/'state.json').read_text()


def test_reactivated_original_is_rejoined_without_a_replacement(setup):
    config,folder,client,clock,tick=setup;staged(config);failed(setup)()
    clock[0]+=dt.timedelta(seconds=1800);client.remotes['job-1'].json['status']='running'
    assert tick() is False
    assert state(folder)['phase']=='waiting_for_remote' and state(folder)['request_id']=='job-1'
    assert state(folder)['retry_count']==0 and len(client.posts)==1


def test_partial_enrollment_after_a_crash_blocks_only_that_init(setup):
    config,folder,client,_,tick=setup;failed(setup)()
    archive=folder/state(folder)['failed_attempt']['directory']
    before={p.name:digest(p) for p in archive.glob('*.json')}
    (archive/'staged_policy_enrollment.json').write_text('{partial')
    staged(config)
    with pytest.raises(worker.BlockedRequestError):tick()
    assert len(client.posts)==1 and all(digest(archive/name)==checksum for name,checksum in before.items())


@pytest.mark.parametrize('damage',[None,'missing_plan','changed_plan'])
def test_existing_failed_native_piece_can_enroll_only_with_its_pinned_plan(partition_setup,damage):
    from climate_download.cams.partition import tick
    config,folder,client=partition_setup
    assert tick(config,folder,'20261027',0,client,'') is False
    client.remotes['piece-1'].json['status']='failed'
    with pytest.raises(worker.BlockedRequestError):tick(config,folder,'20261027',0,client,'')
    before={str(p):digest(p) for p in folder.glob('parts/*/failures/*/*.json')}
    staged(config)
    if damage=='missing_plan':(folder/'partition.json').unlink()
    elif damage=='changed_plan':(folder/'partition.json').write_text('{}')
    if damage:
        with pytest.raises(worker.BlockedRequestError):tick(config,folder,'20261027',0,client,'')
    else:
        assert tick(config,folder,'20261027',0,client,'') is False
        assert state(folder)['phase']=='retry_wait' and state(folder)['retry_due_at']
    assert len(client.posts)==1 and all(digest(Path(p))==checksum for p,checksum in before.items())
