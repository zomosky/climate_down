"""Only a confirmed deleted job can receive one identical, evidenced replacement."""
import datetime as dt
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from climate_download.cams import recovery, worker
from climate_download.cams.state import digest, save
from test_cams_mars_recovery import setup, state
from test_cams_daily_recovery import daily
from test_cams_partition import partition_setup


class DeletedError(RuntimeError):
    def __init__(self, rid='job-1', status=404, reason='deleted'):
        self.response=SimpleNamespace(status_code=status)
        super().__init__(f'{status} Client Error: job not found; job {rid} {reason}')


@pytest.fixture(autouse=True)
def deletion_clock(setup,monkeypatch):
    from climate_download.cams import deleted
    monkeypatch.setattr(deleted,'now',lambda:setup[3][0].isoformat())


def enable(setup):
    config,folder,client,clock,tick=setup
    daily(config)
    object.__setattr__(config,'deleted_order_retry_limit',1)
    current=state(folder);current['mars_retry_policy']=recovery.policy_for(config);save(folder/'state.json',current)
    client.all_jobs=[]
    def jobs(**kwargs):
        return SimpleNamespace(json={'jobs':client.active if kwargs.get('status') else client.all_jobs},next=None)
    client.get_jobs=jobs
    original=client.get_remote
    def remote(rid):
        if rid=='job-1':raise DeletedError()
        return original(rid)
    client.get_remote=remote
    return config,folder,client,clock,tick


def test_deleted_original_is_archived_then_replaced_once_with_identical_parameters(setup):
    config,folder,client,clock,tick=enable(setup)
    before=(folder/'state.json').read_bytes();request=(folder/'request.json').read_bytes()
    assert tick() is False
    assert state(folder)['phase']=='retry_wait'
    proof=state(folder)['failed_attempt'];archive=folder/proof['directory']
    assert (archive/'state.json').read_bytes()==before and (archive/'request.json').read_bytes()==request
    failure=json.loads((archive/'failure.json').read_text())
    assert failure['diagnostic']['kind']=='ads_order_deleted' and failure['deleted_response_verified']
    originals={str(p):digest(p) for p in archive.glob('*.json')}
    clock[0]+=dt.timedelta(minutes=30);assert tick() is False
    assert len(client.posts)==2 and client.posts[1]==client.posts[0]
    assert state(folder)['retry_count']==1 and state(folder)['previous_attempts'][0]['request_id']=='job-1'
    client.get_remote=lambda rid: (_ for _ in ()).throw(DeletedError(rid))
    with pytest.raises(worker.BlockedRequestError):tick()
    clock[0]+=dt.timedelta(minutes=30)
    with pytest.raises(worker.BlockedRequestError):tick()
    assert len(client.posts)==2 and all(digest(Path(p))==value for p,value in originals.items())


def test_saved_legacy_404_preserves_first_failure_time_and_does_not_gain_a_new_day(setup):
    config,folder,client,clock,tick=enable(setup)
    current=state(folder)
    current.update(phase='remote_failed',last_check=(clock[0]-dt.timedelta(hours=23)).isoformat(),
                   error={'type':'HTTPError','message':str(DeletedError())})
    save(folder/'state.json',current)
    assert tick() is False
    assert len(client.posts)==2
    assert recovery._first_failure_at(config,folder,state(folder))==clock[0]-dt.timedelta(hours=23)
    assert tick() is False and len(client.posts)==2


@pytest.mark.parametrize('status,reason,rid',[(404,'not found','job-1'),(403,'deleted','job-1'),
    (404,'deleted','another-job'),(500,'deleted','job-1')])
def test_ambiguous_404_wrong_job_or_other_status_never_reposts(setup,status,reason,rid):
    config,folder,client,clock,tick=enable(setup)
    client.get_remote=lambda _: (_ for _ in ()).throw(DeletedError(rid,status,reason))
    try:tick()
    except worker.BlockedRequestError:pass
    clock[0]+=dt.timedelta(hours=1)
    try:tick()
    except worker.BlockedRequestError:pass
    assert len(client.posts)==1


@pytest.mark.parametrize('damage',['raw','ranges','request','archive'])
def test_deleted_order_recovery_preserves_all_existing_identity_guards(setup,damage):
    config,folder,client,clock,tick=enable(setup)
    if damage=='raw':(folder/'download.grib.part').write_bytes(b'original')
    elif damage=='ranges':(folder/'download.grib.part.ranges').mkdir()
    elif damage=='request':save(folder/'request.json',{'changed':True})
    else:
        tick();(folder/state(folder)['failed_attempt']['directory']/'failure.json').write_text('changed')
    clock[0]+=dt.timedelta(minutes=30)
    with pytest.raises((worker.BlockedRequestError,ValueError)):tick()
    assert len(client.posts)==1


def test_active_job_delays_recovery_without_spending_the_attempt(setup):
    config,folder,client,clock,tick=enable(setup);tick();clock[0]+=dt.timedelta(minutes=30)
    client.active=[{'jobID':'other','status':'running'}]
    assert tick() is False and len(client.posts)==1
    client.active=[];assert tick() is False and len(client.posts)==2


def test_matching_successful_job_prevents_duplicate_replacement(setup):
    config,folder,client,clock,tick=enable(setup);tick();clock[0]+=dt.timedelta(minutes=30)
    request=client.posts[0]
    client.all_jobs=[{'jobID':'already-done','status':'successful','processID':state(folder)['dataset'],
                      'created':clock[0].isoformat()}]
    original=client.get_remote
    client.get_remote=lambda rid:SimpleNamespace(json={'status':'successful','metadata':{'request':{'ids':request}}}) if rid=='already-done' else original(rid)
    with pytest.raises(worker.BlockedRequestError):tick()
    assert len(client.posts)==1


def test_lost_deleted_replacement_response_is_not_retried_after_restart(setup):
    config,folder,client,clock,tick=enable(setup);tick();clock[0]+=dt.timedelta(minutes=30)
    client.on_submit=lambda: (_ for _ in ()).throw(TimeoutError('lost reply'))
    with pytest.raises(worker.BlockedRequestError):tick()
    assert state(folder)['phase']=='submission_uncertain' and state(folder)['retry_count']==1
    clock[0]+=dt.timedelta(hours=1)
    with pytest.raises(worker.BlockedRequestError):tick()
    assert len(client.posts)==2


def test_expired_legacy_deleted_order_does_not_get_a_new_day(setup):
    config,folder,client,clock,tick=enable(setup)
    current=state(folder);current.update(phase='remote_failed',last_check=(clock[0]-dt.timedelta(hours=25)).isoformat(),
        error={'type':'HTTPError','message':str(DeletedError())});save(folder/'state.json',current)
    with pytest.raises(worker.BlockedRequestError):tick()
    assert len(client.posts)==1 and state(folder)['failure_diagnostic']['kind']=='ads_order_deleted'


def test_mars_failed_order_deleted_later_retains_original_mars_archive(setup):
    config,folder,client,clock,tick=enable(setup)
    client.get_remote=lambda rid:client.remotes[rid]
    client.remotes['job-1'].json['status']='failed';tick()
    original={str(p):digest(p) for p in folder.glob('failures/*/*.json')}
    client.get_remote=lambda rid: (_ for _ in ()).throw(DeletedError()) if rid=='job-1' else client.remotes[rid]
    clock[0]+=dt.timedelta(minutes=30);tick();tick()
    assert len(client.posts)==2 and all(digest(Path(p))==value for p,value in original.items())


@pytest.mark.parametrize('status',['deleted','dismissed'])
def test_positive_terminal_status_can_be_recovered_without_fabricating_a_404(setup,status):
    config,folder,client,clock,tick=enable(setup)
    client.get_remote=lambda rid:client.remotes[rid]
    client.remotes['job-1'].json['status']=status
    assert tick() is False
    proof=folder/state(folder)['failed_attempt']['directory']/'failure.json'
    assert json.loads(proof.read_text())['diagnostic']['http_status'] is None
    clock[0]+=dt.timedelta(minutes=30);tick()
    assert len(client.posts)==2


def test_a_reactivated_original_is_rejoined_instead_of_replaced(setup):
    config,folder,client,clock,tick=enable(setup);tick();clock[0]+=dt.timedelta(minutes=30)
    client.get_remote=lambda rid:client.remotes[rid]
    assert tick() is False and len(client.posts)==1
    assert state(folder)['request_id']=='job-1' and state(folder)['phase']=='waiting_for_remote'


def test_transient_metadata_failure_does_not_spend_deleted_budget(setup):
    config,folder,client,clock,tick=enable(setup);tick();clock[0]+=dt.timedelta(minutes=30)
    original=client.get_jobs
    client.get_jobs=lambda **kw: (_ for _ in ()).throw(TimeoutError('temporary'))
    assert tick() is False and len(client.posts)==1
    assert state(folder)['retry_wait_reason']=='metadata_deferred'
    client.get_jobs=original;tick();assert len(client.posts)==2


def test_duplicate_order_on_second_page_is_detected(setup):
    config,folder,client,clock,tick=enable(setup);tick();clock[0]+=dt.timedelta(minutes=30)
    request=client.posts[0]
    second=SimpleNamespace(json={'jobs':[{'jobID':'duplicate','status':'successful',
        'processID':state(folder)['dataset'],'created':clock[0].isoformat()}]},next=None)
    first=SimpleNamespace(json={'jobs':[{'jobID':'unrelated','processID':'other',
        'created':clock[0].isoformat()}]},next=second)
    client.get_jobs=lambda **kw:SimpleNamespace(json={'jobs':[]}) if kw.get('status') else first
    original=client.get_remote
    client.get_remote=lambda rid:SimpleNamespace(json={'status':'successful','metadata':{'request':{'ids':request}}}) if rid=='duplicate' else original(rid)
    with pytest.raises(worker.BlockedRequestError):tick()
    assert len(client.posts)==1


def test_expiry_after_deleted_preflight_does_not_submit(setup,monkeypatch):
    config,folder,client,clock,tick=enable(setup);tick();clock[0]+=dt.timedelta(hours=24)
    def slow(*args):clock[0]+=dt.timedelta(seconds=1);return {'policy_gate':'passed'}
    monkeypatch.setattr(worker,'preflight',slow)
    with pytest.raises(worker.BlockedRequestError):tick()
    assert len(client.posts)==1


def test_deleted_budget_is_shared_across_groups(setup):
    config,folder,client,clock,tick=enable(setup);tick();clock[0]+=dt.timedelta(minutes=30)
    save(config.output_dir/'ec_cams/20261027/00z/.cams/gust/state.json',
         {'retry_count':1,'previous_attempts':[{'request_id':'older','failure_kind':'ads_order_deleted'}]})
    with pytest.raises(worker.BlockedRequestError):tick()
    assert len(client.posts)==1


def test_failed_native_parent_reenters_only_its_pinned_deleted_child(partition_setup,setup):
    from climate_download.cams.partition import tick as piece_tick
    config,folder,client=partition_setup;daily(config)
    assert piece_tick(config,folder,'20261027',0,client,'') is False
    original=client.get_remote
    client.get_remote=lambda rid: (_ for _ in ()).throw(DeletedError(rid)) if rid=='piece-1' else original(rid)
    with pytest.raises(worker.BlockedRequestError):piece_tick(config,folder,'20261027',0,client,'')
    plan=digest(folder/'partition.json');config.deleted_order_retry_limit=1
    client.get_jobs=lambda **kw:SimpleNamespace(json={'jobs':[]},next=None)
    assert piece_tick(config,folder,'20261027',0,client,'') is False
    assert state(folder)['phase']=='retry_wait'
    setup[3][0]+=dt.timedelta(minutes=30)
    assert piece_tick(config,folder,'20261027',0,client,'') is False
    assert len(client.posts)==2 and client.posts[0]==client.posts[1]
    assert digest(folder/'partition.json')==plan


def test_ads_naive_utc_job_timestamps_still_detect_matching_orders(setup):
    config,folder,client,clock,tick=enable(setup);tick();clock[0]+=dt.timedelta(minutes=30)
    request=client.posts[0]
    client.all_jobs=[{'jobID':'duplicate','status':'successful','processID':state(folder)['dataset'],
                      'created':clock[0].replace(tzinfo=None).isoformat()}]
    original=client.get_remote
    client.get_remote=lambda rid:SimpleNamespace(json={'status':'successful','metadata':{'request':{'ids':request}}}) if rid=='duplicate' else original(rid)
    with pytest.raises(worker.BlockedRequestError):tick()
    assert len(client.posts)==1
