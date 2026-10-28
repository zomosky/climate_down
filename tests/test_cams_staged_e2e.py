"""Clock-driven scheduler→ADS→recovery→native union→manifest acceptance.

ADS and the clock are fakes; no network requests or real production data writes.
The actual worker, partition, recovery, locks, publication and weekly runner run.
"""
import copy
import datetime as dt
import json
import os
from pathlib import Path
import shutil
from types import SimpleNamespace

import pytest

from climate_download.cams import backfill, grib, partition, recovery, state, worker
from climate_download.cams.catalogue import VARIABLES
from climate_download.cams.config import CamsConfig
from climate_download.cams.queue_policy import make_policy
from test_cams import header
from test_cams_partition import constraint_rows


@pytest.fixture
def system(tmp_path,monkeypatch):
    clock=[dt.datetime(2026,10,27,10,tzinfo=dt.timezone.utc)]
    stamp=lambda:clock[0].isoformat()
    for module in (state,worker,recovery,partition,grib,backfill):monkeypatch.setattr(module,'now',stamp)
    config=CamsConfig(output_dir=tmp_path/'data',first_init='2026102700',max_run_seconds=3600,
        accounts=[{'name':'mock-account','credential_file':tmp_path/'unused'}],
        cycle_accounts={0:'mock-account',12:'mock-account'},hourly_request_mode='native_constraint_cubes',
        mars_count_retry_limit=3,mars_count_retry_delays_seconds=[1800,7200,21600])
    monkeypatch.setattr(worker.time,'sleep',lambda *a:pytest.fail('staged waiting must return, not sleep'))
    monkeypatch.setattr(backfill.shutil,'disk_usage',lambda *a:(0,0,10**13))
    by_name={v.api_name:v for v in VARIABLES};by_param={v.param_id:v for v in VARIABLES}
    def messages(path):
        raw=path.read_bytes();assert len(raw)%19==0
        for offset in range(0,len(raw),19):
            message=raw[offset:offset+19];assert message[:4]==b'GRIB' and message[-4:]==b'7777'
            date=int.from_bytes(message[4:8],'big');cycle=message[8]
            param=int.from_bytes(message[9:13],'big');lead=int.from_bytes(message[13:15],'big')
            h=header(by_param[param],lead);h.update(dataDate=date,dataTime=cycle*100)
            valid=dt.datetime.strptime(str(date),'%Y%m%d').replace(hour=cycle)+dt.timedelta(hours=lead)
            h.update(validityDate=int(valid.strftime('%Y%m%d')),validityTime=int(valid.strftime('%H%M')))
            yield h,message
    monkeypatch.setattr(grib,'iter_messages',messages)
    def transfer(remote,part,*args,**kwargs):
        request=remote.request;date=int(request['date'].replace('-',''));cycle=int(request['time'][0][:2])
        raw=b''.join(b'GRIB'+date.to_bytes(4,'big')+bytes([cycle])+by_name[name].param_id.to_bytes(4,'big')
                     +int(lead).to_bytes(2,'big')+b'7777'
                     for name in request['variable'] for lead in request['leadtime_hour'])
        part.write_bytes(raw)
        return {'expected_bytes':len(raw),'partial_bytes':len(raw)},True
    monkeypatch.setattr(worker,'transfer',transfer)
    monkeypatch.setattr(worker,'preflight',lambda client,request,group:{'policy_gate':'passed',
        'cost':len(request['variable'])*len(request['leadtime_hour'])})
    class Remote:
        def __init__(self,rid,request,failed):
            self.request_id=rid;self.request=copy.deepcopy(request)
            self.json={'status':'failed' if failed else 'successful',
                       'metadata':{'request':{'ids':copy.deepcopy(request)}}}
        def get_results(self):
            count=len(self.request['variable'])*len(self.request['leadtime_hour'])
            raise RuntimeError(f'MarsRuntimeError mars - ERROR - Last error is -43 Expected {count}, got {count+54}.')
    class Client:
        def __init__(self):self.posts=[];self.remotes={};self.failures=3;self.first_key=None
        def get_collection(self,*a):return SimpleNamespace(constraints=constraint_rows(0))
        def get_jobs(self,**kwargs):return SimpleNamespace(json={'jobs':[]})
        def get_remote(self,rid):return self.remotes[rid]
        def submit(self,dataset,request):
            key=state.signature(request)
            if self.first_key is None:self.first_key=key
            same=sum(p['request_sha256']==key for p in self.posts)
            failed=key==self.first_key and same<self.failures
            rid=f'mock-order-{len(self.posts)+1:03d}'
            self.posts.append({'request_id':rid,'at':stamp(),'date':request['date'],
                               'request_sha256':key,'result':'failed' if failed else 'successful'})
            remote=Remote(rid,request,failed);self.remotes[rid]=remote;return remote
    client=Client();monkeypatch.setattr(worker,'create_client',lambda account:(client,''))
    return config,clock,client,tmp_path


def report(name,config,client,trace,extra):
    value={'environment':'offline_fake_ADS_and_clock','production_data_modified':False,
           'passed':True,'trace':trace,'posts':client.posts,**extra}
    destination=os.environ.get('CAMS_RECOVERY_E2E_REPORT_DIR')
    if destination:
        folder=Path(destination)/name;folder.mkdir(parents=True,exist_ok=True)
        for path in config.output_dir.rglob('*.json'):
            target=folder/'state'/path.relative_to(config.output_dir)
            target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
        state.save(folder/'report.json',value)
    return value


def test_full_native_recovery_covers_8p5_hours_and_history_runs_while_waiting(system):
    config,clock,client,tmp=system;trace=[]
    result=worker.run_init(config,'20261027',0)
    assert result['status']=='recovery_deferred' and len(client.posts)==1
    trace.append({'at':clock[0].isoformat(),**result})
    assert not worker.pending_inits(config,0,clock[0])
    root=tmp/'weekly';plan=backfill.make_plan('20260908','20260914',authorized_at=clock[0].isoformat())
    state.save(root/'plan.json',plan)
    state.save(root/'recovery_policy.json',make_policy(root,plan,[1800,7200,21600],authorized_at=clock[0].isoformat()))
    history=backfill.tick(config,root,0)
    assert history['status']=='complete' and history['init']=='2026090800'
    original={str(p):state.digest(p) for p in config.output_dir.glob('ec_cams/20261027/00z/.cams/hourly/parts/*/failures/*/*.json')}
    began=clock[0]
    for delay in (1800,7200,21600):
        clock[0]+=dt.timedelta(seconds=delay-1)
        before=len(client.posts)
        assert worker.run_init(config,'20261027',0)['status']=='recovery_deferred'
        assert len(client.posts)==before
        clock[0]+=dt.timedelta(seconds=1)
        config=CamsConfig.model_validate(config.model_dump())  # restart: only disk retains retry state
        result=worker.run_init(config,'20261027',0)
        trace.append({'at':clock[0].isoformat(),**result})
    assert clock[0]-began==dt.timedelta(hours=8.5) and result['status']=='complete'
    path=Path(result['manifest']);manifest=json.loads(path.read_text())
    assert len(manifest['variables'])==52 and len(manifest['files'])==121
    assert sum(f['records_selected'] for f in manifest['files'])==5732
    assert grib.verify_published(path.parent,'20261027',0)
    assert all(state.digest(Path(p))==checksum for p,checksum in original.items())
    replacements=[p for p in client.posts if p['request_sha256']==client.first_key]
    assert len(replacements)==4 and [p['result'] for p in replacements]==['failed']*3+['successful']
    assert sum(recovery.used_count(json.loads(p.read_text())) for p in path.parent.glob('.cams/*/parts/*/state.json'))==3
    assert not (root/'weeks'/plan['weeks'][0]['id']/'assessment.json').exists()
    report('staged-success',config,client,trace,{'elapsed_hours':8.5,'messages':5732,
            'variables':52,'history_init_completed_during_wait':'2026090800',
            'failed_originals_unchanged':True,'extra_submissions_for_init':3,'manifest_sha256':state.digest(path)})


def test_new_realtime_init_preempts_due_old_recovery_and_history_is_not_starved(system):
    config,clock,client,tmp=system
    assert worker.run_init(config,'20261027',0)['status']=='recovery_deferred'
    before=len(client.posts);clock[0]=dt.datetime(2026,10,28,10,tzinfo=dt.timezone.utc)
    old=worker.run_init(config,'20261027',0)
    assert old['status']=='yielded_to_realtime' and len(client.posts)==before
    new=worker.run_init(config,'20261028',0)
    assert new['status']=='complete'
    assert '2026102700' in worker.scheduled_inits(config,0,clock[0]+dt.timedelta(days=4))
    assert '2026102700' not in worker.pending_inits(config,0,clock[0])
    root=tmp/'weekly';plan=backfill.make_plan('20260908','20260914',authorized_at=clock[0].isoformat())
    state.save(root/'plan.json',plan)
    state.save(root/'recovery_policy.json',make_policy(root,plan,[1800,7200,21600],authorized_at=clock[0].isoformat()))
    history=backfill.tick(config,root,0)
    assert history['status']=='complete' and history['init']=='2026090800'
    report('realtime-priority',config,client,[old,new,history],
           {'newer_realtime_messages':5732,'old_retry_not_submitted_ahead_of_new_init':True,
            'history_progressed_while_old_recovery_waited':True,'old_recovery_remains_scheduled':True})


def test_exhausted_native_recovery_never_submits_a_fifth_order(system):
    config,clock,client,_=system;client.failures=99;trace=[]
    assert worker.run_init(config,'20261027',0)['status']=='recovery_deferred'
    for delay in (1800,7200):
        clock[0]+=dt.timedelta(seconds=delay)
        result=worker.run_init(config,'20261027',0);assert result['status']=='recovery_deferred';trace.append(result)
    clock[0]+=dt.timedelta(seconds=21600)
    with pytest.raises(worker.BlockedRequestError):worker.run_init(config,'20261027',0)
    clock[0]+=dt.timedelta(days=7)
    assert worker.run_init(config,'20261027',0)['status']=='yielded_to_realtime'
    assert len(client.posts)==4
    folder=config.output_dir/'ec_cams/20261027/00z'
    assert json.loads((folder/'.cams/hourly/state.json').read_text())['phase']=='remote_failed'
    assert len(list(folder.glob('.cams/hourly/parts/*/failures/*/failure.json')))==4
    assert not list(folder.glob('*.manifest.json'))
    report('staged-exhaustion',config,client,trace,{'original_plus_replacements':4,
            'failure_archives':4,'manifest_published':False,'no_fifth_order_after_restart_and_time_jump':True})


def test_half_hourly_native_recovery_survives_restarts_for_23p5_hours(system):
    config,clock,client,tmp=system
    config.mars_count_retry_limit=48
    config.mars_count_retry_delays_seconds=[1800]*48
    config.mars_count_retry_window_seconds=86400
    client.failures=47
    first=worker.run_init(config,'20261027',0)
    assert first['status']=='recovery_deferred'
    originals={str(p):state.digest(p) for p in config.output_dir.glob('ec_cams/20261027/00z/.cams/hourly/parts/*/failures/*/*.json')}
    root=tmp/'weekly';plan=backfill.make_plan('20260908','20260914',authorized_at=clock[0].isoformat())
    state.save(root/'plan.json',plan)
    state.save(root/'recovery_policy.json',make_policy(root,plan,[1800]*48,
               authorized_at=clock[0].isoformat(),retry_window_seconds=86400))
    assert backfill.tick(config,root,0)['status']=='complete'
    for attempt in range(1,48):
        clock[0]+=dt.timedelta(minutes=30)
        config=CamsConfig.model_validate(config.model_dump())
        result=worker.run_init(config,'20261027',0)
        assert result['status']==('complete' if attempt==47 else 'recovery_deferred')
    manifest=json.loads(Path(result['manifest']).read_text())
    assert len(manifest['variables'])==52 and sum(f['records_selected'] for f in manifest['files'])==5732
    assert all(state.digest(Path(p))==checksum for p,checksum in originals.items())
    assert sum(recovery.used_count(json.loads(p.read_text())) for p in Path(result['manifest']).parent.glob('.cams/*/parts/*/state.json'))==47
    report('daily-success',config,client,[first,result],{'elapsed_hours':23.5,'messages':5732,
           'variables':52,'extra_submissions_for_init':47,'history_completed_during_wait':True,
           'original_failures_unchanged':True})


def test_daily_native_expiry_stops_old_post_but_new_realtime_continues(system):
    config,clock,client,_=system
    config.mars_count_retry_limit=48
    config.mars_count_retry_delays_seconds=[1800]*48
    config.mars_count_retry_window_seconds=86400
    client.failures=99
    assert worker.run_init(config,'20261027',0)['status']=='recovery_deferred'
    clock[0]+=dt.timedelta(minutes=30)
    assert worker.run_init(config,'20261027',0)['status']=='recovery_deferred'
    clock[0]+=dt.timedelta(hours=23,minutes=30,seconds=1)
    assert worker.run_init(config,'20261027',0)['status']=='yielded_to_realtime'
    newer=worker.run_init(config,'20261028',0)
    assert newer['status']=='complete'
    count=len(client.posts)
    with pytest.raises(worker.BlockedRequestError):worker.run_init(config,'20261027',0)
    assert len(client.posts)==count
    old_posts=[p for p in client.posts if p['date']=='2026-10-27']
    assert len(old_posts)==2
    report('daily-expiry',config,client,[newer],{'expired_old_init_additional_posts':0,
           'new_realtime_completed':True,'old_orders_retained':2,'manifest_for_old_init':False})
