"""Disjoint native ADS cubes preserve the complete CAMS request contract."""
import copy
import datetime as dt
import json
from types import SimpleNamespace

import pytest

from climate_download.cams.catalogue import DATASET, GROUPS, request_for
from climate_download.cams.client import NotAvailableYet
from climate_download.cams.config import CamsConfig
from climate_download.cams.state import save, signature


def constraint_rows(cycle):
    first00={210207,210209,210208,210210,210211,210212,210073,210074,165,166,167,168,235,151,137,164,186,187,188,141}
    first12=first00|{210213,210214,210215,210216,134}
    cohort_sets=[first00] if cycle==0 else [first12,{215140,215158,215104,215122},{210250,210251}]
    cohort_sets.append({v.param_id for v in GROUPS['hourly']}-set.union(*cohort_sets))
    rows=[]
    for i,params in enumerate(cohort_sets):
        interval=3 if i==len(cohort_sets)-1 else 6
        for take_multiples in (True,False):
            rows.append({'date':['2026-01-01/2026-10-31'],'time':[f'{cycle:02d}:00'],'type':['forecast'],
                         'variable':[v.api_name for v in GROUPS['hourly'] if v.param_id in params],
                         'leadtime_hour':[str(h) for h in range(121) if (h%interval==0)==take_multiples]})
    return rows


@pytest.mark.parametrize('cycle,pieces',[(0,4),(12,8)])
def test_native_cubes_are_disjoint_and_exactly_cover_all_5445_messages(cycle,pieces):
    from climate_download.cams.partition import make_plan, validate_plan
    plan=make_plan('20261027',cycle,constraint_rows(cycle))
    validate_plan(plan,'20261027',cycle)
    assert len(plan['parts'])==pieces
    expected={(v.api_name,h) for v in GROUPS['hourly'] for h in v.leads}
    seen=set()
    for part in plan['parts']:
        request=part['request']
        identities={(v,int(h)) for v in request['variable'] for h in request['leadtime_hour']}
        assert not seen&identities
        seen|=identities
        assert part['id']==signature(request)[:16]
        assert request['area']==[55,70,15,140]
        assert request['time']==[f'{cycle:02d}:00'] and request['date']=='2026-10-27'
        assert set(request)==set(request_for('20261027',cycle,'hourly'))
    assert seen==expected and len(seen)==5445


def test_plan_is_stable_when_constraint_rows_and_values_are_reordered():
    from climate_download.cams.partition import make_plan
    rows=constraint_rows(0)
    shuffled=[]
    for row in reversed(rows):
        shuffled.append({key:list(reversed(value)) for key,value in row.items()})
    first,second=make_plan('20261027',0,rows),make_plan('20261027',0,shuffled)
    assert first['parts']==second['parts']


def test_advertised_gap_never_turns_into_partial_request_plan():
    from climate_download.cams.partition import make_plan
    rows=constraint_rows(0)
    rows[0]['leadtime_hour'].remove('120')
    with pytest.raises(NotAvailableYet,match='native'):
        make_plan('20261027',0,rows)


@pytest.mark.parametrize('problem',['missing','duplicate','area','variable','date','lead','identity'])
def test_modified_saved_plan_is_rejected(problem):
    from climate_download.cams.partition import make_plan, validate_plan
    plan=make_plan('20261027',0,constraint_rows(0))
    if problem=='missing':plan['parts'].pop()
    elif problem=='duplicate':plan['parts'].append(copy.deepcopy(plan['parts'][0]))
    elif problem=='area':plan['parts'][0]['request']['area'][0]=56
    elif problem=='variable':plan['parts'][0]['request']['variable'].append('foreign_variable')
    elif problem=='date':plan['parts'][0]['request']['date']='2026-10-26'
    elif problem=='lead':plan['parts'][0]['request']['leadtime_hour'].append('121')
    else:plan['parts'][0]['id']='different'
    with pytest.raises(ValueError):validate_plan(plan,'20261027',0)


def test_grib_subset_rejects_missing_extra_and_duplicate_native_identities(tmp_path,monkeypatch):
    from climate_download.cams import grib
    from test_cams import header
    variable=next(v for v in GROUPS['hourly'] if v.param_id==167)
    messages=[(header(variable,h),b'GRIBbytes7777') for h in (0,6,12)]
    path=tmp_path/'piece.grib'
    monkeypatch.setattr(grib,'iter_messages',lambda p:iter(messages))
    def write():path.write_bytes(b''.join(message for _,message in messages))
    write()
    assert grib.validate([path],'20261021',0,(variable,),native_leads=(0,6,12))['messages']==3
    for bad in ([messages[0],messages[1]],[*messages,messages[0]],
                [*messages,(header(variable,1),b'GRIBbytes7777')]):
        original=messages[:];messages[:]=bad;write()
        with pytest.raises(ValueError):grib.validate([path],'20261021',0,(variable,),native_leads=(0,6,12))
        messages[:]=original


def test_subset_request_preflight_cost_is_for_exact_piece_not_whole_group():
    from climate_download.cams.client import preflight
    request=request_for('20261027',0,'hourly')
    request.update(variable=['2m_temperature'],leadtime_hour=['0','6','12'])
    class Client:
        _profile_api=SimpleNamespace(accepted_licences=lambda:{'licences':[{'id':'cc-by','revision':1}]})
        def get_jobs(self,**kw):return None
        def get_collection(self,dataset):return SimpleNamespace(constraints=constraint_rows(0))
        def estimate_costs(self,dataset,payload):
            assert payload==request
            return {'cost':3,'limit':10000}
    assert preflight(Client(),request,'hourly')['cost']==3


def test_partition_intent_without_first_id_remains_scheduled_after_recent_window(tmp_path):
    from climate_download.cams.worker import scheduled_inits,pending_inits
    config=CamsConfig(output_dir=tmp_path,first_init='2026102100',accounts=[{'name':'a','credential_file':tmp_path/'key'}],cycle_accounts={0:'a',12:'a'})
    save(tmp_path/'ec_cams/20261027/00z/.cams/hourly/state.json',
         {'date':'20261027','cycle':0,'phase':'partitioned','partitioned_request':True})
    at=dt.datetime(2026,10,31,1,tzinfo=dt.timezone.utc)
    assert '2026102700' in scheduled_inits(config,0,at)
    assert '2026102700' in pending_inits(config,0,at)


@pytest.fixture
def partition_setup(tmp_path,monkeypatch):
    from climate_download.cams import worker
    config=CamsConfig(output_dir=tmp_path,first_init='2026102100',max_run_seconds=0,
        hourly_request_mode='native_constraint_cubes',mars_count_retry_limit=1,
        accounts=[{'name':'a','credential_file':tmp_path/'key'}],cycle_accounts={0:'a',12:'a'})
    monkeypatch.setattr(worker,'preflight',lambda *a:{'passed':True})
    monkeypatch.setattr(worker,'newer_live_due',lambda *a:None)
    class Remote:
        def __init__(self,rid,request):
            self.request_id=rid
            self.json={'status':'accepted','metadata':{'request':{'ids':copy.deepcopy(request)}}}
        def get_results(self):
            raise RuntimeError('MarsRuntimeError mars - ERROR - Last error is -43 Expected 420, got 574.')
    class Client:
        def __init__(self):self.posts=[];self.remotes={};self.crash=False;self.defer=False
        def get_collection(self,*a):
            if self.defer:raise TimeoutError('metadata unavailable')
            return SimpleNamespace(constraints=constraint_rows(0))
        def submit(self,dataset,request):
            self.posts.append(copy.deepcopy(request))
            if self.crash:raise TimeoutError('token-value https://signed.invalid/?token=token-value')
            remote=Remote('piece-'+str(len(self.posts)),request)
            self.remotes[remote.request_id]=remote
            return remote
        def get_remote(self,rid):return self.remotes[rid]
    return config,tmp_path/'hourly',Client()


def test_partition_reuses_original_piece_id_and_does_not_enqueue_the_entire_plan(partition_setup):
    from climate_download.cams.partition import tick
    config,folder,client=partition_setup
    for _ in range(3):assert tick(config,folder,'20261027',0,client,'') is False
    assert len(client.posts)==1
    state=json.loads((folder/'state.json').read_text())
    assert state['parts_complete']==0 and state['parts_total']==4
    assert state['request_ids']==['piece-1']
    assert not (folder/'download.grib').exists()


def test_adopted_later_piece_retains_its_earlier_transfer_time_and_bytes(partition_setup):
    from climate_download.cams.partition import tick
    config,folder,client=partition_setup
    assert tick(config,folder,'20261027',0,client,'') is False
    plan=json.loads((folder/'partition.json').read_text());part=plan['parts'][1]
    save(folder/'parts'/part['id']/'state.json',{'phase':'downloading','request_id':'adopted-order',
         'partial_bytes':1234,'expected_bytes':5678,'first_download_started_at':'2026-10-27T02:00:00+00:00'})
    assert tick(config,folder,'20261027',0,client,'') is False
    state=json.loads((folder/'state.json').read_text())
    assert state['first_download_started_at']=='2026-10-27T02:00:00+00:00'
    assert state['partial_bytes']==1234 and state['expected_bytes']==5678
    assert state['request_ids']==['piece-1','adopted-order'] and len(client.posts)==1


def test_piece_count_failure_is_archived_and_never_identically_retried(partition_setup):
    from climate_download.cams.partition import tick
    from climate_download.cams.worker import BlockedRequestError
    config,folder,client=partition_setup
    assert tick(config,folder,'20261027',0,client,'') is False
    client.remotes['piece-1'].json['status']='failed'
    with pytest.raises(BlockedRequestError):tick(config,folder,'20261027',0,client,'')
    with pytest.raises(BlockedRequestError):tick(config,folder,'20261027',0,client,'')
    assert len(client.posts)==1
    failures=list(folder.glob('parts/*/failures/*/failure.json'))
    assert len(failures)==1 and 'Expected 420, got 574' in failures[0].read_text()
    assert json.loads((folder/'state.json').read_text())['phase']=='remote_failed'


def test_uncertain_piece_submission_cannot_be_recreated(partition_setup):
    from climate_download.cams.partition import tick
    from climate_download.cams.worker import BlockedRequestError
    config,folder,client=partition_setup;client.crash=True
    for _ in range(2):
        with pytest.raises(BlockedRequestError):tick(config,folder,'20261027',0,client,'token-value')
    assert len(client.posts)==1
    assert json.loads((folder/'state.json').read_text())['phase']=='submission_uncertain'
    assert 'token-value' not in (folder/'state.json').read_text()


def test_transient_plan_metadata_failure_can_resume_without_an_orphan_plan(partition_setup):
    from climate_download.cams.partition import tick
    config,folder,client=partition_setup;client.defer=True
    assert tick(config,folder,'20261027',0,client,'') is False
    assert not client.posts and not (folder/'partition.json').exists()
    client.defer=False
    assert tick(config,folder,'20261027',0,client,'') is False
    assert len(client.posts)==1


def test_existing_failed_or_partial_bundle_cannot_be_silently_adopted(partition_setup):
    from climate_download.cams.partition import tick
    from climate_download.cams.worker import BlockedRequestError
    config,folder,client=partition_setup
    save(folder/'state.json',{'date':'20261027','cycle':0,'group':'hourly','account':'a','dataset':DATASET,
        'request_sha256':signature(request_for('20261027',0,'hourly')),'request_id':'original-failure','phase':'remote_failed'})
    before=(folder/'state.json').read_bytes()
    with pytest.raises(BlockedRequestError):tick(config,folder,'20261027',0,client,'')
    assert (folder/'state.json').read_bytes()==before and not client.posts


def test_plan_content_drift_stops_before_another_submit(partition_setup):
    from climate_download.cams.partition import tick
    config,folder,client=partition_setup
    assert tick(config,folder,'20261027',0,client,'') is False
    plan=json.loads((folder/'partition.json').read_text());plan['parts'][0]['constraint_signature']='a'*64
    save(folder/'partition.json',plan)
    with pytest.raises(ValueError,match='checksum'):tick(config,folder,'20261027',0,client,'')
    assert len(client.posts)==1


@pytest.mark.parametrize('missing', [False, True])
def test_completed_partition_still_requires_its_original_pinned_plan(partition_setup, missing):
    from climate_download.cams.partition import tick
    from climate_download.cams.state import digest
    config,folder,client=partition_setup
    assert tick(config,folder,'20261027',0,client,'') is False
    raw=folder/'download.grib';raw.write_bytes(b'original completed group')
    state=json.loads((folder/'state.json').read_text())
    state.update(phase='complete',sha256=digest(raw))
    save(folder/'state.json',state)
    plan_path=folder/'partition.json'
    if missing:
        plan_path.unlink()
    else:
        plan=json.loads(plan_path.read_text())
        plan['parts'][0]['constraint_signature']='a'*64
        save(plan_path,plan)
    with pytest.raises(ValueError,match='plan is missing|checksum changed'):
        tick(config,folder,'20261027',0,client,'')
    assert len(client.posts)==1 and raw.read_bytes()==b'original completed group'


@pytest.mark.parametrize('missing',[False,True])
def test_combination_is_byte_exact_and_never_accepts_a_missing_field(partition_setup,monkeypatch,missing):
    from climate_download.cams import grib,worker
    from climate_download.cams.partition import tick
    from test_cams import header
    config,folder,client=partition_setup
    by_name={v.api_name:v for v in GROUPS['hourly']}
    by_param={v.param_id:v for v in GROUPS['hourly']}
    def fake_group(config,child,date,cycle,group,client,secret,*,request):
        messages=[]
        for name in request['variable']:
            for h in map(int,request['leadtime_hour']):
                if missing and by_name[name].param_id==167 and h==120:continue
                messages.append(b'GRIB'+by_name[name].param_id.to_bytes(4,'big')+h.to_bytes(2,'big')+b'7777')
        child.mkdir(parents=True,exist_ok=True);raw=b''.join(messages);(child/'download.grib').write_bytes(raw)
        save(child/'state.json',{'phase':'complete','request_id':child.name,'account':'a','request_sha256':signature(request),
             'bytes':len(raw),'first_download_started_at':'2026-10-28T02:00:00+00:00'})
        return True
    def read_messages(path):
        data=path.read_bytes()
        for i in range(0,len(data),14):
            message=data[i:i+14];p=int.from_bytes(message[4:8],'big');h=int.from_bytes(message[8:10],'big')
            value=header(by_param[p],h);value['dataDate']=20261027
            valid=dt.datetime(2026,10,27)+dt.timedelta(hours=h)
            value['validityDate']=int(valid.strftime('%Y%m%d'));value['validityTime']=int(valid.strftime('%H%M'))
            yield value,message
    monkeypatch.setattr(worker,'group_tick',fake_group);monkeypatch.setattr(grib,'iter_messages',read_messages)
    if missing:
        with pytest.raises(worker.BlockedRequestError):tick(config,folder,'20261027',0,client,'')
        assert not (folder/'download.grib').exists()
        assert json.loads((folder/'state.json').read_text())['phase']=='validation_failed'
    else:
        assert tick(config,folder,'20261027',0,client,'') is True
        plan=json.loads((folder/'partition.json').read_text())
        assert (folder/'download.grib').read_bytes()==b''.join((folder/'parts'/p['id']/'download.grib').read_bytes() for p in plan['parts'])
        assert json.loads((folder/'validation.json').read_text())['messages']==5445


def test_latest_realtime_init_runs_ahead_of_old_recovery(partition_setup,monkeypatch):
    from climate_download import cli
    from climate_download.cams import worker,config as config_module
    config,_,_=partition_setup
    monkeypatch.setattr(config_module,'load_config',lambda path:config)
    monkeypatch.setattr(worker,'scheduled_inits',lambda *a:['2026102700','2026102800'])
    seen=[]
    def run(cfg,date,cycle):seen.append(date);return {'status':'pending','date':date,'cycle':cycle}
    monkeypatch.setattr(worker,'run_init',run)
    args=SimpleNamespace(config='unused',once=True,date=None,cycle=0,scheduled=True,preflight_only=False)
    assert cli.cmd_cams(args)==0 and seen==['20261028']


def test_old_recovery_yields_between_bounded_transfers_without_new_submission(partition_setup,monkeypatch):
    from climate_download.cams import worker
    config,_,client=partition_setup
    monkeypatch.setattr(worker,'newer_live_due',lambda *a:'2026102800')
    result=worker.run_init(config,'20261027',0,client=client)
    assert result['status']=='yielded_to_realtime' and result['priority_init']=='2026102800'
    assert not client.posts


def test_partition_yields_before_submitting_the_next_piece_when_a_new_cycle_is_due(partition_setup,monkeypatch):
    from climate_download.cams import worker
    from climate_download.cams.partition import tick
    config,folder,client=partition_setup;calls=[]
    def complete(cfg,child,*a,request):
        calls.append(child.name);child.mkdir(parents=True,exist_ok=True)
        save(child/'state.json',{'phase':'complete','request_id':'existing','bytes':10})
        return True
    monkeypatch.setattr(worker,'group_tick',complete)
    monkeypatch.setattr(worker,'newer_live_due',lambda *a:'2026102800' if calls else None)
    assert tick(config,folder,'20261027',0,client,'') is False
    state=json.loads((folder/'state.json').read_text())
    assert len(calls)==1 and state['priority_init']=='2026102800' and state['parts_complete']==1
    assert not client.posts


def acceptance_module():
    import importlib.util
    from pathlib import Path
    pytest.importorskip('xarray')
    path=Path(__file__).parents[1]/'deploy/cams_accept_backfill.py'
    spec=importlib.util.spec_from_file_location('cams_partition_acceptance',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('damage',[None,'path','payload','plan'])
def test_acceptance_reads_each_actual_order_once_and_checks_its_pinned_request(tmp_path,damage):
    from climate_download.cams.partition import make_plan
    from climate_download.cams.state import digest
    module=acceptance_module();plan=make_plan('20261027',0,constraint_rows(0))
    folder=tmp_path/'init';group=folder/'.cams/hourly';save(group/'partition.json',plan)
    save(group/'state.json',{'partitioned_request':True,'partition_sha256':digest(group/'partition.json')})
    part=plan['parts'][0];save(group/'parts'/part['id']/'request.json',part['request'])
    request={'group':'hourly','part':part['id'],'request_sha256':signature(part['request'])}
    if damage=='path':request['part']='../foreign-order'
    elif damage=='payload':
        payload=copy.deepcopy(part['request']);payload['area'][0]=56;save(group/'parts'/part['id']/'request.json',payload)
    elif damage=='plan':
        plan['parts'].pop();save(group/'partition.json',plan)
    if damage:
        with pytest.raises((AssertionError,StopIteration)):module.request_directory(folder,request)
    else:
        assert module.request_directory(folder,request)==group/'parts'/part['id']
        assert module.request_directory(folder,{'group':'gust'})==folder/'.cams/gust'


def test_acceptance_combines_native_lead_partitions_before_raw_zarr_comparison():
    import numpy as np
    xr=pytest.importorskip('xarray')
    module=acceptance_module();parts=[]
    for leads in (range(0,121,6),[h for h in range(121) if h%6]):
        leads=list(leads)
        parts.append(xr.DataArray(np.array(leads)[:,None,None],dims=['step','latitude','longitude'],
            coords={'step':np.array(leads,dtype='timedelta64[h]'),'latitude':[39.6],'longitude':[116.]},
            attrs={'GRIB_paramId':167,'units':'K','GRIB_stepType':'instant'}))
    combined,_=module.combine(parts,set(range(121)))
    np.testing.assert_array_equal(combined.values[:,0,0],np.arange(121))
    with pytest.raises(AssertionError):module.combine([parts[0],parts[1].isel(step=slice(1,None))],set(range(121)))
