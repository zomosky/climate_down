"""Unknown submission is one retained gap, not a permanent realtime priority."""
import datetime as dt
import json
from types import SimpleNamespace

import pytest

from climate_download.cams import worker
from climate_download.cams.catalogue import DATASET, request_for
from climate_download.cams.state import save, digest, signature
from test_cams_scheduler_recovery import config, args
from test_cams_backfill import setup


def unknown(config, key='2026102400', *, partition=False):
    f=config.output_dir/'ec_cams'/key[:8]/(key[8:]+'z')/'.cams/hourly'
    s={'date':key[:8],'cycle':int(key[8:]),'group':'hourly','account':config.account_for(int(key[8:])).name,
       'dataset':DATASET,'request_sha256':signature(request_for(key[:8],int(key[8:]),'hourly')),
       'phase':'submission_uncertain','submission_started_at':'2026-10-24T14:00:00+00:00'}
    if partition:
        save(f/'state.json',{**s,'phase':'partitioned','partitioned_request':True})
        f=f/'parts/unknown-piece'
    save(f/'state.json',s)
    return f


@pytest.mark.parametrize('partition',[False,True])
def test_opt_in_quarantine_removes_only_uncertain_live_priority(config,partition):
    config=config.model_copy(update={'quarantine_uncertain_submissions':True})
    f=unknown(config,partition=partition);before=(f/'state.json').read_bytes()
    at=dt.datetime(2026,10,26,1,tzinfo=dt.UTC)
    assert worker.pending_inits(config,0,at)==['2026102500']
    assert (f/'state.json').read_bytes()==before
    assert '2026102400' in worker.scheduled_inits(config,0,at)


def test_default_still_protects_uncertain_priority(config):
    unknown(config)
    assert worker.pending_inits(config,0,dt.datetime(2026,10,26,1,tzinfo=dt.UTC))==['2026102400','2026102500']


def test_quarantined_saved_init_returns_without_network_or_state_mutation(config):
    config=config.model_copy(update={'quarantine_uncertain_submissions':True})
    f=unknown(config);before=(f/'state.json').read_bytes()
    result=worker.run_init(config,'20261024',0,client=object())
    assert result['status']=='quarantined' and result['reason']=='submission_uncertain'
    assert (f/'state.json').read_bytes()==before


def test_scheduled_quarantine_is_reported_but_does_not_starve_the_next_init(config,monkeypatch,capsys):
    from climate_download import cli
    from climate_download.cams import config as cm
    monkeypatch.setattr(cm,'load_config',lambda p:config)
    monkeypatch.setattr(worker,'scheduled_inits',lambda *a:['2026102300','2026102400'])
    calls=[]
    def run(cfg,date,cycle):
        calls.append(date)
        return {'status':'quarantined' if date=='20261023' else 'pending','date':date,'cycle':cycle}
    monkeypatch.setattr(worker,'run_init',run)
    assert cli.cmd_cams(args())==0
    assert calls==['20261023','20261024']
    assert [json.loads(x)['status'] for x in capsys.readouterr().out.splitlines()]==['quarantined','pending']


@pytest.fixture
def case(config,monkeypatch):
    from climate_download.cams import submission, recovery
    cfg=config.model_copy(update={'quarantine_uncertain_submissions':True,
        'mars_count_retry_limit':48,'mars_count_retry_delays_seconds':[1800]*48,
        'mars_count_retry_window_seconds':86400})
    clock=[dt.datetime(2026,10,24,15,tzinfo=dt.UTC)]
    for module in (submission,recovery,worker):monkeypatch.setattr(module,'now',lambda:clock[0].isoformat())
    folder=cfg.output_dir/'ec_cams/20261024/00z/.cams/gust';request=request_for('20261024',0,'gust')
    saved={'phase':'submission_uncertain','date':'20261024','cycle':0,'group':'gust','account':'a',
        'dataset':DATASET,'request_sha256':signature(request),'created_at':'2026-10-24T13:58:00+00:00',
        'submission_started_at':'2026-10-24T14:00:00+00:00','error':{'type':'TimeoutError','message':'lost reply'},
        'mars_retry_policy':recovery.policy_for(cfg)}
    save(folder/'request.json',request);save(folder/'state.json',saved)
    auth={'schema_version':1,'state_sha256':digest(folder/'state.json'),'request_sha256':signature(request),
        'max_extra_submissions':1,'authorized_at':clock[0].isoformat(),'expires_at':'2026-10-25T14:00:00+00:00',
        'reason':'Explicit test authorization for one identical extra submission'}
    class Client:
        def __init__(self):self.rows=[];self.remotes={};self.posts=[];self.lose_reply=False
        def get_jobs(self,**kwargs):
            rows=self.rows
            if kwargs.get('status'):rows=[x for x in rows if x['status'] in kwargs['status']]
            return SimpleNamespace(json={'jobs':rows},next=None)
        def get_remote(self,rid):return SimpleNamespace(json=self.remotes[rid])
        def submit(self,dataset,body):
            self.posts.append({'dataset':dataset,'body':json.loads(json.dumps(body))})
            if self.lose_reply:raise TimeoutError('server accepted but its reply was lost')
            return SimpleNamespace(request_id='one-new-order')
    client=Client()
    monkeypatch.setattr(submission,'preflight',lambda *a:{'checked_at':clock[0].isoformat(),'cost':41})
    return cfg,clock,folder,request,auth,client


def candidate(client,request,rid='original',status='successful',created='2026-10-24T14:00:00'):
    row={'jobID':rid,'status':status,'created':created,'processID':DATASET}
    client.rows.append(row)
    client.remotes[rid]={'jobID':rid,'status':status,'created':created,'processID':DATASET,
                         'metadata':{'request':{'ids':request}}}


@pytest.mark.parametrize('status',['accepted','running','successful','failed'])
def test_find_original_reattaches_without_any_new_post(case,status):
    from climate_download.cams.submission import recover_submission
    cfg,_,folder,request,_,client=case;original=(folder/'state.json').read_bytes()
    candidate(client,request,status=status)
    result=recover_submission(cfg,'20261024',0,'gust',client=client)
    assert result['status']=='original_recovered' and result['request_id']=='original' and not client.posts
    assert json.loads((folder/'state.json').read_text())['phase']=='submitted'
    assert next((folder/'uncertain').glob('*/state.json')).read_bytes()==original


def test_absence_alone_never_grants_a_replacement(case):
    from climate_download.cams.submission import recover_submission
    cfg,_,folder,_,_,client=case;original=(folder/'state.json').read_bytes()
    result=recover_submission(cfg,'20261024',0,'gust',client=client)
    assert result['status']=='not_found' and result['lookup']['absence_is_not_proof'] and not client.posts
    assert (folder/'state.json').read_bytes()==original


def test_one_authorized_identical_post_and_repeated_execution_are_bounded(case):
    from climate_download.cams import submission,recovery
    cfg,_,folder,request,auth,client=case;original=(folder/'state.json').read_bytes();original_request=(folder/'request.json').read_bytes()
    first=submission.recover_submission(cfg,'20261024',0,'gust',authorization=auth,client=client)
    after_first=(folder/'state.json').read_bytes()
    second=submission.recover_submission(cfg,'20261024',0,'gust',authorization=auth,client=client)
    assert first['status']=='replacement_submitted' and second['status']=='already_resumed'
    assert client.posts==[{'dataset':DATASET,'body':request}]
    assert next((folder/'uncertain').glob('*/state.json')).read_bytes()==original
    assert (folder/'request.json').read_bytes()==original_request
    assert (folder/'state.json').read_bytes()==after_first
    saved=json.loads((folder/'state.json').read_text())
    assert recovery.init_used_count(cfg,folder,saved)==1
    assert recovery._first_failure_at(cfg,folder,saved)==dt.datetime(2026,10,24,14,tzinfo=dt.UTC)


def test_a_second_lost_reply_remains_spent_and_does_not_post_again(case):
    from climate_download.cams.submission import recover_submission
    cfg,_,folder,_,auth,client=case;client.lose_reply=True
    with pytest.raises(worker.BlockedRequestError):
        recover_submission(cfg,'20261024',0,'gust',authorization=auth,client=client)
    result=recover_submission(cfg,'20261024',0,'gust',authorization=auth,client=client)
    assert result['status']=='replacement_spent' and len(client.posts)==1
    assert json.loads((folder/'state.json').read_text())['phase']=='submission_uncertain'


def test_reserved_slot_survives_crash_before_state_is_written(case,monkeypatch):
    from climate_download.cams import submission,recovery
    cfg,_,folder,_,auth,client=case;original=(folder/'state.json').read_bytes();real_save=submission.save
    def crash(path,value):
        if path==folder/'state.json':raise SystemExit('crash after durable reservation')
        real_save(path,value)
    monkeypatch.setattr(submission,'save',crash)
    with pytest.raises(SystemExit):submission.recover_submission(cfg,'20261024',0,'gust',authorization=auth,client=client)
    assert (folder/'state.json').read_bytes()==original and not client.posts
    assert recovery.init_used_count(cfg,folder,json.loads(original))==1
    assert submission.recover_submission(cfg,'20261024',0,'gust',authorization=auth,client=client)['status']=='replacement_spent'


@pytest.mark.parametrize('damage',['state_hash','request_hash','count','naive','expired','raw','account','shared_cap'])
def test_unsafe_replacement_does_not_consume_a_post(case,damage):
    from climate_download.cams import submission,recovery
    cfg,clock,folder,_,auth,client=case
    if damage=='state_hash':auth['state_sha256']='bad'
    elif damage=='request_hash':auth['request_sha256']='bad'
    elif damage=='count':auth['max_extra_submissions']=2
    elif damage=='naive':auth['authorized_at']='2026-10-24T15:00:00'
    elif damage=='expired':auth['expires_at']='2026-10-24T14:30:00+00:00'
    elif damage=='raw':(folder/'download.grib.part').write_bytes(b'kept-original-bytes')
    elif damage=='account':
        saved=json.loads((folder/'state.json').read_text());saved['account']='another';save(folder/'state.json',saved)
    elif damage=='shared_cap':save(folder.parent/'hourly/state.json',{'retry_count':48})
    with pytest.raises((recovery.RecoveryBlocked,ValueError)):
        submission.recover_submission(cfg,'20261024',0,'gust',authorization=auth,client=client)
    assert not client.posts


@pytest.mark.parametrize('failure',['multiple','metadata','unsorted','page_limit','missing_request','missing_dataset'])
def test_unverifiable_order_lookup_is_not_treated_as_absence(case,failure):
    from climate_download.cams import submission,recovery
    cfg,_,_,request,auth,client=case
    if failure=='multiple':
        candidate(client,request,'one');candidate(client,request,'two')
    elif failure=='metadata':client.get_jobs=lambda **k:(_ for _ in ()).throw(TimeoutError('metadata'))
    elif failure=='unsorted':
        candidate(client,request,'one',created='2026-10-24T14:00:00');candidate(client,request,'two',created='2026-10-24T14:01:00')
    elif failure=='page_limit':
        class Pages:
            def __init__(self,n):self.n=n;self.json={'jobs':[{'jobID':str(n),'created':'2026-10-24T14:00:00','processID':'another'}]}
            @property
            def next(self):return Pages(self.n+1)
        client.get_jobs=lambda **k:Pages(0)
    elif failure=='missing_request':
        candidate(client,request);client.remotes['original']['metadata']={}
    else:
        candidate(client,request);client.rows[0].pop('processID')
    with pytest.raises((recovery.RecoveryBlocked,TimeoutError)):
        submission.recover_submission(cfg,'20261024',0,'gust',authorization=auth,client=client)
    assert not client.posts


def test_original_appearing_during_preflight_is_adopted_instead_of_reposted(case,monkeypatch):
    from climate_download.cams import submission
    cfg,_,_,request,auth,client=case
    def preflight(*a):candidate(client,request);return {'cost':41}
    monkeypatch.setattr(submission,'preflight',preflight)
    assert submission.recover_submission(cfg,'20261024',0,'gust',authorization=auth,client=client)['status']=='original_recovered'
    assert not client.posts


def test_expiry_during_preflight_prevents_the_final_post(case,monkeypatch):
    from climate_download.cams import submission,recovery
    cfg,clock,_,_,auth,client=case
    save(cfg.output_dir/'ec_cams/20261025/00z/20261025_00z_ec_cams.manifest.json',{})
    def preflight(*a):clock[0]+=dt.timedelta(days=1);return {'cost':41}
    monkeypatch.setattr(submission,'preflight',preflight)
    with pytest.raises(recovery.RecoveryBlocked,match='window'):
        submission.recover_submission(cfg,'20261024',0,'gust',authorization=auth,client=client)
    assert not client.posts


@pytest.mark.parametrize('found',[False,True])
def test_partition_parent_reopens_and_preserves_the_original_plan_and_records(case,found):
    from climate_download.cams import submission,partition
    from test_cams_partition import constraint_rows
    cfg,_,old_folder,_,_,client=case
    original=json.loads((old_folder/'state.json').read_text())
    parent=old_folder.parent/'hourly';plan=partition.make_plan('20261024',0,constraint_rows(0));piece=plan['parts'][0]
    folder=parent/'parts'/piece['id'];request=piece['request']
    saved={**original,'group':'hourly','request_sha256':signature(request)}
    save(folder/'state.json',saved);save(folder/'request.json',request)
    save(parent/'request.json',request_for('20261024',0,'hourly'));save(parent/'partition.json',plan)
    pstate={**saved,'request_sha256':signature(request_for('20261024',0,'hourly')),
            'partitioned_request':True,'partition_sha256':digest(parent/'partition.json'),'failed_part':piece['id']}
    save(parent/'state.json',pstate)
    original_parent=(parent/'state.json').read_bytes();original_child=(folder/'state.json').read_bytes()
    plan_hash=digest(parent/'partition.json')
    auth={'schema_version':1,'state_sha256':digest(folder/'state.json'),'request_sha256':signature(request),
          'max_extra_submissions':1,'authorized_at':'2026-10-24T15:00:00+00:00',
          'expires_at':'2026-10-25T14:00:00+00:00','reason':'Explicit same-part replacement'}
    if found:candidate(client,request)
    result=submission.recover_submission(cfg,'20261024',0,'hourly',part=piece['id'],authorization=auth,client=client)
    assert result['orders_submitted']==(0 if found else 1)
    assert json.loads((parent/'state.json').read_text())['phase']=='partitioned'
    assert digest(parent/'partition.json')==plan_hash
    archive=next((folder/'uncertain').iterdir())
    assert (archive/'state.json').read_bytes()==original_child
    assert (archive/'parent-state.json').read_bytes()==original_parent
    # Reproduce a crash after the leaf is committed but before its parent is reopened.
    (parent/'state.json').write_bytes(original_parent)
    assert submission.recover_submission(cfg,'20261024',0,'hourly',part=piece['id'],client=client)['status']=='already_resumed'
    assert json.loads((parent/'state.json').read_text())['phase']=='partitioned'
    assert len(client.posts)==(0 if found else 1)
    completed=json.loads((parent/'state.json').read_text());completed['phase']='complete';save(parent/'state.json',completed)
    before=(parent/'state.json').read_bytes()
    submission.recover_submission(cfg,'20261024',0,'hourly',part=piece['id'],client=client)
    assert (parent/'state.json').read_bytes()==before


def test_older_verified_failure_anchors_the_same_shared_window(case):
    from climate_download.cams import submission,recovery
    cfg,_,folder,_,auth,client=case
    save(folder.parent/'hourly/failures/known/failure.json',{
        'diagnostic':{'kind':'mars_field_count_mismatch'},'captured_at':'2026-10-24T10:00:00+00:00',
        'local_request_verified':True,'remote_request_verified':True})
    submission.recover_submission(cfg,'20261024',0,'gust',authorization=auth,client=client)
    state=json.loads((folder/'state.json').read_text())
    assert recovery._first_failure_at(cfg,folder,state)==dt.datetime(2026,10,24,10,tzinfo=dt.UTC)
    assert recovery.init_used_count(cfg,folder,state)==1


def test_new_live_work_preempts_an_authorized_old_unknown_replacement(case):
    from climate_download.cams.submission import recover_submission
    cfg,clock,_,_,auth,client=case;clock[0]=dt.datetime(2026,10,25,10,tzinfo=dt.UTC)
    result=recover_submission(cfg,'20261024',0,'gust',authorization=auth,client=client)
    assert result['status']=='waiting_for_account_or_realtime' and not client.posts


def test_a_busy_account_prevents_an_authorized_replacement(case):
    from climate_download.cams.submission import recover_submission
    cfg,_,_,request,auth,client=case
    candidate(client,{**request,'date':'2026-10-23'},'other-order',status='running')
    assert recover_submission(cfg,'20261024',0,'gust',authorization=auth,client=client)['status']=='waiting_for_account_or_realtime'
    assert not client.posts


def test_reactivated_successful_predecessor_is_reused_not_duplicated(case):
    from climate_download.cams.submission import recover_submission
    cfg,_,folder,request,_,client=case;s=json.loads((folder/'state.json').read_text())
    s['previous_attempts']=[{'request_id':'old-failed-now-successful'}];save(folder/'state.json',s)
    candidate(client,request,'old-failed-now-successful',created='2026-10-24T10:00:00')
    assert recover_submission(cfg,'20261024',0,'gust',client=client)['request_id']=='old-failed-now-successful'
    assert not client.posts


def test_recovery_reexecution_never_downgrades_a_completed_raw_group(case):
    from climate_download.cams.submission import recover_submission
    cfg,_,folder,_,auth,client=case
    recover_submission(cfg,'20261024',0,'gust',authorization=auth,client=client)
    saved=json.loads((folder/'state.json').read_text());saved['phase']='complete';save(folder/'state.json',saved)
    before=(folder/'state.json').read_bytes()
    assert recover_submission(cfg,'20261024',0,'gust',authorization=auth,client=client)['status']=='already_resumed'
    assert (folder/'state.json').read_bytes()==before and len(client.posts)==1


@pytest.mark.parametrize('account_busy',[False,True])
def test_quarantined_live_gap_yields_to_history_with_an_account_guard(system,account_busy):
    from climate_download.cams import backfill
    from climate_download.cams.queue_policy import make_policy
    config,clock,client,tmp=system;client.failures=0
    config.quarantine_uncertain_submissions=True
    config.mars_count_retry_limit=48;config.mars_count_retry_delays_seconds=[1800]*48
    config.mars_count_retry_window_seconds=86400
    unknown(config,'2026102700')
    root=tmp/'week';plan=backfill.make_plan('20260908','20260914',authorized_at=clock[0].isoformat())
    save(root/'plan.json',plan)
    save(root/'recovery_policy.json',make_policy(root,plan,[1800]*48,authorized_at=clock[0].isoformat(),
        retry_window_seconds=86400,continue_other_weeks_when_blocked=True))
    client.get_jobs=lambda **k:SimpleNamespace(json={'jobs':[{'jobID':'unknown-original','status':'running'}] if account_busy else []})
    result=backfill.tick(config,root,0)
    assert result['status']==('waiting_for_account' if account_busy else 'complete')
    assert result.get('init')=='2026090800'
    assert len(client.posts)==(0 if account_busy else 3)


def test_quarantine_does_not_remove_new_realtime_admission(system):
    cfg,clock,client,_=system;client.failures=0;cfg.quarantine_uncertain_submissions=True
    unknown(cfg,'2026102600')
    client.get_jobs=lambda **k:pytest.fail('New realtime must retain its original admission opportunity')
    assert worker.run_init(cfg,'20261027',0)['status']=='complete'


@pytest.mark.parametrize('phase',['submitting','submission_uncertain'])
def test_history_skips_a_child_only_unknown_after_partition_parent_crash(setup,monkeypatch,phase):
    from climate_download.cams import backfill
    from test_cams_staged_queue import prepare
    from test_cams_uncertain_queue import enable
    cfg,plan,root=prepare(setup,monkeypatch);enable(cfg,root,plan)
    cfg.quarantine_uncertain_submissions=True
    child=unknown(cfg,'2026091500',partition=True)
    saved=json.loads((child/'state.json').read_text());saved['phase']=phase
    save(child/'state.json',saved)
    paths=[child/'state.json',child.parents[1]/'state.json']
    before={str(p):p.read_bytes() for p in paths}
    calls=[]
    def run(config,date,cycle,**kwargs):
        calls.append((date,cycle,kwargs))
        return {'status':'pending','date':date,'cycle':cycle}
    monkeypatch.setattr(backfill,'run_init',run)
    result=backfill.tick(cfg,root,0)
    assert calls==[('20260916',0,{'require_idle_account':True})]
    assert result['init']=='2026091600' and result['quarantined'][0]['init']=='2026091500'
    assert before=={str(p):p.read_bytes() for p in paths}
    assert not (root/'weeks'/plan['weeks'][0]['id']/'assessment.json').exists()


def test_a_new_timeout_is_quarantined_in_the_same_scheduled_tick(config,monkeypatch,capsys):
    from climate_download import cli
    from climate_download.cams import config as cm
    config.quarantine_uncertain_submissions=True
    monkeypatch.setattr(cm,'load_config',lambda p:config)
    monkeypatch.setattr(worker,'scheduled_inits',lambda *a:['2026102300','2026102400'])
    calls=[]
    def run(cfg,date,cycle):
        calls.append(date)
        if date=='20261023':
            unknown(cfg,date+'00');raise worker.BlockedRequestError('POST reply timed out')
        return {'status':'pending'}
    monkeypatch.setattr(worker,'run_init',run)
    assert cli.cmd_cams(args())==0 and calls==['20261023','20261024']
    assert [json.loads(x)['status'] for x in capsys.readouterr().out.splitlines()]==['quarantined','pending']


def test_an_explicit_quarantined_cli_request_is_not_reported_as_success(config,monkeypatch,capsys):
    from climate_download import cli
    from climate_download.cams import config as cm
    monkeypatch.setattr(cm,'load_config',lambda p:config)
    monkeypatch.setattr(worker,'run_init',lambda *a:{'status':'quarantined'})
    assert cli.cmd_cams(args(scheduled=False))==1
    assert json.loads(capsys.readouterr().out)['status']=='quarantined'


@pytest.mark.parametrize('original_exists',[True,False])
def test_full_native_submission_recovery_then_history_and_new_realtime(system,monkeypatch,original_exists):
    from climate_download.cams import submission,backfill,grib
    from climate_download.cams.queue_policy import make_policy
    from test_cams_staged_e2e import report
    cfg,clock,client,tmp=system;client.failures=0
    cfg.quarantine_uncertain_submissions=True;cfg.mars_count_retry_limit=48
    cfg.mars_count_retry_delays_seconds=[1800]*48;cfg.mars_count_retry_window_seconds=86400
    monkeypatch.setattr(submission,'now',lambda:clock[0].isoformat())
    monkeypatch.setattr(submission,'preflight',worker.preflight)
    submit=client.submit
    def lost(dataset,request):
        if original_exists:submit(dataset,request)
        raise TimeoutError('original POST response lost')
    client.submit=lost
    with pytest.raises(worker.BlockedRequestError):worker.run_init(cfg,'20261027',0)
    leaf=next(cfg.output_dir.glob('ec_cams/20261027/00z/.cams/hourly/parts/*/state.json')).parent
    original=(leaf/'state.json').read_bytes();request=json.loads((leaf/'request.json').read_text())
    client.submit=submit
    def jobs(**kwargs):
        rows=[{'jobID':p['request_id'],'processID':DATASET,'status':client.remotes[p['request_id']].json['status'],
               'created':p['at']} for p in reversed(client.posts)]
        if kwargs.get('status'):rows=[p for p in rows if p['status'] in kwargs['status']]
        return SimpleNamespace(json={'jobs':rows},next=None)
    client.get_jobs=jobs
    auth={'schema_version':1,'state_sha256':digest(leaf/'state.json'),'request_sha256':signature(request),
        'max_extra_submissions':1,'authorized_at':clock[0].isoformat(),
        'expires_at':(clock[0]+dt.timedelta(days=1)).isoformat(),'reason':'One explicit original native-part replacement'}
    recovery_result=submission.recover_submission(cfg,'20261027',0,'hourly',part=leaf.name,authorization=auth,client=client)
    trace=[recovery_result,worker.run_init(cfg,'20261027',0)]
    assert trace[-1]['status']=='complete'
    assert recovery_result['orders_submitted']==(0 if original_exists else 1)
    assert len([p for p in client.posts if p['request_sha256']==signature(request)])==1
    assert next((leaf/'uncertain').glob('*/state.json')).read_bytes()==original
    root=tmp/'weekly';plan=backfill.make_plan('20260908','20260914',authorized_at=clock[0].isoformat())
    save(root/'plan.json',plan);save(root/'recovery_policy.json',make_policy(root,plan,[1800]*48,
        authorized_at=clock[0].isoformat(),retry_window_seconds=86400,continue_other_weeks_when_blocked=True))
    trace.append(backfill.tick(cfg,root,0));assert trace[-1]['status']=='complete'
    clock[0]+=dt.timedelta(days=1)
    trace.append(backfill.tick(cfg,root,0));assert trace[-1]['status']=='waiting_for_realtime'
    trace.append(worker.run_init(cfg,'20261028',0));assert trace[-1]['status']=='complete'
    for key in ('2026102700','2026090800','2026102800'):
        folder=cfg.output_dir/'ec_cams'/key[:8]/(key[8:]+'z')
        assert grib.verify_published(folder,key[:8],0)
        manifest=json.loads((folder/f'{key[:8]}_00z_ec_cams.manifest.json').read_text())
        assert len(manifest['files'])==121 and len(manifest['variables'])==52
        assert sum(x['records_selected'] for x in manifest['files'])==5732
    report('submission-original-resumed' if original_exists else 'submission-once-resumed',cfg,client,trace,
           {'original_uncertain_record_preserved':True,'same_request_posts':1,
            'extra_submissions':recovery_result['orders_submitted'],'full_realtime_and_history_manifests':3})


# Reuse the offline ADS/clock/GRIB fixture for the cross-scheduler regressions above.
from test_cams_staged_e2e import system
