"""Paused recovery may free a weekly lane, never manufacture an accepted week."""
import datetime as dt
import json

import pytest

from climate_download.cams import backfill
from climate_download.cams.state import digest, save
from test_cams_backfill import setup
from test_cams_backfill_host import evidence, host


def enable(config,root,plan):
    from climate_download.cams.queue_policy import make_policy
    config.mars_count_retry_limit=3
    config.mars_count_retry_delays_seconds=[1800,7200,21600]
    policy=make_policy(root,plan,config.mars_count_retry_delays_seconds,
                       authorized_at='2026-10-28T06:00:00+00:00')
    save(root/'recovery_policy.json',policy)
    return policy


def prepare(setup,monkeypatch):
    config,plan,root=setup
    monkeypatch.setattr(backfill,'pending_inits',lambda *a:[])
    monkeypatch.setattr(backfill.shutil,'disk_usage',lambda p:(0,0,10**13))
    return config,plan,root


def paused(config,key,phase='retry_wait'):
    save(config.output_dir/'ec_cams'/key[:8]/(key[8:]+'z')/'.cams/hourly/state.json',
         {'date':key[:8],'cycle':int(key[8:]),'group':'hourly','phase':phase,'request_id':'old-'+key,
          'failure_diagnostic':{'kind':'mars_field_count_mismatch'},
          'retry_due_at':'2026-10-28T12:00:00+00:00'})


def test_waiting_init_does_not_stop_other_inits_in_the_same_week(setup,monkeypatch):
    config,plan,root=prepare(setup,monkeypatch);enable(config,root,plan)
    calls=[]
    def run(cfg,date,cycle):
        calls.append(date)
        return {'status':'recovery_deferred' if date=='20260915' else 'pending',
                'date':date,'cycle':cycle,'retry_due_at':'later'}
    monkeypatch.setattr(backfill,'run_init',run)
    result=backfill.tick(config,root,0)
    assert calls==['20260915','20260916'] and result['init']=='2026091600'
    assert result['deferred'][0]['init']=='2026091500'


@pytest.mark.parametrize('phase',['retry_wait','remote_failed'])
def test_paused_week_does_not_stop_previous_weeks_and_is_not_accepted(setup,monkeypatch,phase):
    config,plan,root=prepare(setup,monkeypatch);enable(config,root,plan)
    first,second=plan['weeks'][:2]
    for key in first['inits']:
        if key.endswith('00'):paused(config,key,phase)
    def run(cfg,date,cycle):
        return {'status':'recovery_deferred' if date>='20260915' else 'pending',
                'date':date,'cycle':cycle,'retry_due_at':'later'}
    monkeypatch.setattr(backfill,'run_init',run)
    before=digest(root/'plan.json')
    result=backfill.tick(config,root,0)
    assert result['week']==second['id'] and result['init']=='2026090800'
    assert first['id'] in result['deferred_weeks']
    assert not (root/'weeks'/first['id']/'assessment.json').exists()
    assert digest(root/'plan.json')==before


def test_no_runtime_authorization_keeps_the_original_week_barrier(setup,monkeypatch):
    config,plan,root=prepare(setup,monkeypatch)
    config.mars_count_retry_limit=3;config.mars_count_retry_delays_seconds=[1800,7200,21600]
    for key in plan['weeks'][0]['inits']:
        if key.endswith('00'):paused(config,key,'remote_failed')
    monkeypatch.setattr(backfill,'run_init',lambda *a:pytest.fail('legacy plan cannot open a previous week'))
    result=backfill.tick(config,root,0)
    assert result['status']=='needs_inspection' and result['week']==plan['weeks'][0]['id']


def test_a_prior_paused_week_does_not_remove_the_next_normal_week_barrier(setup,monkeypatch):
    config,plan,root=prepare(setup,monkeypatch);enable(config,root,plan)
    for key in plan['weeks'][0]['inits']:
        if key.endswith('00'):paused(config,key)
    for key in plan['weeks'][1]['inits']:
        if key.endswith('00'):
            save(config.output_dir/'ec_cams'/key[:8]/'00z'/f'{key[:8]}_00z_ec_cams.manifest.json',{})
    def run(cfg,date,cycle):
        assert date>='20260915','normal next week is still waiting for its acceptance'
        return {'status':'recovery_deferred','date':date,'cycle':cycle,'retry_due_at':'later'}
    monkeypatch.setattr(backfill,'run_init',run)
    result=backfill.tick(config,root,0)
    assert result['week']==plan['weeks'][1]['id'] and result['status']=='waiting_for_week_acceptance'


@pytest.mark.parametrize('field,value',[('plan_sha256','a'*64),('authorized_at',''),
                                       ('continue_other_weeks_when_deferred',False),
                                       ('max_replacements_per_init',99)])
def test_invalid_recovery_authorization_blocks_before_any_order(setup,monkeypatch,field,value):
    config,plan,root=prepare(setup,monkeypatch);policy=enable(config,root,plan)
    policy[field]=value;save(root/'recovery_policy.json',policy)
    monkeypatch.setattr(backfill,'run_init',lambda *a:pytest.fail('invalid authorization'))
    with pytest.raises(ValueError):backfill.tick(config,root,0)


def test_uncertain_submit_is_not_bypassed_as_an_old_count_failure(setup,monkeypatch):
    config,plan,root=prepare(setup,monkeypatch);enable(config,root,plan)
    for key in plan['weeks'][0]['inits']:
        if key.endswith('00'):paused(config,key,'remote_failed')
    paused(config,'2026091600','submission_uncertain')
    monkeypatch.setattr(backfill,'run_init',lambda *a:pytest.fail('unknown active order needs inspection'))
    assert backfill.tick(config,root,0)['status']=='blocked'


def host_setup(tmp_path,monkeypatch,authorize=True):
    from climate_download.cams.config import CamsConfig
    root,op=tmp_path/'data',tmp_path/'op'
    plan=backfill.make_plan('20260908','20260921',authorized_at='2026-10-27T00:00:00Z')
    save(root/'plan.json',plan)
    save(op/'deployment.json',{'host_queue_root':str(root),'plan_sha256':digest(root/'plan.json')})
    config=CamsConfig(output_dir=tmp_path/'raw',first_init='2026102100',
                      accounts=[{'name':'a','credential_file':tmp_path/'key'}],cycle_accounts={0:'a',12:'a'})
    if authorize:enable(config,root,plan)
    second=plan['weeks'][1]
    for key in second['inits']:
        folder=root/'weeks'/second['id']/'acceptance'/key
        evidence(folder,key);save(folder/'case.json',{'passed':True})
    monkeypatch.setattr(host,'ready',lambda deployment,key:key in second['inits'])
    monkeypatch.setattr(host,'raw_ready',lambda deployment,key:key in second['inits'])
    monkeypatch.setattr(host,'run_stage',lambda *a:pytest.fail('complete evidence already exists'))
    return root,op,plan


def test_host_accepts_a_complete_later_week_without_accepting_the_earlier_gap(tmp_path,monkeypatch):
    root,op,plan=host_setup(tmp_path,monkeypatch)
    result=host.accept_tick(op)
    assert result['phase']=='week_accepted' and result['week']==plan['weeks'][1]['id']
    assert result['accepted_weeks']==1
    assert not (root/'weeks'/plan['weeks'][0]['id']/'assessment.json').exists()
    assert backfill.active_week(root,plan)==plan['weeks'][0]


def test_receipts_after_a_gap_are_still_verified_before_any_more_work(tmp_path,monkeypatch):
    root,op,plan=host_setup(tmp_path,monkeypatch);host.accept_tick(op)
    path=root/'weeks'/plan['weeks'][1]['id']/'acceptance'/plan['weeks'][1]['inits'][0]/'raw_acceptance.json'
    path.write_text('changed scientific acceptance')
    with pytest.raises(ValueError):backfill.active_week(root,plan)
    with pytest.raises(AssertionError):host.accept_tick(op)


def test_host_without_runtime_authorization_preserves_original_week_order(tmp_path,monkeypatch):
    _,op,plan=host_setup(tmp_path,monkeypatch,authorize=False)
    result=host.accept_tick(op)
    assert result['week']==plan['weeks'][0]['id'] and result['phase']=='downloading_week'


@pytest.mark.parametrize('phase',['remote_failed','validation_failed','waiting_for_remote'])
def test_one_gap_on_other_cycle_does_not_block_a_finished_lane(setup,monkeypatch,phase):
    from climate_download.cams.queue_policy import make_policy
    config,plan,root=prepare(setup,monkeypatch);enable(config,root,plan)
    save(root/'recovery_policy.json',make_policy(root,plan,config.mars_count_retry_delays_seconds,
         authorized_at='2026-10-29T06:00:00+00:00',continue_other_weeks_when_blocked=True))
    first,second=plan['weeks'][:2]
    gap=next(k for k in first['inits'] if k.endswith('12'))
    for key in first['inits']:
        folder=config.output_dir/'ec_cams'/key[:8]/(key[8:]+'z')
        if key!=gap:save(folder/f'{key[:8]}_{key[8:]}z_ec_cams.manifest.json',{})
        else:save(folder/'.cams/hourly/state.json',{'phase':phase,'date':key[:8],'cycle':12,
                  'group':'hourly','request_id':'deleted-original',
                  'failure_diagnostic':{'kind':'ads_order_deleted'},'deleted_recovery':{'retained':True}})
    calls=[]
    def run(cfg,date,cycle):
        calls.append(date);return {'status':'pending','date':date,'cycle':cycle}
    monkeypatch.setattr(backfill,'run_init',run)
    result=backfill.tick(config,root,0)
    assert result['week']==second['id'] and result['init']==second['inits'][0]
    assert calls==[second['start']] and first['id'] in result['deferred_weeks']
    assert not (root/'weeks'/first['id']/'assessment.json').exists()


def test_recovered_gap_keeps_the_normal_whole_week_acceptance_barrier(setup,monkeypatch):
    from climate_download.cams.queue_policy import make_policy
    config,plan,root=prepare(setup,monkeypatch);enable(config,root,plan)
    save(root/'recovery_policy.json',make_policy(root,plan,config.mars_count_retry_delays_seconds,
         authorized_at='2026-10-29T06:00:00+00:00',continue_other_weeks_when_blocked=True))
    first=plan['weeks'][0]
    for key in first['inits']:
        folder=config.output_dir/'ec_cams'/key[:8]/(key[8:]+'z')
        save(folder/f'{key[:8]}_{key[8:]}z_ec_cams.manifest.json',{})
        save(folder/'.cams/hourly/state.json',{'phase':'complete','deleted_recovery':{'retained':True}})
    monkeypatch.setattr(backfill,'run_init',lambda *a:pytest.fail('a completed week is waiting for acceptance'))
    result=backfill.tick(config,root,0)
    assert result['status']=='waiting_for_week_acceptance' and result['week']==first['id']


@pytest.mark.parametrize('new_live',[False,True])
def test_short_account_race_does_not_starve_history_or_new_live_work(setup,monkeypatch,new_live):
    from climate_download.cams.queue_policy import make_policy
    config,plan,root=prepare(setup,monkeypatch);enable(config,root,plan)
    save(root/'recovery_policy.json',make_policy(root,plan,config.mars_count_retry_delays_seconds,
         authorized_at='2026-10-29T06:00:00+00:00',continue_other_weeks_when_blocked=True))
    calls=[];sleeps=[]
    def run(*args):calls.append(1);return {'status':'locked' if len(calls)<3 else 'pending'}
    monkeypatch.setattr(backfill,'run_init',run)
    monkeypatch.setattr(backfill.time,'sleep',lambda seconds:sleeps.append(seconds))
    monkeypatch.setattr(backfill,'pending_inits',lambda *a:['2026102900'] if new_live and calls else [])
    result=backfill.tick(config,root,0)
    assert result['status']==('waiting_for_realtime' if new_live else 'pending')
    assert len(calls)==(1 if new_live else 3) and len(sleeps)==(0 if new_live else 2)
