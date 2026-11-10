"""Failure cases: live starvation, interrupted download, partial publication, duplicate work."""
import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import sys
import pytest

deploy = Path(__file__).parents[1] / 'deploy'
spec = importlib.util.spec_from_file_location('gefs_run', deploy/'gefs_run.py')
live = importlib.util.module_from_spec(spec); spec.loader.exec_module(live)
sys.modules['gefs_run'] = live
spec = importlib.util.spec_from_file_location('gefs_backfill', deploy/'gefs_backfill.py')
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)


def cfg(tmp_path):
    return dict(history_start='20261031', history_end='20261101', live_start_date='20261105',
                release_root='/release', data_root='/data', zarr_root='/zarr',
                host_zarr_root=str(tmp_path/'zarr'), state_file=str(tmp_path/'state.json'), member_count=5)


def now(minute): return datetime(2026,11,9,1,minute,tzinfo=timezone.utc)


def products(c, date):
    for source in ['gefs-ens','gefs-ctrl']:
        p=Path(c['host_zarr_root'])/source/f'{date}_00z_{source}.zarr'/'.zmetadata'
        p.parent.mkdir(parents=True);p.write_text('{}')


def test_inclusive_queue_and_reject_overlap(tmp_path):
    assert m.dates_between('20261001','20261104')[0]=='20261104'
    assert len(m.dates_between('20261001','20261104'))==35
    c=cfg(tmp_path);c['history_end']='20261105'
    with pytest.raises(ValueError):m.tick(c,{},now(10))


@pytest.mark.parametrize('minute',[0,4,29,30,34,59])
def test_reserve_live_windows(tmp_path,minute):
    def never(*a,**k):pytest.fail('history must yield')
    assert m.tick(cfg(tmp_path),{},now(minute),run=never)['status']=='yield_to_live'


def test_timeout_keeps_history_pending_and_never_publishes(tmp_path):
    calls=[];state={}
    def fail(cmd,**kw):calls.append(cmd);return SimpleNamespace(returncode=124)
    result=m.tick(cfg(tmp_path),state,now(10),run=fail,monotonic=lambda:0)
    assert result['status']=='pending' and len(calls)==1
    assert '1140s' in calls[0] and '7200s' not in calls[0]
    assert not state['dates']['20261101'].get('download_complete')


def test_download_then_defer_publish_and_resume_without_redownload(tmp_path):
    c=cfg(tmp_path);state={};clock=iter([0,0,1000]);calls=[]
    def ok(cmd,**kw):calls.append(cmd);return SimpleNamespace(returncode=0)
    assert m.tick(c,state,now(10),run=ok,monotonic=lambda:next(clock))['next_phase']=='publish'
    assert len(calls)==1 and state['dates']['20261101']['download_complete']
    def publish(cmd,**kw):
        assert 'gefs_publish.py' in ' '.join(cmd)
        products(c,'20261101');return SimpleNamespace(returncode=0)
    assert m.tick(c,state,now(40),run=publish,monotonic=lambda:0)['status']=='published'
    products(c,'20261031');state['dates']['20261031']={'status':'complete','member_count':5}
    assert m.tick(c,state,now(40),run=ok)['status']=='complete'


def test_missing_product_is_not_complete(tmp_path):
    c=cfg(tmp_path);state={'dates':{'20261101':{'download_complete':True}}}
    with pytest.raises(RuntimeError):m.tick(c,state,now(10),run=lambda *a,**k:SimpleNamespace(returncode=0),monotonic=lambda:0)
    assert state['dates']['20261101']['status']=='pending'
