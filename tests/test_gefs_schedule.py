import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import pytest
p=Path(__file__).parents[1]/'deploy/gefs_run.py'
spec=importlib.util.spec_from_file_location('gefs_run',p);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

def config(tmp_path):
 return dict(start_date='20261105',release_root='/workspace/releases/test',data_root='/climate_data',zarr_root='/zarr',host_zarr_root=str(tmp_path/'zarr'),member_count=5,state_file=str(tmp_path/'state.json'))

def test_beijing_1730_boundary_and_midnight():
 for utc,latest in [('2026-11-06T09:29:59','20261105'),('2026-11-06T09:30:00','20261106'),('2026-11-06T17:00:00','20261106'),('2026-11-07T01:00:00','20261106')]:
  assert m.eligible_dates(datetime.fromisoformat(utc).replace(tzinfo=timezone.utc),'20261101')[0]==latest

def test_pending_old_dates_survive():
 assert '20261001' in m.eligible_dates(datetime(2026,11,6,10,tzinfo=timezone.utc),'20261001',['20261001'])
 assert m.eligible_dates(datetime(2026,11,6,1,tzinfo=timezone.utc),'20261106')==[]

def test_command_has_inner_timeout_and_complete_member_selection(tmp_path):
 cmds=m.commands(config(tmp_path),'20261105')
 assert cmds[0][0]=='download' and cmds[1][0]=='publish'
 for _,cmd in cmds:assert 'timeout' in cmd and '--bounded-exec' in cmd and '7200s' in cmd
 assert cmds[0][1][cmds[0][1].index('--member-count')+1]=='5'

@pytest.mark.parametrize('bad',[0,32,True,'5'])
def test_invalid_member_count(tmp_path,bad):
 c=config(tmp_path);c['member_count']=bad
 with pytest.raises(ValueError):m.commands(c,'20261105')

def test_failed_download_never_publishes_and_retries(tmp_path):
 c=config(tmp_path);state={};calls=[]
 def fail(cmd,**kw):calls.append(cmd);return SimpleNamespace(returncode=1)
 assert m.tick(c,state,datetime(2026,11,5,10,tzinfo=timezone.utc),run=fail)==1
 assert len(calls)==1 and state['dates']['20261105']['status']=='pending'
 def ok(cmd,**kw):
  calls.append(cmd)
  if 'gefs_publish.py' in ' '.join(cmd):
   for s in ['gefs-ens','gefs-ctrl']:
    p=Path(c['host_zarr_root'])/s/f'20261105_00z_{s}.zarr'/'.zmetadata';p.parent.mkdir(parents=True);p.write_text('{}')
  return SimpleNamespace(returncode=0)
 assert m.tick(c,state,datetime(2026,11,5,10,tzinfo=timezone.utc),run=ok)==0
 assert len(calls)==3
 assert m.tick(c,state,datetime(2026,11,5,11,tzinfo=timezone.utc),run=ok)==0
 assert len(calls)==3
 (Path(c['host_zarr_root'])/'gefs-ctrl/20261105_00z_gefs-ctrl.zarr/.zmetadata').unlink()
 assert not m.completed(c,'20261105',state['dates']['20261105'])
