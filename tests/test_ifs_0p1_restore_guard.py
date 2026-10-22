import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace


def module():
    path=Path(__file__).parents[1]/'deploy/restore_ifs_0p1.py'
    spec=importlib.util.spec_from_file_location('restore_ifs_0p1',path)
    result=importlib.util.module_from_spec(spec);spec.loader.exec_module(result)
    return result


def test_low_memory_defers_only_the_new_source(monkeypatch):
    m=module();calls=[]
    def run(args,**kwargs):
        calls.append(args)
        return SimpleNamespace(returncode=0,stdout=json.dumps({'limit_bytes':36*1024**3,'used_bytes':26*1024**3}),stderr='')
    monkeypatch.setattr(m.subprocess,'run',run)
    assert m.main([])==0 and len(calls)==1


def test_ready_guard_delegates_existing_scanner_with_independent_config(monkeypatch):
    m=module();calls=[]
    def run(args,**kwargs):
        calls.append((args,kwargs))
        return SimpleNamespace(returncode=0,stdout=json.dumps({'limit_bytes':36*1024**3,'used_bytes':21*1024**3}),stderr='')
    monkeypatch.setattr(m.subprocess,'run',run)
    assert m.main(['--runner','/operation/restore_scan.sh'])==0
    assert len(calls)==2
    env=calls[1][1]['env']
    assert env['SOURCE']=='ifs-hres-0p1'
    assert env['JOB']=='config/jobs/ifs_0p1_zarr_china_ser.yaml'
    assert env['LOCK']=='/operation/lock/restore_scan_ifs_0p1.lock'


def test_unknown_memory_limit_is_not_treated_as_free_memory(monkeypatch):
    m=module();calls=[]
    def run(args,**kwargs):
        calls.append(args)
        return SimpleNamespace(returncode=0,stdout=json.dumps({'limit_bytes':None,'used_bytes':1024}),stderr='')
    monkeypatch.setattr(m.subprocess,'run',run)
    assert m.main([])==1 and len(calls)==1


def test_reclaimable_grib_file_cache_does_not_block_restore():
    m=module()
    memory={'limit_bytes':36*1024**3,'used_bytes':35*1024**3,'inactive_file_bytes':31*1024**3}
    assert m.available_headroom(memory)==32*1024**3
