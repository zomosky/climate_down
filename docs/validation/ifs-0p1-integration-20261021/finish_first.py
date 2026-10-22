"""Hand off only our first runner's post-download memory wait.

The initial running process used total cgroup usage including reclaimable
file cache. Preserve its original download timing and raw checksum receipt;
only replace that process's waiting/restore control after download completes.
"""
from __future__ import annotations

import datetime as dt
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import time


def main():
    root=Path('/logs/ifs-0p1-integration-20261021');key='20261020_12z'
    path=root/(key+'.json');deadline=time.monotonic()+12*3600
    while True:
        receipt=json.loads(path.read_text())
        if receipt['status'] in ['produced','failed']:return
        if receipt.get('stage')=='waiting_memory':break
        if time.monotonic()>deadline:raise TimeoutError('download did not finish within handoff window')
        time.sleep(5)
    assert receipt['stages']['download']['status']=='ok'
    assert receipt['stages']['raw_integrity']['all_sha256_ok']
    old_pid=625937
    command=Path(f'/proc/{old_pid}/cmdline').read_bytes().decode().split('\x00')
    assert str(root/'run_day.py') in command and '20261020' in command and '12' in command,command
    os.kill(old_pid,signal.SIGTERM)
    def now():return dt.datetime.now(dt.timezone.utc).isoformat()
    def save():
        temp=path.with_suffix('.handoff.tmp');temp.write_text(json.dumps(receipt,indent=2)+'\n');os.replace(temp,path)
    with (root/'pipeline.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        receipt['guard_handoff']={'at':now(),'original_pid':old_pid,
            'reason':'exclude reclaimable inactive_file cache from memory pressure; original download and SHA receipts retained'}
        memory=Path('/sys/fs/cgroup')
        while True:
            stat={k:int(v) for line in (memory/'memory.stat').read_text().splitlines() for k,v in [line.split()]}
            free=int((memory/'memory.max').read_text())-int((memory/'memory.current').read_text())+stat.get('inactive_file',0)
            receipt['memory_headroom_bytes']=free;save()
            if free>=12*1024**3:break
            time.sleep(15)
        began=time.perf_counter()
        command=['nice','-n','10','/workspace/climate_restorage/.venv/bin/climate_restore','run',
                 '--config','config/jobs/ifs_0p1_zarr_china_ser.yaml','--manifest',
                 '/climate_data/ifs-hres-0p1/20261020/12z/20261020_12z_ifs-hres-0p1.manifest.json']
        receipt['stage']='restore';receipt['status']='running'
        stage={'started_at':now(),'status':'running','command':command};receipt['stages']['restore']=stage
        download_finished=dt.datetime.fromisoformat(receipt['stages']['download']['finished_at'])
        receipt['restore_resource_wait_s']=round(max(0,(dt.datetime.now(dt.timezone.utc)-download_finished).total_seconds()-receipt['stages']['raw_integrity']['elapsed_s']),3)
        save()
        env=dict(os.environ);env.update(OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',NUMEXPR_NUM_THREADS='1',BLOSC_NTHREADS='2')
        with (root/(key+'-restore.log')).open('w') as log:
            result=subprocess.run(command,cwd='/workspace/climate_restorage',env=env,stdout=log,stderr=subprocess.STDOUT)
        stage.update(finished_at=now(),elapsed_s=round(time.perf_counter()-began,3),returncode=result.returncode,status='ok' if result.returncode==0 else 'failed')
        store=Path('/climate_data/climate_data_storage/zarr/ifs-hres-0p1/20261020_12z_ifs-hres-0p1.zarr')
        passed=result.returncode==0 and (store/'.zmetadata').exists()
        receipt.update(status='produced' if passed else 'failed',finished_at=now(),zarr=str(store),
            manifest=command[-1],native_leads=list(range(0,145,3))+list(range(150,361,6)))
        receipt['elapsed_s']=round((dt.datetime.now(dt.timezone.utc)-dt.datetime.fromisoformat(receipt['started_at'])).total_seconds(),3)
        if not passed:receipt['error']='restore did not complete after memory-guard handoff'
        save()


if __name__=='__main__':main()
