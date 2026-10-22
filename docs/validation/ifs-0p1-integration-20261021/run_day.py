"""Timed one-init run of production download -> manifest -> restore.

Runs inside zhangmy-dev. Persistent logs/receipts survive SSH disconnects.
Only ifs-hres-0p1 paths are written, and native full leads are mandatory.
"""
from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--date',required=True)
    parser.add_argument('--cycle',type=int,choices=[0,12],required=True)
    args=parser.parse_args()
    root=Path('/logs/ifs-0p1-integration-20261021')
    root.mkdir(parents=True,exist_ok=True)
    key=f'{args.date}_{args.cycle:02d}z'
    receipt_path=root/(key+'.json')
    if receipt_path.exists():raise RuntimeError('receipt already exists; refusing to overwrite timings')
    receipt={'date':args.date,'cycle':args.cycle,'source':'ifs-hres-0p1',
             'status':'queued','queued_at':now(),'stages':{}}
    def save():
        temp=receipt_path.with_suffix('.tmp')
        temp.write_text(json.dumps(receipt,indent=2)+'\n');os.replace(temp,receipt_path)
    save()
    env=dict(os.environ)
    for name in ('HTTP_PROXY','HTTPS_PROXY','ALL_PROXY','http_proxy','https_proxy','all_proxy'):
        env.pop(name,None)
    env.update(OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',
               NUMEXPR_NUM_THREADS='1',BLOSC_NTHREADS='2')
    with (root/'pipeline.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        began=time.perf_counter()
        receipt.update(status='running',started_at=now());save()
        def stage(name,command,cwd):
            start=time.perf_counter()
            record={'started_at':now(),'status':'running','command':command}
            receipt['stage']=name;receipt['stages'][name]=record;save()
            with (root/(key+'-'+name+'.log')).open('w') as log:
                result=subprocess.run(command,cwd=cwd,env=env,stdout=log,stderr=subprocess.STDOUT)
            record.update(finished_at=now(),elapsed_s=round(time.perf_counter()-start,3),
                          returncode=result.returncode,status='ok' if result.returncode==0 else 'failed')
            save()
            if result.returncode:raise RuntimeError(name+' exited '+str(result.returncode))
        try:
            manifest=Path(f'/climate_data/ifs-hres-0p1/{args.date}/{args.cycle:02d}z/{key}_ifs-hres-0p1.manifest.json')
            if manifest.exists():raise RuntimeError('existing manifest would invalidate cold-download timing')
            stage('download',['/workspace/climate_down/.venv/bin/climate_download','run',
                  '--config','config/jobs/ifs_0p1_renewables_ser.yaml','--date',args.date,
                  '--cycle',str(args.cycle),'--steps','all','--no-progress'],'/workspace/climate_down')
            data=json.loads(manifest.read_text())
            expected=list(range(0,145,3))+list(range(150,361,6))
            actual=sorted(x['step_hours'] for x in data['files'])
            if actual!=expected or data.get('failures'):
                raise RuntimeError('manifest is not the full 85-lead native product')
            receipt['download_bytes']=sum(x['size_bytes'] for x in data['files'])
            check_start=time.perf_counter()
            for entry in data['files']:
                path=Path(entry['path'])
                digest=hashlib.file_digest(path.open('rb'),'sha256').hexdigest()
                if digest!=entry['sha256']:raise RuntimeError('raw sha256 mismatch: '+path.name)
            receipt['stages']['raw_integrity']={'elapsed_s':round(time.perf_counter()-check_start,3),'files':len(actual),'all_sha256_ok':True}
            save()
            wait_start=time.perf_counter()
            memory=Path('/sys/fs/cgroup/memory.current');limit=Path('/sys/fs/cgroup/memory.max')
            while memory.exists() and limit.exists() and limit.read_text().strip()!='max':
                stat={k:int(v) for line in (memory.parent/'memory.stat').read_text().splitlines()
                      for k,v in [line.split()]}
                free=int(limit.read_text())-int(memory.read_text())+stat.get('inactive_file',0)
                if free>=12*1024**3:break
                receipt['stage']='waiting_memory';receipt['memory_headroom_bytes']=free;save()
                if time.perf_counter()-wait_start>3600:raise RuntimeError('insufficient memory headroom for restore')
                time.sleep(15)
            receipt['restore_resource_wait_s']=round(time.perf_counter()-wait_start,3)
            stage('restore',['nice','-n','10','/workspace/climate_restorage/.venv/bin/climate_restore','run',
                  '--config','config/jobs/ifs_0p1_zarr_china_ser.yaml','--manifest',str(manifest)],'/workspace/climate_restorage')
            product=Path(f'/climate_data/climate_data_storage/zarr/ifs-hres-0p1/{key}_ifs-hres-0p1.zarr')
            if not (product/'.zmetadata').exists():raise RuntimeError('restore has no completion marker')
            receipt.update(status='produced',finished_at=now(),elapsed_s=round(time.perf_counter()-began,3),
                           manifest=str(manifest),zarr=str(product),native_leads=actual)
        except Exception as error:
            receipt.update(status='failed',finished_at=now(),error=f'{type(error).__name__}: {error}',
                           elapsed_s=round(time.perf_counter()-began,3))
            raise
        finally:save()


if __name__=='__main__':main()
