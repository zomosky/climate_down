"""One-off host supervisor for this explicitly requested init's final checks."""
from __future__ import annotations

import datetime as dt
import argparse
import fcntl
import json
import os
from pathlib import Path
import subprocess
import time


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--retry-failed',action='store_true')
    args=parser.parse_args()
    root = Path('/home/zhangmingyu/operation/releases/ifs-0p1-20261021')
    production_root = Path('/home/zhangmingyu/dev-docker/logs/ifs-0p1-integration-20261021')
    key = '20261020_12z'
    path = root/'acceptance.json'
    attempt=1
    if path.exists():
        previous=json.loads(path.read_text())
        if not args.retry_failed or previous['status']!='failed':
            raise RuntimeError('acceptance receipt already exists; preserve original timings')
        attempt=previous.get('attempt',1)+1
        backup=root/f'acceptance-attempt{attempt-1}.json'
        if backup.exists():raise RuntimeError('previous attempt backup already exists')
        path.rename(backup)
        for name in previous['stages']:
            log=root/(key+'-'+name+'.log')
            if log.exists():log.rename(root/(key+'-'+name+f'-attempt{attempt-1}.log'))
    receipt = {'status':'waiting_product','date':'20261020','cycle':12,
               'started_at_utc':now(),'stages':{},'attempt':attempt}

    def save():
        temporary = path.with_suffix('.tmp')
        temporary.write_text(json.dumps(receipt,indent=2)+'\n')
        os.replace(temporary,path)

    with (root/'acceptance.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        save()
        deadline = time.monotonic()+6*3600
        try:
            while True:
                production = json.loads((production_root/(key+'.json')).read_text())
                if production['status']=='produced':
                    break
                if production['status']=='failed':
                    raise RuntimeError('production failed: '+production.get('error','unknown'))
                if time.monotonic()>deadline:
                    raise TimeoutError('product did not finish within acceptance window')
                time.sleep(5)
            receipt['product_observed_at_utc'] = now()

            def stage(name,command):
                started = time.perf_counter()
                receipt['status'] = name
                record = {'status':'running','started_at_utc':now()}
                receipt['stages'][name] = record
                save()
                with (root/(key+'-'+name+'.log')).open('w') as log:
                    result = subprocess.run(command,stdout=log,stderr=subprocess.STDOUT)
                record.update(status='ok' if result.returncode==0 else 'failed',
                              finished_at_utc=now(),seconds=round(time.perf_counter()-started,3),
                              returncode=result.returncode)
                save()
                if result.returncode:
                    raise RuntimeError(name+' failed; see dedicated log')

            # First HTTP query comes before the exhaustive Zarr audit to measure
            # first-service response without the audit warming every Zarr chunk.
            api_dir='/tmp/ifs-0p1-integration-20261021/'+key
            site_id='exact_attempt2' if attempt>=3 else f'exact_attempt{attempt}'
            stage('api_audit',['docker','exec','climate-watcher',
                              '/workspace/climate_watcher/.venv/bin/python',
                              '/tmp/ifs-0p1-integration-20261021/audit_api.py',
                              '--date','20261020','--cycle','12','--output-dir',api_dir,
                              '--site-id',site_id]+(['--reuse-json'] if attempt>=3 else []))
            stage('native_audit',['docker','exec','-e','OMP_NUM_THREADS=1',
                                 '-e','OPENBLAS_NUM_THREADS=1','zhangmy-dev',
                                 'nice','-n','10','/workspace/climate_restorage/.venv/bin/python',
                                 '/logs/ifs-0p1-integration-20261021/audit_native.py',
                                 '--date','20261020','--cycle','12','--output',
                                 '/logs/ifs-0p1-integration-20261021/'+key+'-native-audit.json'])
            receipt.update(status='ok',finished_at_utc=now())
        except Exception as error:
            receipt.update(status='failed',finished_at_utc=now(),error=repr(error))
            raise
        finally:
            save()


if __name__=='__main__':
    main()
