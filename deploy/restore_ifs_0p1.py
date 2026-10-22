#!/usr/bin/env python3
"""Run the existing restore scanner for 0p1 only when memory permits."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess

MEMORY_PROBE = """
import json
from pathlib import Path
root=Path('/sys/fs/cgroup')
cap=(root/'memory.max').read_text().strip()
used=int((root/'memory.current').read_text())
stat={k:int(v) for line in (root/'memory.stat').read_text().splitlines() for k,v in [line.split()]}
print(json.dumps({'limit_bytes':int(cap) if cap!='max' else None,'used_bytes':used,
                  'inactive_file_bytes':stat.get('inactive_file',0)}))
"""


def available_headroom(memory):
    cap=memory.get('limit_bytes')
    if cap is None:return None
    # Docker's working-set convention: inactive file cache is reclaimable.
    # Counting all cached GRIB pages as busy would defer this job forever.
    return max(0,min(cap,cap-memory['used_bytes']+memory.get('inactive_file_bytes',0)))


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runner',type=Path,default=Path('/home/zhangmingyu/operation/restore_scan.sh'))
    parser.add_argument('--min-headroom-gib',type=float,default=12)
    parser.add_argument('--dry-run',action='store_true')
    args=parser.parse_args(argv)
    if args.min_headroom_gib<=0:parser.error('min-headroom-gib must be positive')
    container=os.environ.get('CONTAINER','zhangmy-dev')
    probe=subprocess.run(['docker','exec',container,'/workspace/climate_restorage/.venv/bin/python',
                          '-c',MEMORY_PROBE],text=True,capture_output=True,timeout=20)
    if probe.returncode:
        print('ifs_0p1_restore: memory probe failed: '+probe.stderr.strip()[-600:])
        return 1
    memory=json.loads(probe.stdout)
    free=available_headroom(memory)
    memory['headroom_bytes']=free
    if free is None:
        print('ifs_0p1_restore: finite container memory limit required')
        return 1
    if free<args.min_headroom_gib*1024**3:
        print(f'ifs_0p1_restore: defer, headroom={free/1024**3:.2f} GiB; minimum={args.min_headroom_gib:g} GiB')
        return 0
    env=dict(os.environ)
    env.update(SOURCE='ifs-hres-0p1',JOB='config/jobs/ifs_0p1_zarr_china_ser.yaml',
               LOCK=str(args.runner.parent/'lock/restore_scan_ifs_0p1.lock'))
    if args.dry_run:
        print(json.dumps({'ready':True,'memory':memory,'runner':str(args.runner),
                          'source':env['SOURCE'],'job':env['JOB']}))
        return 0
    return subprocess.run(['/bin/bash',str(args.runner)],env=env).returncode


if __name__=='__main__':raise SystemExit(main())
