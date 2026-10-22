"""Sample this producer cgroup through the end of the current one-init run."""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
import time


def pairs(path):
    return {key:int(value) for line in path.read_text().splitlines()
            for key,value in [line.split()]}


def main():
    root=Path('/logs/ifs-0p1-integration-20261021')
    cgroup=Path('/sys/fs/cgroup')
    output=root/'resource-samples.jsonl'
    if output.exists():
        raise RuntimeError('resource sample file already exists')
    baseline=pairs(cgroup/'memory.events')
    peak={};previous=None;count=0
    began=dt.datetime.now(dt.timezone.utc).isoformat()
    with output.open('w') as log:
        while True:
            receipt=json.loads((root/'20261020_12z.json').read_text())
            stat=pairs(cgroup/'memory.stat')
            current=int((cgroup/'memory.current').read_text())
            limit=int((cgroup/'memory.max').read_text())
            working=max(0,current-stat.get('inactive_file',0))
            cpu=pairs(cgroup/'cpu.stat')['usage_usec'];tick=time.monotonic()
            cores=(cpu-previous[1])/(tick-previous[0])/1e6 if previous else None
            previous=(tick,cpu)
            row={'utc':dt.datetime.now(dt.timezone.utc).isoformat(),'stage':receipt.get('stage'),
                 'status':receipt['status'],'limit_bytes':limit,'cgroup_current_bytes':current,
                 'inactive_file_bytes':stat.get('inactive_file',0),'working_bytes':working,
                 'cpu_cores_interval_mean':round(cores,3) if cores is not None else None}
            log.write(json.dumps(row)+'\n');log.flush();count+=1
            stage=receipt.get('stage','unknown')
            peak[stage]=max(peak.get(stage,0),working)
            if receipt['status'] in ['produced','failed']:
                break
            time.sleep(5)
    final=pairs(cgroup/'memory.events')
    summary={'started_at_utc':began,'finished_at_utc':row['utc'],'sampling_interval_s':5,
             'samples':count,'producer_limit_bytes':limit,'peak_working_bytes_by_stage':peak,
             'oom_kill_delta':final.get('oom_kill',0)-baseline.get('oom_kill',0),
             'scope':'producer cgroup; last part of download and entire restore; includes concurrent producer workloads'}
    (root/'resource-sampling.json').write_text(json.dumps(summary,indent=2)+'\n')


if __name__=='__main__':
    main()
