"""Read-only current-task progress stream; no scheduler or data mutations."""
from __future__ import annotations

from pathlib import Path
import subprocess

REMOTE = r'''
import datetime as dt,json,time
from pathlib import Path
root=Path('/home/zhangmingyu/dev-docker/logs/ifs-0p1-integration-20261021')
api=Path('/home/zhangmingyu/operation/releases/ifs-0p1-20261021/acceptance.json')
while True:
    receipt=json.loads((root/'20261020_12z.json').read_text())
    files=list(Path('/training_data/climate_data/ifs-hres-0p1/20261020/12z').glob('f*.subset.grib2'))
    acceptance=json.loads(api.read_text()) if api.exists() else {}
    now=dt.datetime.now(dt.timezone.utc)
    print(json.dumps({'utc':now.isoformat(),'files':len(files),
                      'MiB':round(sum(path.stat().st_size for path in files)/1024**2,1),
                      'minutes_since_download_start':round((now-dt.datetime.fromisoformat(receipt['started_at'])).total_seconds()/60,1),
                      'production_status':receipt['status'],'stage':receipt.get('stage'),
                      'acceptance_status':acceptance.get('status'),
                      'error':receipt.get('error') or acceptance.get('error')}),flush=True)
    if receipt['status']=='failed' or acceptance.get('status') in ['ok','failed']:
        break
    time.sleep(50)
'''


def main():
    process=subprocess.Popen(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=8',
                              'zhangmingyu@192.168.1.241','python3','-u','-'],
                             stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
    process.stdin.write(REMOTE)
    process.stdin.close()
    with Path(__file__).with_name('watch-progress.jsonl').open('a') as log:
        for line in process.stdout:
            log.write(line);log.flush()
            print(line,end='',flush=True)
    raise SystemExit(process.wait())


if __name__=='__main__':
    main()
