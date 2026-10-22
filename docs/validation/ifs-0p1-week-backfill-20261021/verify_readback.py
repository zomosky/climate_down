"""Verify unchanged original data and the available 0p1 init through the live API."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess


REMOTE = r'''
import datetime,hashlib,json,pathlib,subprocess,time,urllib.request
release=pathlib.Path('/home/zhangmingyu/operation/releases/ifs-0p1-20261021')
operation=release.parents[1]
report={'checked_at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
def query(spec):
 started=time.monotonic()
 request=urllib.request.Request('http://127.0.0.1:8090/api/v1/query',data=json.dumps(spec).encode(),headers={'Content-Type':'application/json'})
 with urllib.request.urlopen(request,timeout=30) as response:
  body=json.load(response)
  assert response.status==200
 return {'seconds':round(time.monotonic()-started,4),'rows':len(body['index']),
  'sha256':hashlib.sha256(json.dumps({k:body[k] for k in ['index','columns','values']},sort_keys=True).encode()).hexdigest()}
old={'source':'ifs-hres','variables':['t2m','u100','ssrd_accum'],
 'time':{'type':'operation','start':'2026-10-21T00:00:00Z','horizon_hours':24,'tz':'UTC','init_date':'2026-10-20','init_cycle':12},
 'space':{'kind':'sites','sites':[{'id':'cd','lat':30.67,'lon':104.06}]},'postprocess':{'resample':{'freq':'15min'}}}
report['original_025']=query(old)
report['original_025']['expected_sha256']=json.loads((release/'service-baseline.json').read_text())['production_before']['sha256']
assert report['original_025']['sha256']==report['original_025']['expected_sha256']
new={'source':'ifs-hres-0p1','variables':['t2m','u100','v100','ssrd_accum','t_850'],
 'time':{'type':'operation','start':'2026-10-20T18:00:00+00:00','horizon_hours':24,'tz':'UTC','init_date':'2026-10-20','init_cycle':12},
 'space':{'kind':'sites','sites':[{'id':'exact_attempt2','lat':35.0,'lon':116.0}],'method':'nearest'},
 'postprocess':{'deaccumulate':False}}
report['new_01']=query(new)
result=subprocess.run(['python3',str(operation/'reconcile_sources.py'),'--only-source','ifs-hres-0p1','--dry-run',
 '--lock','/tmp/ifs-0p1-week-readback.lock'],capture_output=True,text=True,timeout=30)
assert result.returncode==0,result.stderr
assert 'HEALTHY ifs-hres-0p1 20261020 12z' in result.stdout and 'REPAIR' not in result.stdout,result.stdout
report['reconcile']=result.stdout.strip()
result=subprocess.run(['python3',str(operation/'restore_ifs_0p1.py'),'--dry-run'],capture_output=True,text=True,timeout=15)
assert result.returncode==0,result.stderr
report['restore_guard']=json.loads(result.stdout)
report['status']='ok'
print(json.dumps(report,ensure_ascii=False))
'''


def main() -> None:
    root = Path(__file__).resolve().parent
    baseline = json.loads((root.parent / 'ifs-0p1-integration-20261021/evidence/api/native_sites.json').read_text())
    expected = hashlib.sha256(json.dumps(
        {k: baseline[k] for k in ['index', 'columns', 'values']}, sort_keys=True,
    ).encode()).hexdigest()
    result = subprocess.run(
        ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8',
         'zhangmingyu@192.168.1.241', 'python3', '-'],
        input=REMOTE, text=True, capture_output=True, timeout=100,
    )
    if result.returncode:
        raise RuntimeError(result.stderr[-3000:])
    report = json.loads(result.stdout)
    report['new_01']['expected_sha256'] = expected
    assert report['new_01']['sha256'] == expected, '0p1 live query differs from accepted native values'
    (root / 'readback.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
