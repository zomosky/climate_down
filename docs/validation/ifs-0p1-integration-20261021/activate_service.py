"""Activate the verified reader image and verify the unchanged 0.25 query."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess

REMOTE=r'''
import datetime,hashlib,json,pathlib,subprocess,time,urllib.request
root=pathlib.Path('/home/zhangmingyu/operation/releases/ifs-0p1-20261021')
image=json.loads((root/'service-image.json').read_text())
baseline=json.loads((root/'service-baseline.json').read_text())
repo=pathlib.Path('/home/zhangmingyu/src/climate_pipeline/climate_watcher')
def run(args):
 p=subprocess.run(args,cwd=repo,text=True,capture_output=True,timeout=90)
 if p.returncode:raise RuntimeError(p.stderr[-2000:])
 return p.stdout.strip()
before=json.loads(run(['docker','inspect','climate-watcher']))[0]
assert before['Image']==image['old_image'], 'production image changed after canary verification'
spec={'source':'ifs-hres','variables':['t2m','u100','ssrd_accum'],'time':{'type':'operation','start':'2026-10-21T00:00:00Z','horizon_hours':24,'tz':'UTC','init_date':'2026-10-20','init_cycle':12},'space':{'kind':'sites','sites':[{'id':'cd','lat':30.67,'lon':104.06}]},'postprocess':{'resample':{'freq':'15min'}}}
report={'started_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'old_image':before['Image'],'new_image':image['new_image'],'rollback_tag':image['rollback_tag']}
began=time.perf_counter()
try:
 run(['docker','tag',image['new_image'],'climate-watcher:latest'])
 run(['docker','compose','up','-d','--no-deps','--no-build','watcher'])
 for _ in range(45):
  try:
   with urllib.request.urlopen('http://127.0.0.1:8090/api/v1/health',timeout=3) as response:health=json.load(response)
   if health.get('status')=='ok':break
  except Exception:time.sleep(1)
 else:raise RuntimeError('production did not become healthy')
 with urllib.request.urlopen('http://127.0.0.1:8090/',timeout=5) as response:
  assert b'<html' in response.read(131072).lower()
 request=urllib.request.Request('http://127.0.0.1:8090/api/v1/query',data=json.dumps(spec).encode(),headers={'Content-Type':'application/json'})
 with urllib.request.urlopen(request,timeout=130) as response:data=json.load(response)
 selected={key:data[key] for key in ['index','columns','values']}
 digest=hashlib.sha256(json.dumps(selected,sort_keys=True).encode()).hexdigest()
 assert digest==baseline['production_before']['sha256'], 'old 0.25-degree query changed'
 after=json.loads(run(['docker','inspect','climate-watcher']))[0]
 assert after['Image']==image['new_image']
 for key in ['Memory','NanoCpus','CpuQuota','CpuPeriod']:
  assert after['HostConfig'][key]==before['HostConfig'][key],key
 report.update(status='ok',elapsed_s=round(time.perf_counter()-began,3),health=health,
               old_025_query_sha256=digest,old_025_query_unchanged=True,
               memory_limit_bytes=after['HostConfig']['Memory'],nano_cpus=after['HostConfig']['NanoCpus'])
 (root/'production_after-025-query.json').write_text(json.dumps(data,ensure_ascii=False)+'\n')
 run(['docker','rm','-f','climate-watcher-ifs0p1-canary'])
 report['canary_removed']=True
except Exception as error:
 run(['docker','tag',image['old_image'],'climate-watcher:latest'])
 run(['docker','compose','up','-d','--no-deps','--no-build','watcher'])
 report.update(status='rolled_back',error=str(error))
 (root/'service-activation.json').write_text(json.dumps(report,indent=2)+'\n')
 raise
(root/'service-activation.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report,indent=2))
'''


def main():
    result=subprocess.run(['ssh','-o','BatchMode=yes','-o','ServerAliveInterval=10',
        'zhangmingyu@192.168.1.241','python3','-'],input=REMOTE,text=True,capture_output=True,timeout=240)
    Path(__file__).with_name('service-activation-output.txt').write_text(result.stdout+result.stderr)
    if result.returncode:raise RuntimeError(result.stderr[-3000:])
    Path(__file__).with_name('service-activation.json').write_text(result.stdout)
    print(result.stdout,end='')


if __name__=='__main__':main()
