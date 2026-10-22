"""Compact read-only snapshot of the bounded 0p1 acceptance run."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess

REMOTE=r'''
import datetime,json,pathlib,subprocess,urllib.request,xml.etree.ElementTree as ET
root=pathlib.Path('/home/zhangmingyu/dev-docker/logs/ifs-0p1-integration-20261021')
report={'time_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'inits':[]}
for key in ['20261020_12z','20261021_00z']:
 p=root/(key+'.json')
 if not p.exists():continue
 receipt=json.loads(p.read_text());date=receipt['date'];cycle=receipt['cycle']
 files=list(pathlib.Path(f'/training_data/climate_data/ifs-hres-0p1/{date}/{cycle:02d}z').glob('f*.subset.grib2'))
 log=root/(key+'-'+receipt.get('stage','download')+'.log')
 latest=log.read_text(errors='replace').splitlines()[-1:] if log.exists() else []
 report['inits'].append({'date':date,'cycle':cycle,'status':receipt['status'],'stage':receipt.get('stage'),
  'files':len(files),'MiB':round(sum(p.stat().st_size for p in files)/1024**2,1),
  'started_at':receipt.get('started_at'),'finished_at':receipt.get('finished_at'),
  'elapsed_s':receipt.get('elapsed_s'),'stages':receipt.get('stages'),
  'error':receipt.get('error'),'latest':latest})
p=subprocess.run(['docker','stats','--no-stream','--format','{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}','zhangmy-dev','climate-watcher'],text=True,capture_output=True,timeout=10)
report['resources']=p.stdout.strip()
acceptance=pathlib.Path('/home/zhangmingyu/operation/releases/ifs-0p1-20261021/acceptance.json')
if acceptance.exists():report['acceptance']=json.loads(acceptance.read_text())
try:
 with urllib.request.urlopen('http://127.0.0.1:8090/api/v1/health',timeout=3) as response:
  health=json.load(response)
 report['health']={'status':health.get('status'),'fd':health.get('checks',{}).get('file_descriptors',{}).get('used')}
except Exception as error:report['health']={'error':str(error)}
if __UPSTREAM__:
 try:
  url='https://storage.googleapis.com/ecmwf-open-data/?prefix=20261021/00z/ifs/0p1/oper/'
  with urllib.request.urlopen(url,timeout=12) as response:xml=ET.fromstring(response.read(512000))
  import re
  keys=[e.text for e in xml.iter() if e.tag.endswith('}Key') or e.tag=='Key']
  parts={part:sorted(int(m.group(1)) for key in keys if (m:=re.search(r'-(\d+)h-oper-fc-'+part+r'\.index$',key))) for part in ['sfc','pl']}
  report['upstream_20261021_00z']={'sfc':len(parts['sfc']),'pl':len(parts['pl']),
   'common':len(set(parts['sfc'])&set(parts['pl'])),'complete':all(len(parts[p])==85 for p in parts)}
 except Exception as error:report['upstream_20261021_00z']={'error':f'{type(error).__name__}: {error}'}
print(json.dumps(report,ensure_ascii=False))
'''


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--upstream',action='store_true')
    parser.add_argument('--brief',action='store_true');args=parser.parse_args()
    result=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=8','zhangmingyu@192.168.1.241','python3','-'],
        input=REMOTE.replace('__UPSTREAM__',repr(args.upstream)),text=True,capture_output=True,timeout=40)
    if result.returncode:raise RuntimeError(result.stderr[-1500:])
    data=json.loads(result.stdout)
    with Path(__file__).with_name('progress.jsonl').open('a') as handle:handle.write(json.dumps(data,ensure_ascii=False)+'\n')
    shown=data
    if args.brief:
        shown={key:value for key,value in data.items() if key!='inits'}
        shown['inits']=[{key:value for key,value in init.items()
                        if key in ['date','cycle','status','stage','files','MiB','elapsed_s','error']}
                       for init in data['inits']]
    print(json.dumps(shown,ensure_ascii=False,indent=2))


if __name__=='__main__':main()
