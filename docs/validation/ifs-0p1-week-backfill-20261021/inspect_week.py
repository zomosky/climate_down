"""Read-only, reproducible Google availability and production backfill audit."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess


REMOTE = r'''
import concurrent.futures,datetime,hashlib,json,pathlib,re,subprocess,time,urllib.parse,urllib.request,xml.etree.ElementTree as ET
source='ifs-hres-0p1'
data_root=pathlib.Path('/training_data/climate_data')
expected=list(range(0,145,3))+list(range(150,361,6))
days=[(datetime.date(2026,10,14)+datetime.timedelta(days=n)).strftime('%Y%m%d') for n in range(7)]

def listing(init):
 day,cycle=init
 prefix=f'{day}/{cycle:02d}z/ifs/0p1/oper/'
 url='https://storage.googleapis.com/ecmwf-open-data/?'+urllib.parse.urlencode({'prefix':prefix,'max-keys':1000})
 started=time.monotonic()
 result={'date':day,'cycle':cycle,'prefix':prefix,'url':url}
 for attempt in range(1,4):
  try:
   with urllib.request.urlopen(url,timeout=18) as response:
    body=response.read(1024*1024)
    result['http_status']=response.status
   root=ET.fromstring(body)
   nodes={}
   for element in root.iter():nodes.setdefault(element.tag.rsplit('}',1)[-1],[]).append(element.text)
   if nodes.get('IsTruncated')!=['false']:raise ValueError('Unexpected truncated listing')
   keys=nodes.get('Key',[])
   leads={}
   for part in ['sfc','pl']:
    for suffix in ['index','grib2']:
     pattern=re.compile(r'-(\d+)h-oper-fc-'+part+r'\.'+suffix+r'$')
     leads[part+'_'+suffix]=sorted(int(match.group(1)) for key in keys if (match:=pattern.search(key)))
   result.update({'attempts':attempt,'elapsed_s':round(time.monotonic()-started,3),
    'objects':len(keys),'leads':leads,'complete':all(value==expected for value in leads.values()),
    'empty':not keys,'xml_sha256':hashlib.sha256(body).hexdigest(),'xml':body.decode()})
   return result
  except Exception as error:
   result['error']=f'{type(error).__name__}: {error}'
   if attempt<3:time.sleep(attempt)
 result['attempts']=3
 return result

def inventory(day,cycle,verify_hashes=False):
 key=f'{day}_{cycle:02d}z'
 directory=data_root/source/day/f'{cycle:02d}z'
 files=sorted(directory.glob('f*.subset.grib2'))
 result={'date':day,'cycle':cycle,'files':len(files),'bytes':sum(p.stat().st_size for p in files)}
 manifest=directory/f'{key}_{source}.manifest.json'
 if manifest.exists():
  body=manifest.read_bytes();payload=json.loads(body)
  records=payload.get('files',[])
  result['manifest']={'path':str(manifest),'sha256':hashlib.sha256(body).hexdigest(),
   'completed_at':payload.get('completed_at'),'failures':payload.get('failures',[]),
   'leads':sorted(f['step_hours'] for f in records)}
  checks=[]
  for record in records:
   path=pathlib.Path(record['path'])
   if path.is_absolute():path=data_root/path.relative_to('/climate_data')
   else:path=directory/path
   check={'step':record['step_hours'],'exists':path.is_file()}
   if path.is_file():
    with path.open('rb') as handle:
     check['head_ok']=handle.read(4)==b'GRIB'
     handle.seek(-4,2);check['tail_ok']=handle.read(4)==b'7777'
    if verify_hashes:
     digest=hashlib.sha256()
     with path.open('rb') as handle:
      for block in iter(lambda:handle.read(8*1024*1024),b''):digest.update(block)
     check['sha256_ok']=digest.hexdigest()==record['sha256']
   checks.append(check)
  result['manifest']['raw_checks']=checks
  result['manifest']['healthy']=bool(payload.get('completed_at')) and not payload.get('failures') and result['manifest']['leads']==expected and all(all(value for name,value in check.items() if name!='step') for check in checks)
 zarr=data_root/'climate_data_storage/zarr'/source/f'{key}_{source}.zarr'
 marker=zarr/'.zmetadata'
 result['zarr']={'path':str(zarr),'consolidated':marker.is_file()}
 if marker.is_file():
  body=marker.read_bytes();metadata=json.loads(body)['metadata']
  result['zarr'].update({'metadata_sha256':hashlib.sha256(body).hexdigest(),
   'variables':sorted(k.split('/')[0] for k in metadata if k.endswith('/.zarray') and k.split('/')[0] not in ['latitude','longitude','step','time','valid_time','pressure_level']),
   'step_shape':metadata['step/.zarray']['shape'],
   'latitude_shape':metadata['latitude/.zarray']['shape'],
   'longitude_shape':metadata['longitude/.zarray']['shape']})
 return result

report={'created_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
 'source':source,'requested_window':{'start':'20261014','end':'20261020','cycles':[0,12],'timezone':'UTC'},'expected_leads':expected}
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
 report['upstream']=list(pool.map(listing,[(day,cycle) for day in days for cycle in [0,12]]))
report['inventory']=[inventory(item['date'],item['cycle'],True) for item in report['upstream'] if item.get('complete')]
report['current_day']=inventory('20261021',0)
cron=subprocess.run(['crontab','-l'],capture_output=True,text=True,check=True).stdout
report['cron']={'sha256':hashlib.sha256(cron.encode()).hexdigest(),'lines':[line for line in cron.splitlines() if 'ifs_0p1' in line or 'IFS-0P1' in line]}
before=pathlib.Path('/home/zhangmingyu/operation/releases/ifs-0p1-20261021/crontab-before.txt').read_text()
report['cron']['previous_lines_preserved']=all(line in cron.splitlines() for line in before.splitlines())
report['host_timezone']=pathlib.Path('/etc/timezone').read_text().strip()
report['reconcile_timer']=subprocess.run(['systemctl','is-active','climate-source-reconcile.timer'],capture_output=True,text=True).stdout.strip()
for name in ['download_ifs_0p1_00z.log','download_ifs_0p1_12z.log','restore_ifs_0p1.log']:
 path=pathlib.Path('/home/zhangmingyu/operation/logs')/name
 lines=path.read_text(errors='replace').splitlines() if path.exists() else []
 report[name]={'first_lines':lines[:2],'last_lines':lines[-6:]}
acceptance=pathlib.Path('/home/zhangmingyu/operation/releases/ifs-0p1-20261021/acceptance.json')
report['previous_full_acceptance']=json.loads(acceptance.read_text())
with urllib.request.urlopen('http://127.0.0.1:8090/api/v1/health',timeout=10) as response:
 report['service_health']=json.load(response)
print(json.dumps(report,ensure_ascii=False))
'''


def main() -> None:
    root = Path(__file__).resolve().parent
    result = subprocess.run(
        ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8',
         'zhangmingyu@192.168.1.241', 'python3', '-'],
        input=REMOTE, text=True, capture_output=True, timeout=200,
    )
    if result.returncode:
        raise RuntimeError(result.stderr[-3000:])
    report = json.loads(result.stdout)
    listings = root / 'google-listings'
    listings.mkdir(exist_ok=True)
    for item in report['upstream']:
        if 'xml' in item:
            (listings / f"{item['date']}_{item['cycle']:02d}z.xml").write_text(item.pop('xml'))
    (root / 'inspection.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({
        'created_at': report['created_at'],
        'window': report['requested_window'],
        'available': [{k: item[k] for k in ['date', 'cycle', 'objects', 'complete']}
                      for item in report['upstream'] if item.get('objects')],
        'empty_inits': sum(item.get('empty', False) for item in report['upstream']),
        'errors': [item for item in report['upstream'] if 'error' in item and 'objects' not in item],
        'inventory': [{k: item[k] for k in ['date', 'cycle', 'files', 'bytes', 'zarr']} |
                      {'healthy': item.get('manifest', {}).get('healthy')}
                      for item in report['inventory']],
        'current_day': report['current_day'],
        'cron': report['cron'],
        'host_timezone': report['host_timezone'],
        'reconcile_timer': report['reconcile_timer'],
        'service_status': report['service_health'].get('status'),
    }, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
