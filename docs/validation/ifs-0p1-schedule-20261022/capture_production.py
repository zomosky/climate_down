"""Capture live schedules and independent Google publication timestamps."""
import json
from pathlib import Path
import subprocess


REMOTE = r'''
import concurrent.futures,datetime,hashlib,json,pathlib,re,subprocess,urllib.parse,urllib.request,xml.etree.ElementTree as ET
operation=pathlib.Path('/home/zhangmingyu/operation')
def listing(args):
 provider,day,cycle,res=args
 prefix=f'{day}/{cycle:02d}z/ifs/{res}/oper/'
 url=('https://storage.googleapis.com/ecmwf-open-data/' if provider=='google' else 'https://ecmwf-forecasts.s3.eu-central-1.amazonaws.com/')+'?'+urllib.parse.urlencode({'prefix':prefix,'max-keys':1000})
 report={'provider':provider,'date':day,'cycle':cycle,'resolution':res,'url':url}
 try:
  with urllib.request.urlopen(url,timeout=18) as response:body=response.read();report['http_status']=response.status
  root=ET.fromstring(body)
  for element in root.iter():element.tag=element.tag.rsplit('}',1)[-1]
  assert root.findtext('IsTruncated')=='false'
  objects=[{'key':x.findtext('Key'),'at':x.findtext('LastModified')} for x in root.findall('Contents')]
  files=[x for x in objects if re.search(r'-(\d+)h-oper-fc(?:-(?:sfc|pl))?\.(?:index|grib2)$',x['key'])]
  report.update(objects=len(objects),forecast_objects=len(files),first_modified=min((x['at'] for x in files),default=None),last_modified=max((x['at'] for x in files),default=None),xml_sha256=hashlib.sha256(body).hexdigest(),xml=body.decode())
 except Exception as error:report['error']=f'{type(error).__name__}: {error}'
 return report
report={'at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
tasks=[('google',day,cycle,res) for day,cycle in [('20261020',12),('20261021',0),('20261021',12)] for res in ['0p25','0p1']]
tasks += [('aws',day,12,'0p1') for day in ['20261014','20261019']]
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:report['upstream']=list(pool.map(listing,tasks))
report['cron']=subprocess.run(['crontab','-l'],capture_output=True,text=True,check=True).stdout
report['files']={name:hashlib.sha256((operation/name).read_bytes()).hexdigest() for name in ['download_run.sh','restore_scan.sh','reconcile_sources.py','recover_all_on_boot.sh','restore_ifs_0p1.py']}
report['products']=[]
for directory in sorted(pathlib.Path('/training_data/climate_data/ifs-hres-0p1').glob('2026102[012]/*z')):
 manifests=list(directory.glob('*.manifest.json'))
 if not manifests:continue
 payload=json.loads(manifests[0].read_text())
 key=f'{directory.parent.name}_{directory.name}_ifs-hres-0p1.zarr'
 metadata=pathlib.Path('/training_data/climate_data/climate_data_storage/zarr/ifs-hres-0p1')/key/'.zmetadata'
 report['products'].append({'date':directory.parent.name,'cycle':int(directory.name[:-1]),'manifest':str(manifests[0]),'manifest_sha256':hashlib.sha256(manifests[0].read_bytes()).hexdigest(),'steps':len(payload['files']),'failures':payload.get('failures',[]),'completed_at':payload.get('completed_at'),'zarr_consolidated':metadata.is_file(),'zarr_metadata_sha256':hashlib.sha256(metadata.read_bytes()).hexdigest() if metadata.is_file() else None})
report['containers']={}
for name in ['zhangmy-dev','climate-watcher']:
 data=json.loads(subprocess.run(['docker','inspect',name],text=True,capture_output=True,check=True).stdout)[0]
 report['containers'][name]={'running':data['State']['Running'],'image':data['Image'],'memory_bytes':data['HostConfig']['Memory'],'nano_cpus':data['HostConfig']['NanoCpus']}
with urllib.request.urlopen('http://127.0.0.1:8090/api/v1/health',timeout=10) as response:report['service_health']=json.load(response)
print(json.dumps(report,ensure_ascii=False))
'''


def main():
    root = Path(__file__).resolve().parent
    result = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8',
                             'zhangmingyu@192.168.1.241', 'python3', '-'],
                            input=REMOTE, text=True, capture_output=True, timeout=90)
    if result.returncode:
        raise RuntimeError(result.stderr[-2500:])
    report = json.loads(result.stdout)
    listings = root / 'listings'
    listings.mkdir(exist_ok=True)
    for row in report['upstream']:
        if 'xml' in row:
            name = f"{row['provider']}-{row['date']}_{row['cycle']:02d}z-{row['resolution']}.xml"
            (listings / name).write_text(row.pop('xml'))
    (root / 'crontab-before.txt').write_text(report.pop('cron'))
    (root / 'production-before.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({key: report[key] for key in ['at_utc', 'upstream', 'products']}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
