"""Read back the new cron and execute the deployed guard against completed inits."""
import json
from pathlib import Path
import subprocess


REMOTE = r'''
import datetime,hashlib,json,pathlib,subprocess,urllib.request
baseline=__BASELINE__
activation=__ACTIVATION__
operation=pathlib.Path('/home/zhangmingyu/operation')
report={'checked_at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
cron=subprocess.run(['crontab','-l'],capture_output=True,text=True,check=True).stdout
assert hashlib.sha256(cron.encode()).hexdigest()==activation['cron_after_sha256']
assert activation['new_block'] in cron
assert hashlib.sha256((operation/'download_ifs_0p1.py').read_bytes()).hexdigest()==activation['script_sha256']
for name,digest in baseline['files'].items():
 assert hashlib.sha256((operation/name).read_bytes()).hexdigest()==digest,name+' changed'
report['scheduler']={'installed':True,'cron_sha256':activation['cron_after_sha256'],'existing_scripts_unchanged':True}
report['actual_guard_calls']=[]
for cycle in [12,0]:
 result=subprocess.run(['python3',str(operation/'download_ifs_0p1.py'),'--cycle',str(cycle)],capture_output=True,text=True,timeout=15)
 assert result.returncode==0,result.stderr
 output=json.loads(result.stdout)
 assert output['event']=='ifs0p1_skip_healthy',output
 report['actual_guard_calls'].append(output)
result=subprocess.run(['python3',str(operation/'download_ifs_0p1.py'),'--cycle','0','--now','2026-10-22T16:05:00+08:00','--dry-run'],capture_output=True,text=True,timeout=15)
assert result.returncode==0,result.stderr
output=json.loads(result.stdout)
assert output['date']=='20261022' and output['cycle']==0 and output['dry_run']
report['early_window_dry_run']=output
report['products_unchanged']=[]
for product in baseline['products']:
 path=pathlib.Path(product['manifest'])
 assert hashlib.sha256(path.read_bytes()).hexdigest()==product['manifest_sha256']
 key=f"{product['date']}_{product['cycle']:02d}z_ifs-hres-0p1.zarr"
 marker=pathlib.Path('/training_data/climate_data/climate_data_storage/zarr/ifs-hres-0p1')/key/'.zmetadata'
 assert marker.is_file() and hashlib.sha256(marker.read_bytes()).hexdigest()==product['zarr_metadata_sha256']
 report['products_unchanged'].append({'date':product['date'],'cycle':product['cycle'],'steps':product['steps'],'raw_manifest_unchanged':True,'zarr_metadata_unchanged':True})
report['containers']={}
for name,before in baseline['containers'].items():
 data=json.loads(subprocess.run(['docker','inspect',name],capture_output=True,text=True,check=True).stdout)[0]
 current={'running':data['State']['Running'],'image':data['Image'],'memory_bytes':data['HostConfig']['Memory'],'nano_cpus':data['HostConfig']['NanoCpus']}
 assert current==before,name+' changed'
 report['containers'][name]=current
def query(spec):
 request=urllib.request.Request('http://127.0.0.1:8090/api/v1/query',data=json.dumps(spec).encode(),headers={'Content-Type':'application/json'})
 with urllib.request.urlopen(request,timeout=30) as response:payload=json.load(response)
 return {'rows':len(payload['index']),'sha256':hashlib.sha256(json.dumps({k:payload[k] for k in ['index','columns','values']},sort_keys=True).encode()).hexdigest()}
old={'source':'ifs-hres','variables':['t2m','u100','ssrd_accum'],'time':{'type':'operation','start':'2026-10-21T00:00:00Z','horizon_hours':24,'tz':'UTC','init_date':'2026-10-20','init_cycle':12},'space':{'kind':'sites','sites':[{'id':'cd','lat':30.67,'lon':104.06}]},'postprocess':{'resample':{'freq':'15min'}}}
report['original_025_query']=query(old)
assert report['original_025_query']['sha256']=='409b5aff844303ed7023f689ce592e5e859d52c405cc6468f9ae1275431a0321'
new={'source':'ifs-hres-0p1','variables':['t2m','u100','v100','ssrd_accum','t_850'],'time':{'type':'operation','start':'2026-10-20T18:00:00+00:00','horizon_hours':24,'tz':'UTC','init_date':'2026-10-20','init_cycle':12},'space':{'kind':'sites','sites':[{'id':'exact_attempt2','lat':35.0,'lon':116.0}],'method':'nearest'},'postprocess':{'deaccumulate':False}}
report['new_01_query']=query(new)
assert report['new_01_query']['sha256']=='bef12a6f32ecec8b4507d506c7f766098e6ba435bdabce181ca991582f8d25fb'
with urllib.request.urlopen('http://127.0.0.1:8090/api/v1/health',timeout=10) as response:report['health']=json.load(response)
assert report['health']['status']=='ok'
report['status']='ok'
(operation/'releases/ifs-0p1-schedule-20261022/verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
print(json.dumps(report,ensure_ascii=False))
'''


def main():
    root = Path(__file__).resolve().parent
    source = REMOTE.replace('__BASELINE__', repr(json.loads((root / 'production-before.json').read_text())))
    source = source.replace('__ACTIVATION__', repr(json.loads((root / 'activation.json').read_text())))
    result = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8',
                             'zhangmingyu@192.168.1.241', 'python3', '-'],
                            input=source, text=True, capture_output=True, timeout=100)
    if result.returncode:
        raise RuntimeError(result.stderr[-2500:])
    report = json.loads(result.stdout)
    (root / 'verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({key: report[key] for key in ['status', 'scheduler', 'actual_guard_calls',
                     'early_window_dry_run', 'products_unchanged', 'original_025_query', 'new_01_query']},
                     ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
