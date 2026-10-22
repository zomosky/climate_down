"""Final readback of the live service, old query, schedules and idle scanner."""
from pathlib import Path
import json
import subprocess

REMOTE = r'''
from pathlib import Path
import datetime,hashlib,json,math,subprocess,urllib.request,shutil
release=Path('/home/zhangmingyu/operation/releases/ifs-0p1-20261021')
operation=release.parents[1]
report={'checked_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'checks':{}}
def run(command):
    result=subprocess.run(command,text=True,capture_output=True,timeout=45)
    if result.returncode:raise RuntimeError((result.stdout+result.stderr)[-2400:])
    return result.stdout
spec={'source':'ifs-hres','variables':['t2m','u100','ssrd_accum'],'time':{'type':'operation','start':'2026-10-21T00:00:00Z','horizon_hours':24,'tz':'UTC','init_date':'2026-10-20','init_cycle':12},'space':{'kind':'sites','sites':[{'id':'cd','lat':30.67,'lon':104.06}]},'postprocess':{'resample':{'freq':'15min'}}}
request=urllib.request.Request('http://127.0.0.1:8090/api/v1/query',data=json.dumps(spec).encode(),headers={'Content-Type':'application/json'})
with urllib.request.urlopen(request,timeout=130) as response:data=json.load(response)
digest=hashlib.sha256(json.dumps({key:data[key] for key in ['index','columns','values']},sort_keys=True).encode()).hexdigest()
expected=json.loads((release/'service-baseline.json').read_text())['production_before']['sha256']
assert digest==expected,'old0p25 query changed'
report['checks']['old_025_query']={'unchanged':True,'rows':len(data['index']),'sha256':digest}
containers={}
for name,memory,cores in [('zhangmy-dev',36*1024**3,18),('climate-watcher',24*1024**3,12)]:
    container=json.loads(run(['docker','inspect',name]))[0]
    host=container['HostConfig']
    actual=host['NanoCpus']/1e9 if host['NanoCpus'] else host['CpuQuota']/host['CpuPeriod']
    assert host['Memory']==memory and math.isclose(actual,cores,abs_tol=2e-6)
    containers[name]={'memory_bytes':memory,'nano_cpus':host['NanoCpus'],
                      'effective_cpu_limit':actual,'image':container['Image'],
                      'running':container['State']['Running'],'restart_count':container['RestartCount']}
report['checks']['container_limits_unchanged']=containers
with urllib.request.urlopen('http://127.0.0.1:8090/api/v1/health',timeout=5) as response:health=json.load(response)
assert health['status']=='ok' and health['checks']['homepage']['status']=='ok'
report['checks']['health']=health
cron=run(['crontab','-l'])
block=json.loads((release/'operations-activation.json').read_text())['new_cron_block'].strip()
assert block in cron
assert cron.replace(block,'').rstrip()+'\n'==(release/'crontab-before.txt').read_text().rstrip()+'\n'
report['checks']['scheduler']={'active':True,'all_prior_cron_lines_preserved':True,'new_entries':5}
report['checks']['restore_guard_dry_run']=json.loads(run(['python3',str(operation/'restore_ifs_0p1.py'),'--dry-run']))
reconcile=run(['python3',str(operation/'reconcile_sources.py'),'--only-source','ifs-hres-0p1','--dry-run','--now','2026-10-21T10:00:00Z','--lock',str(release/'verify-reconcile.lock')])
assert 'HEALTHY ifs-hres-0p1 20261020 12z' in reconcile and 'REPAIR' not in reconcile
report['checks']['reconcile']=reconcile.strip()
store=Path('/training_data/climate_data/climate_data_storage/zarr/ifs-hres-0p1/20261020_12z_ifs-hres-0p1.zarr')
before=(store/'.zmetadata').stat().st_mtime_ns
scan=run(['python3',str(operation/'restore_ifs_0p1.py')])
assert (store/'.zmetadata').stat().st_mtime_ns==before
report['checks']['real_restore_scanner_noop']={'returncode':0,'metadata_mtime_unchanged':True,'output':scan[-2500:]}
report['checks']['zarr_bytes']=sum(path.stat().st_size for path in store.rglob('*') if path.is_file())
report['checks']['disk_free_bytes']=shutil.disk_usage('/training_data/climate_data').free
report['status']='ok'
(release/'runtime-verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
print(json.dumps(report,ensure_ascii=False,indent=2))
'''


def main():
    result=subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=8',
                           'zhangmingyu@192.168.1.241','python3','-'],
                          input=REMOTE,text=True,capture_output=True,timeout=150)
    if result.returncode:raise RuntimeError(result.stderr[-3000:])
    Path(__file__).with_name('runtime-verification.json').write_text(result.stdout)
    value=json.loads(result.stdout)
    print(json.dumps({'status':value['status'],
                      'old_025_query':value['checks']['old_025_query'],
                      'scheduler':value['checks']['scheduler'],
                      'reconcile':value['checks']['reconcile'],
                      'scanner':value['checks']['real_restore_scanner_noop'],
                      'zarr_GiB':round(value['checks']['zarr_bytes']/1024**3,3)},
                     ensure_ascii=False,indent=2))


if __name__=='__main__':main()
