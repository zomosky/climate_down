"""Activate only the independently accepted 0p1 schedule, preserving other jobs."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess

REMOTE = r'''
import datetime as dt,hashlib,json,os,subprocess
from pathlib import Path
payload=__PAYLOAD__
operation=Path('/home/zhangmingyu/operation')
release=operation/'releases/ifs-0p1-20261021'
logroot=Path('/home/zhangmingyu/dev-docker/logs/ifs-0p1-integration-20261021')
cron=subprocess.run(['crontab','-l'],text=True,capture_output=True,check=True).stdout
begin='# BEGIN IFS-0P1 Google';end='# END IFS-0P1 Google'
if begin in cron or end in cron:
    assert cron.count(begin)==cron.count(end)==1,'ambiguous existing0p1 block'
    before,tail=cron.split(begin,1);_,after=tail.split(end,1)
    cron_without=before.rstrip()+'\n'+after.lstrip('\n')
else:
    assert not any('ifs_0p1' in line or 'ifs-hres-0p1' in line for line in cron.splitlines()),'unmarked0p1 scheduler already exists'
    cron_without=cron
newcron=cron_without.rstrip()+'\n\n'+payload['cron'].strip()+'\n'
changes=[]
for name,content in payload['files'].items():
    path=operation/name;old=path.read_bytes() if path.exists() else None;new=content.encode()
    if old!=new:
        expected=payload['baseline'].get(name)
        assert (hashlib.sha256(old).hexdigest() if old is not None else None)==expected,'unexpected live content: '+name
    changes.append((name,path,old,new))
acceptance={}
for name in ['20261020_12z.json','20261020_12z-native-audit.json']:
    path=logroot/name
    if path.exists():acceptance[name]=json.loads(path.read_text())
api_path='/tmp/ifs-0p1-integration-20261021/20261020_12z/api-acceptance.json'
probe=subprocess.run(['docker','exec','climate-watcher','cat',api_path],text=True,capture_output=True)
if probe.returncode==0:acceptance['api']=json.loads(probe.stdout)
ready=(acceptance.get('20261020_12z.json',{}).get('status')=='produced'
       and acceptance.get('20261020_12z-native-audit.json',{}).get('status')=='ok'
       and acceptance.get('20261020_12z-native-audit.json',{}).get('files')==85
       and acceptance.get('api',{}).get('status')=='ok')
if not payload['activate']:
    print(json.dumps({'mode':'preview','acceptance_ready':ready,'files':[x[0] for x in changes],
                      'added_cron_lines':len(payload['cron'].splitlines()),'crontab':newcron},indent=2));raise SystemExit(0)
assert ready,'refusing schedule activation before full native/API acceptance'
assert subprocess.run(['crontab','-l'],text=True,capture_output=True,check=True).stdout==cron,'crontab changed during preparation'
release.mkdir(parents=True,exist_ok=True)
backup=release/'crontab-before.txt'
if not backup.exists():backup.write_text(cron)
file_receipts=[]
for name,path,old,new in changes:
    if old!=new:
        if old is not None:
            backup=release/('before-'+name)
            if not backup.exists():backup.write_bytes(old)
        temporary=path.with_name(path.name+'.ifs-0p1.tmp');temporary.write_bytes(new)
        os.chmod(temporary,0o755);os.replace(temporary,path)
    file_receipts.append({'name':name,'before_sha256':hashlib.sha256(old).hexdigest() if old is not None else None,
                          'after_sha256':hashlib.sha256(new).hexdigest(),'changed':old!=new})
subprocess.run(['crontab','-'],input=newcron,text=True,check=True)
verified=subprocess.run(['crontab','-l'],text=True,capture_output=True,check=True).stdout
assert verified==newcron,'crontab readback differs'
for name in ['download_run.sh','restore_scan.sh','recover_all_on_boot.sh']:
    assert hashlib.sha256((operation/name).read_bytes()).hexdigest()==payload['baseline'][name],name+' changed'
receipt={'status':'active','at_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'files':file_receipts,
         'cron_before_sha256':hashlib.sha256(cron.encode()).hexdigest(),
         'cron_after_sha256':hashlib.sha256(newcron.encode()).hexdigest(),
         'new_cron_block':payload['cron'],'all_unrelated_cron_lines_preserved':True,
         'existing_runner_hashes_unchanged':True,'acceptance_ready':ready}
(release/'operations-activation.json').write_text(json.dumps(receipt,indent=2)+'\n')
(release/'crontab-after.txt').write_text(newcron)
print(json.dumps(receipt,indent=2))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--activate', action='store_true')
    args = parser.parse_args()
    directory = Path(__file__).resolve().parent
    project = directory.parents[2]
    docs = (project/'deploy/IFS_0P1.md').read_text()
    cron = docs.split('```cron\n', 1)[1].split('```', 1)[0]
    before = json.loads((directory/'operations-before.json').read_text())
    payload = {'activate': args.activate, 'cron': cron,
               'files': {name: (project/'deploy'/name).read_text()
                         for name in ['reconcile_sources.py', 'restore_ifs_0p1.py']},
               'baseline': {name: item['sha256'] for name,item in before['files'].items()}}
    result = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8',
                             'zhangmingyu@192.168.1.241', 'python3', '-'],
                            input=REMOTE.replace('__PAYLOAD__', repr(payload)),
                            text=True, capture_output=True, timeout=35)
    if result.returncode:
        raise RuntimeError(result.stderr[-2400:])
    target = directory/('operations-activation.json' if args.activate else 'operations-preview.json')
    target.write_text(result.stdout)
    value = json.loads(result.stdout)
    print(json.dumps({name:item for name,item in value.items() if name!='crontab'}, indent=2))


if __name__ == '__main__':
    main()
