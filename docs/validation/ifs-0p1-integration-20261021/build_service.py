"""Build a reader-only image delta from the live image; preserve rollback."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess

REMOTE=r'''
import hashlib,json,pathlib,subprocess,time,urllib.request
root=pathlib.Path('/home/zhangmingyu/operation/releases/ifs-0p1-20261021')
context=root/'image';context.mkdir(parents=True,exist_ok=True)
def run(args,**kw):
 p=subprocess.run(args,text=True,capture_output=True,timeout=kw.pop('timeout',60),**kw)
 if p.returncode:raise RuntimeError(p.stderr[-3000:])
 return p.stdout.strip()
c=json.loads(run(['docker','inspect','climate-watcher']))[0]
old=c['Image'];rollback='climate-watcher:pre-ifs0p1-20261021';new='climate-watcher:ifs0p1-20261021'
assert old=='sha256:b18949804aeb44f44e3a9f71e3afbf12e42911a8dbb953fe75696440374ad85e', 'live image changed; re-evaluate before build'
run(['docker','tag',old,rollback])
source=pathlib.Path('/home/zhangmingyu/src/climate_pipeline/climate_zrread/src/climate_zrread/profiles')
for original,target in [('ifs_0p1.py','ifs_0p1.py'),('__init__.py','profiles_init.py')]:
 (context/target).write_bytes((source/original).read_bytes())
dockerfile=f"""FROM {rollback}
COPY ifs_0p1.py /workspace/climate_zrread/src/climate_zrread/profiles/ifs_0p1.py
COPY profiles_init.py /workspace/climate_zrread/src/climate_zrread/profiles/__init__.py
LABEL org.huamod.ifs-0p1="2026-10-21" org.huamod.ifs-0p1.profile-sha256="{hashlib.sha256((source/'ifs_0p1.py').read_bytes()).hexdigest()}"
"""
(context/'Dockerfile').write_text(dockerfile)
began=time.perf_counter()
build=subprocess.run(['docker','build','--network=none','--pull=false','-t',new,str(context)],capture_output=True,text=True,timeout=120)
(root/'image-build.log').write_text(build.stdout+build.stderr)
if build.returncode:raise RuntimeError(build.stderr[-3000:])
python='/workspace/climate_watcher/.venv/bin/python'
check="import json,hashlib,pathlib;from climate_zrread.profiles import get_profile;p=get_profile('ifs-hres-0p1');o=get_profile('ifs-hres');assert p.name=='ifs-hres-0p1' and p.solar_vars==o.solar_vars and o.availability_delay_hours==8;print(json.dumps({'new_source':p.name,'old_source':o.name,'profile_sha256':hashlib.sha256(pathlib.Path('/workspace/climate_zrread/src/climate_zrread/profiles/ifs_0p1.py').read_bytes()).hexdigest()}))"
validation=json.loads(run(['docker','run','--rm','--network=none','--cpus=1','--memory=1g','--entrypoint',python,new,'-c',check]))
image=run(['docker','image','inspect','--format','{{.Id}}',new])
report={'old_image':old,'rollback_tag':rollback,'new_image':image,'new_tag':new,'build_s':round(time.perf_counter()-began,3),'profile_check':validation}
assert not subprocess.run(['docker','container','inspect','climate-watcher-ifs0p1-canary'],capture_output=True).returncode==0, 'canary already exists; inspect before replacing'
command=['docker','run','-d','--name','climate-watcher-ifs0p1-canary','--cpus=2','--memory=4g','-p','127.0.0.1:8091:8090']
for mount in c['Mounts']:
 if mount['Destination'] in ['/climate_data','/save_data','/satelite_data','/var/lib/climate_watcher']:
  command+=['--mount',f"type=bind,source={mount['Source']},target={mount['Destination']},readonly"]
command+=['--tmpfs','/var/cache/climate_watcher:rw,size=512m','-e','TZ=Asia/Shanghai',new]
report['canary_id']=run(command)
for _ in range(30):
 try:
  with urllib.request.urlopen('http://127.0.0.1:8091/api/v1/health',timeout=3) as response:
   health=json.load(response)
  if health.get('status')=='ok':break
 except Exception:time.sleep(1)
else:raise RuntimeError('canary did not become healthy')
with urllib.request.urlopen('http://127.0.0.1:8091/',timeout=5) as response:
 assert response.status==200 and b'<html' in response.read(131072).lower()
report['canary_health']=health
(root/'service-image.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report,indent=2))
'''


def main():
    result=subprocess.run(['ssh','-o','BatchMode=yes','-o','ServerAliveInterval=10',
        'zhangmingyu@192.168.1.241','python3','-'],input=REMOTE,text=True,capture_output=True,timeout=180)
    Path(__file__).with_name('service-build-output.txt').write_text(result.stdout+result.stderr)
    if result.returncode:raise RuntimeError(result.stderr[-3000:])
    Path(__file__).with_name('service-image.json').write_text(result.stdout)
    print(result.stdout,end='')


if __name__=='__main__':main()
