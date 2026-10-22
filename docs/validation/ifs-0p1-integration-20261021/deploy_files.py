"""Deploy only the explicitly listed 0p1 additions, with hashes and rollback copies."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess

REMOTE = r'''
import hashlib,json,os
from pathlib import Path
payload = __PAYLOAD__
release = Path(payload['release'])
pending=[]
for item in payload['files']:
    path=Path(item['target'])
    old=path.read_bytes() if path.exists() else None
    if item.get('append'):
        assert old is not None, str(path)
        addition=item['text'].encode()
        new=old if addition.strip() in old else old.rstrip()+b'\n'+addition
    else:
        new=item['text'].encode()
        if old is not None and old!=new:
            if hashlib.sha256(old).hexdigest()!=item.get('expected_before_sha256'):
                raise RuntimeError('refusing to replace a different existing new-source file: '+str(path))
    pending.append((item,path,old,new))
receipt=[]
for item,path,old,new in pending:
    if old!=new:
        if old is not None:
            backup=release/'backups'/str(path).lstrip('/')
            backup.parent.mkdir(parents=True,exist_ok=True)
            if not backup.exists():backup.write_bytes(old)
        path.parent.mkdir(parents=True,exist_ok=True)
        temporary=path.with_name(path.name+'.ifs-0p1.tmp')
        temporary.write_bytes(new)
        if path.exists():os.chmod(temporary,path.stat().st_mode & 0o777)
        os.replace(temporary,path)
    receipt.append({'path':str(path),'before_sha256':hashlib.sha256(old).hexdigest() if old is not None else None,'after_sha256':hashlib.sha256(new).hexdigest(),'changed':old!=new})
release.mkdir(parents=True,exist_ok=True)
out=release/(payload['phase']+'-deployment.json')
out.write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps({'phase':payload['phase'],'receipt':receipt,'receipt_path':str(out)},indent=2))
'''


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('phase',choices=['download','restore','reader'])
    args=parser.parse_args()
    root=Path(__file__).resolve().parents[4]
    definitions={
        'download':('download','/workspace/climate_down',[
            'src/climate_download/sources/ifs_split.py',
            'config/sources/ifs_0p1.yaml','config/jobs/ifs_0p1_renewables_ser.yaml'],
            'src/climate_download/sources/__init__.py',
            'from climate_download.sources import ifs_split as _ifs_split  # noqa: F401\n'),
        'restore':('climate_restorage','/workspace/climate_restorage',[
            'src/climate_restore/sources/ifs_0p1.py','config/jobs/ifs_0p1_zarr_china_ser.yaml'],
            'src/climate_restore/sources/__init__.py',
            'from climate_restore.sources import ifs_0p1 as _ifs_0p1  # noqa: F401\n'),
        'reader':('climate_zrread','/home/zhangmingyu/src/climate_pipeline/climate_zrread',[
            'src/climate_zrread/profiles/ifs_0p1.py'],
            'src/climate_zrread/profiles/__init__.py',
            'from climate_zrread.profiles import ifs_0p1 as _ifs_0p1  # noqa: F401\n')}
    project,target,files,init,addition=definitions[args.phase]
    payload={'phase':args.phase,'files':[{'target':target+'/'+p,'text':(root/project/p).read_text()} for p in files]}
    prior=Path(__file__).with_name(args.phase+'-deployment.json')
    if prior.exists():
        previous={entry['path']:entry['after_sha256'] for entry in json.loads(prior.read_text())['receipt']}
        for entry in payload['files']:
            entry['expected_before_sha256']=previous.get(entry['target'])
    payload['release']=('/logs/ifs-0p1-release-20261021' if args.phase!='reader'
                        else '/home/zhangmingyu/operation/releases/ifs-0p1-20261021')
    payload['files'].append({'target':target+'/'+init,'text':addition,'append':True})
    script=REMOTE.replace('__PAYLOAD__',repr(payload))
    command=['ssh','-o','BatchMode=yes','-o','ConnectTimeout=8','zhangmingyu@192.168.1.241']
    if args.phase!='reader':
        command+=['docker','exec','-i','zhangmy-dev','/workspace/climate_reanalysis/.venv/bin/python','-']
    else:
        command+=['python3','-']
    result=subprocess.run(command,input=script,text=True,capture_output=True,timeout=40)
    if result.returncode:raise RuntimeError(result.stderr[-3000:])
    output=Path(__file__).with_name(args.phase+'-deployment.json')
    output.write_text(result.stdout)
    print(result.stdout,end='')


if __name__=='__main__':main()
