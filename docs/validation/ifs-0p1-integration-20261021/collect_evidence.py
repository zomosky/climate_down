"""Copy only this run's small receipts/logs/exports; leave raw and Zarr on241."""
from __future__ import annotations

import base64
import gzip
import hashlib
import json
from pathlib import Path
import subprocess

REMOTE = r'''
import base64,gzip,hashlib,json,subprocess
from pathlib import Path
root=Path('/home/zhangmingyu/dev-docker/logs/ifs-0p1-integration-20261021')
release=Path('/home/zhangmingyu/operation/releases/ifs-0p1-20261021')
acceptance=json.loads((release/'acceptance.json').read_text())
assert acceptance['status']=='ok','full acceptance is not complete'
files={}
def add(name,data):
    assert len(data)<10*1024**2,name+' too large for receipt collection'
    files[name]={'sha256':hashlib.sha256(data).hexdigest(),'size':len(data),
                 'gzip_base64':base64.b64encode(gzip.compress(data,mtime=0)).decode()}
for name in ['20261020_12z.json','20261020_12z-native-audit.json','prefix-native-audit.json',
             '20261020_12z-download.log','20261020_12z-restore.log','finish-first.log',
             'resource-samples.jsonl','resource-sampling.json']:
    path=root/name
    if path.exists():add('production/'+name,path.read_bytes())
manifest=Path('/training_data/climate_data/ifs-hres-0p1/20261020/12z/20261020_12z_ifs-hres-0p1.manifest.json')
add('production/'+manifest.name,manifest.read_bytes())
for name in ['acceptance.json','acceptance-attempt1.json','acceptance-attempt2.json',
             'acceptance-launch.json','acceptance-retry-launch.json','acceptance-retry3-launch.json',
             'service-baseline.json','service-activation.json',
             'service-image.json','production_after-025-query.json','operations-activation.json',
             '20261020_12z-api_audit.log','20261020_12z-native_audit.log',
             '20261020_12z-api_audit-attempt1.log','20261020_12z-api_audit-attempt2.log']:
    path=release/name
    if path.exists():add('host/'+name,path.read_bytes())
api=r"""
from pathlib import Path
import base64,json
root=Path('/tmp/ifs-0p1-integration-20261021/20261020_12z')
allowed={'.json','.nc','.csv','.parquet','.arrow'}
result={}
for path in root.iterdir():
 if path.is_file() and path.suffix in allowed:
  assert path.stat().st_size<10*1024**2,path.name
  result[path.name]=base64.b64encode(path.read_bytes()).decode()
print(json.dumps(result))
"""
result=subprocess.run(['docker','exec','-i','climate-watcher','/workspace/climate_watcher/.venv/bin/python','-'],
                      input=api,text=True,capture_output=True,check=True)
for name,data in json.loads(result.stdout).items():add('api/'+name,base64.b64decode(data))
print(json.dumps(files))
'''


def main():
    result = subprocess.run(['ssh','-o','BatchMode=yes','-o','ConnectTimeout=8',
                             'zhangmingyu@192.168.1.241','python3','-'],
                            input=REMOTE,text=True,capture_output=True,timeout=40)
    if result.returncode:
        raise RuntimeError(result.stderr[-1500:])
    root = Path(__file__).resolve().parent/'evidence'
    manifest = {}
    for name,item in json.loads(result.stdout).items():
        assert '..' not in Path(name).parts and not Path(name).is_absolute()
        data = gzip.decompress(base64.b64decode(item['gzip_base64']))
        assert hashlib.sha256(data).hexdigest()==item['sha256']
        target = root/name
        target.parent.mkdir(parents=True,exist_ok=True)
        target.write_bytes(data)
        manifest[name] = {key:item[key] for key in ['sha256','size']}
    (root/'SHA256SUMS.json').write_text(json.dumps(manifest,indent=2)+'\n')
    print(json.dumps({'files':len(manifest),'bytes':sum(item['size'] for item in manifest.values()),
                      'all_transfer_hashes_ok':True,'output':str(root)},indent=2))


if __name__=='__main__':
    main()
