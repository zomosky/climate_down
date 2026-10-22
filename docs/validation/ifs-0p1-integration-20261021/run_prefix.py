"""A three-lead restore/value check outside the publication roots."""
from pathlib import Path
import subprocess

REMOTE=r'''
from pathlib import Path
import subprocess,sys,os
root=Path('/logs/ifs-0p1-integration-20261021')
old=root/'prefix-fixed.zarr'
if old.exists():
    preserved=root/'prefix-coordinate-only.zarr'
    if preserved.exists():raise RuntimeError('refusing to overwrite prior prefix evidence')
    old.rename(preserved)
(root/'audit_native.py').write_text(__AUDIT__)
code=r"""
import json,time
from pathlib import Path
from climate_restore.config import ZarrOptions
from climate_restore.processor import process_init
from climate_restore.sources import get_source
root=Path('/logs/ifs-0p1-integration-20261021')
paths=[Path('/climate_data/ifs-hres-0p1/20261020/12z')/f'f{s:03d}.subset.grib2' for s in [0,3,6]]
assert all(p.exists() for p in paths)
start=time.perf_counter()
variables=process_init(paths,adapter=get_source('ifs-hres-0p1')(),bbox=(70,140,15,55),
 out_path=root/'prefix-fixed.zarr',workers=2,output_format='zarr',zarr_options=ZarrOptions())
(root/'prefix-restore-fixed.json').write_text(json.dumps({'variables':variables,'seconds':time.perf_counter()-start})+'\n')
"""
with (root/'prefix-restore.log').open('w') as log:
 result=subprocess.run(['/workspace/climate_restorage/.venv/bin/python','-c',code],stdout=log,stderr=subprocess.STDOUT)
if result.returncode:raise RuntimeError((root/'prefix-restore.log').read_text()[-2000:])
result=subprocess.run(['/workspace/climate_restorage/.venv/bin/python',str(root/'audit_native.py'),
 '--date','20261020','--cycle','12','--steps','0,3,6','--store',str(root/'prefix-fixed.zarr'),
 '--output',str(root/'prefix-native-audit.json')],text=True,capture_output=True)
print(result.stdout)
if result.returncode:raise RuntimeError(result.stderr[-3000:])
'''

if __name__=='__main__':
    script=REMOTE.replace('__AUDIT__',repr(Path(__file__).with_name('audit_native.py').read_text()))
    result=subprocess.run(['ssh','-o','BatchMode=yes','-o','ServerAliveInterval=10','zhangmingyu@192.168.1.241',
        'docker','exec','-i','zhangmy-dev','/workspace/climate_restorage/.venv/bin/python','-'],
        input=script,text=True,capture_output=True,timeout=240)
    Path(__file__).with_name('prefix-output.txt').write_text(result.stdout+result.stderr)
    if result.returncode:raise RuntimeError(result.stderr[-3000:])
    print(result.stdout)
