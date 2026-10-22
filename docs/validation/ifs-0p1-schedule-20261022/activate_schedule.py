"""Change only the IFS0p1 cron block and install its guarded existing-runner entry."""
import argparse
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import re
import shlex
import subprocess
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET


REMOTE = r'''
import datetime,hashlib,json,os,pathlib,subprocess
payload=__PAYLOAD__
operation=pathlib.Path('/home/zhangmingyu/operation')
release=operation/'releases/ifs-0p1-schedule-20261022'
cron=subprocess.run(['crontab','-l'],capture_output=True,text=True,check=True).stdout
assert cron==payload['before_cron'],'live crontab changed since captured baseline'
begin='# BEGIN IFS-0P1 Google';end='# END IFS-0P1 Google'
assert cron.count(begin)==cron.count(end)==1
start=cron.index(begin);stop=cron.index(end)+len(end)
new=cron[:start]+payload['block'].strip()+cron[stop:]
assert new[:start]==cron[:start] and new[start+len(payload['block'].strip()):]==cron[stop:]
for name,digest in payload['before_files'].items():
 assert hashlib.sha256((operation/name).read_bytes()).hexdigest()==digest,name+' changed'
target=operation/'download_ifs_0p1.py'
code=payload['script'].encode();code_sha=hashlib.sha256(code).hexdigest()
assert not target.exists() or target.read_bytes()==code,'existing download guard differs'
acceptance=json.loads((operation/'releases/ifs-0p1-20261021/acceptance.json').read_text())
assert acceptance['status']=='ok'
report={'mode':'active' if payload['activate'] else 'preview','at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
 'cron_before_sha256':hashlib.sha256(cron.encode()).hexdigest(),'cron_after_sha256':hashlib.sha256(new.encode()).hexdigest(),
 'new_block':payload['block'],'script_sha256':code_sha,'all_other_cron_content_preserved':True,
 'existing_scripts_unchanged':True,'schedule_validation':payload['validation']}
if payload['activate']:
 release.mkdir(parents=True,exist_ok=True)
 assert not (release/'activation.json').exists(),'activation already recorded; inspect rather than repeat'
 (release/'crontab-before.txt').write_text(cron)
 (release/'download_ifs_0p1.py').write_bytes(code)
 temporary=target.with_suffix('.py.ifs0p1.tmp');temporary.write_bytes(code)
 os.chmod(temporary,0o755);os.replace(temporary,target)
 assert subprocess.run(['crontab','-l'],capture_output=True,text=True,check=True).stdout==cron,'concurrent crontab update'
 try:
  subprocess.run(['crontab','-'],input=new,capture_output=True,text=True,check=True)
  assert subprocess.run(['crontab','-l'],capture_output=True,text=True,check=True).stdout==new
  for name,digest in payload['before_files'].items():
   assert hashlib.sha256((operation/name).read_bytes()).hexdigest()==digest,name+' changed'
 except Exception:
  current=subprocess.run(['crontab','-l'],capture_output=True,text=True,check=True).stdout
  if current==new:subprocess.run(['crontab','-'],input=cron,text=True,check=True)
  raise
 (release/'crontab-after.txt').write_text(new)
 (release/'activation.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
print(json.dumps(report,ensure_ascii=False))
'''


def values(expression):
    span, _, step = expression.partition('/')
    first, _, last = span.partition('-')
    return list(range(int(first), int(last or first) + 1, int(step or 1)))


def validate_schedule(block):
    rows = [shlex.split(line) for line in block.splitlines() if line and not line.startswith('#')]
    downloads = [row for row in rows if 'download_ifs_0p1.py' in ' '.join(row)]
    assert len(rows) == 3 and len(downloads) == 2
    assert sum('restore_ifs_0p1.py' in ' '.join(row) for row in rows) == 1
    results = []
    for row in downloads:
        assert row[2:5] == ['*', '*', '*']
        cycle = int(row[row.index('--cycle') + 1])
        slots = [(hour, minute) for hour in values(row[1]) for minute in values(row[0])]
        assert len(slots) == 24
        assert slots[0] == ((4, 5) if cycle == 12 else (16, 5))
        assert slots[-1] == ((7, 55) if cycle == 12 else (19, 55))
        assert all((b[0] * 60 + b[1]) - (a[0] * 60 + a[1]) == 10 for a, b in zip(slots, slots[1:]))
        for date in ['2026-10-22', '2026-11-01', '2027-01-01']:
            for hour, minute in slots:
                local = datetime.fromisoformat(date).replace(hour=hour, minute=minute, tzinfo=ZoneInfo('Asia/Shanghai'))
                actual = local.astimezone(ZoneInfo('UTC')) - timedelta(hours=cycle + 8)
                expected = local.date() - timedelta(days=cycle == 12)
                assert actual.date() == expected
        results.append({'cycle': cycle, 'runs_per_window': len(slots), 'first_bjt': '%02d:%02d' % slots[0],
                        'last_bjt': '%02d:%02d' % slots[-1], 'gap_minutes': 10, 'validated_timestamps': len(slots) * 3})
    assert {r['cycle'] for r in results} == {0, 12}
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--activate', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    project = root.parents[2]
    docs = (project / 'deploy/IFS_0P1.md').read_text()
    block = docs.split('```cron\n', 1)[1].split('```', 1)[0].strip()
    validation = validate_schedule(block)
    tests = ET.parse(root / 'focused-tests.xml')
    suites = list(tests.getroot().iter('testsuite'))
    assert sum(int(s.attrib['tests']) for s in suites) >= 20
    assert all(int(s.attrib.get('failures', 0)) == int(s.attrib.get('errors', 0)) == 0 for s in suites)
    before = json.loads((root / 'production-before.json').read_text())
    payload = {'activate': args.activate, 'before_cron': (root / 'crontab-before.txt').read_text(),
               'before_files': before['files'], 'block': block,
               'script': (project / 'deploy/download_ifs_0p1.py').read_text(), 'validation': validation}
    result = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8',
                             'zhangmingyu@192.168.1.241', 'python3', '-'],
                            input=REMOTE.replace('__PAYLOAD__', repr(payload)),
                            text=True, capture_output=True, timeout=40)
    if result.returncode:
        raise RuntimeError(result.stderr[-2000:])
    report = json.loads(result.stdout)
    (root / ('activation.json' if args.activate else 'preview.json')).write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
