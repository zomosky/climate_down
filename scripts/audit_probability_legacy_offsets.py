#!/usr/bin/env python3
"""Analysis only: check whether complete old AIFS indexes can be rebased.

No production adapter changes. Require index lengths to cover the entire
object, then sample corrected headers in both bundles. This is not full decode.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timedelta,timezone
import argparse,json
from pathlib import Path

from audit_probability_history import ROOT,build_client,request_with_retry,check_index,sample_header


def corrected_records(records, size):
    offset=0;corrected=[]
    for record in records:
        length=int(record['_length'])
        if length<16:raise ValueError('invalid message length')
        corrected.append({**record,'_offset':offset})
        offset+=length
    if offset!=size:raise ValueError('index message lengths do not cover whole object')
    return corrected


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--out',type=Path,required=True)
    args=p.parse_args()
    days={r['date']:r for l in (args.out/'inventory-days.jsonl').read_text().splitlines() if l.strip() for r in [json.loads(l)]}
    start,end=datetime(2026,5,12,12),datetime(2026,6,23,0)
    targets=[start+timedelta(hours=12*i) for i in range(int((end-start).total_seconds()/43200)+1)]
    with build_client(timeout=30) as client:
        def check(t):
            date,cycle=t.strftime('%Y%m%d'),t.hour
            out=dict(init_utc=t.replace(tzinfo=timezone.utc).isoformat(),status='rebased_headers_pass',bundles={})
            objects={r['name']:r for r in days[date]['objects']}
            try:
                for bundle in (240,360):
                    name=f'{date}/{cycle:02}z/aifs-ens/0p25/enfo/{date}{cycle:02}0000-{bundle}h-enfo-ep'
                    response=request_with_retry(client,'GET',ROOT+'/'+name+'.index');response.raise_for_status()
                    records=[json.loads(l) for l in response.text.splitlines() if l.strip()]
                    rebased=corrected_records(records,int(objects[name+'.grib2']['size']))
                    info,selected=check_index(rebased,'aifs-ens',bundle)
                    out['bundles'][str(bundle)]=info
                    if not info['complete']:
                        out['status']='fields_or_windows_incomplete';return out
                    chosen=[r for r in selected if r['param']=='tpg50']
                    samples=[sample_header(client,ROOT+'/'+name+'.grib2',r) for r in [chosen[0],chosen[-1]]]
                    info['corrected_header_samples']=samples
                    if not all(x['valid'] for x in samples):
                        out['status']='rebased_header_mismatch';return out
            except Exception as exc:out.update(status='error',error=f'{type(exc).__name__}: {exc}')
            return out
        with ThreadPoolExecutor(max_workers=4) as pool,(args.out/'legacy-offset-checks.jsonl').open('w') as report:
            for i,row in enumerate(pool.map(check,targets),1):
                report.write(json.dumps(row)+'\n');report.flush()
                if i%20==0 or i==len(targets):print(json.dumps(dict(phase='legacy_offsets',completed=i,total=len(targets))),flush=True)


if __name__=='__main__':main()
