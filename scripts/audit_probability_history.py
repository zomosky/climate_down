#!/usr/bin/env python3
"""Read-only historical-init census; save metadata, per-init results and gaps.

No full forecast bundles are downloaded. Inventory includes AIFS 06/18 UTC;
field/window checks and one 16-byte GRIB header per bundle cover the current
00/12 UTC pipeline. Header checks are sampling, not full GRIB/Zarr acceptance.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import time

import httpx

from climate_download.sources._http import build_client, request_with_retry
from climate_download.sources.ecmwf_probabilities import expected_windows

API = "https://storage.googleapis.com/storage/v1/b/ecmwf-open-data/o"
ROOT = "https://storage.googleapis.com/ecmwf-open-data"
MODELS = {"ifs": (0, 12), "aifs-ens": (0, 6, 12, 18)}


def object_names(date, model, cycle):
    return {f"{date}/{cycle:02}z/{model}/0p25/enfo/{date}{cycle:02}0000-{b}h-enfo-ep.{s}"
            for b in (240, 360) for s in ("index", "grib2")}


def list_day(client, date):
    wanted = set().union(*(object_names(date, m, c) for m, cycles in MODELS.items() for c in cycles))
    params = dict(prefix=date+"/", matchGlob=date+"/*z/{ifs,aifs-ens}/0p25/enfo/*-{240,360}h-enfo-ep.{index,grib2}",
                  endOffset=date+"/18z/ifs/", maxResults=1000,
                  fields="items(name,size,timeCreated),nextPageToken")
    objects, tokens = {}, set()
    while True:
        response = request_with_retry(client, "GET", API, params=params)
        response.raise_for_status()
        data = response.json()
        objects.update({x["name"]: x for x in data.get("items", []) if x["name"] in wanted})
        token = data.get("nextPageToken")
        if wanted <= objects.keys() or not token:
            return dict(date=date, objects=list(objects.values()))
        if token in tokens:
            raise RuntimeError("repeated inventory page token")
        tokens.add(token)
        params["pageToken"] = token


def init_rows(days):
    rows = []
    for day in days:
        date = day["date"]
        objects = {x["name"]: x for x in day.get("objects", [])}
        for model, cycles in MODELS.items():
            for cycle in cycles:
                wanted = object_names(date, model, cycle)
                found = wanted & objects.keys()
                nonempty = all(int(objects[n].get("size", 0)) > 0 for n in found)
                row = dict(date=date, cycle=cycle, model=model,
                    init_utc=datetime.strptime(f"{date}{cycle:02}", "%Y%m%d%H").replace(tzinfo=timezone.utc).isoformat(),
                    pipeline_cycle=cycle in (0,12), objects_found=len(found),
                    objects_complete=len(found)==4 and nonempty and not day.get("error"),
                    missing_objects=sorted(wanted-found), inventory_error=day.get("error"))
                row["objects"] = {n: objects[n] for n in found}
                rows.append(row)
    return rows


def check_index(rows, model, bundle):
    wanted = {(p,w) for p,windows in expected_windows(model,bundle).items() for w in windows}
    selected = [r for r in rows if r.get("type")=="ep" and r.get("param") in expected_windows(model,bundle)]
    actual = [(r["param"],str(r.get("step"))) for r in selected]
    return dict(complete=set(actual)==wanted and len(actual)==len(set(actual)),
        records=len(selected), missing_parameters=sorted({p for p,w in wanted-set(actual)}),
        missing_windows=len(wanted-set(actual)), unexpected_windows=len(set(actual)-wanted)), selected


def sample_header(client, url, record):
    offset, length = int(record["_offset"]), int(record["_length"])
    for attempt in range(4):
        try:
            with client.stream("GET", url, headers={"Range":f"bytes={offset}-{offset+15}"}) as response:
                if response.status_code in (408,425,429,500,502,503,504):
                    raise httpx.TransportError(f"retryable status {response.status_code}")
                # Never consume a full bundle if Range was ignored.
                if response.status_code != 206:
                    return dict(valid=False,status=response.status_code)
                data = response.read()
                return dict(valid=len(data)==16 and data[:4]==b"GRIB" and data[7]==2
                    and int.from_bytes(data[8:16],"big")==length,
                    status=206, index_length=length,
                    grib_length=int.from_bytes(data[8:16],"big") if data[:4]==b"GRIB" else None)
        except (httpx.TransportError,httpx.TimeoutException):
            if attempt==3: raise
            time.sleep(min(0.5*2**attempt,4))


def check_init(client, row):
    result = {k:row[k] for k in ("date","cycle","model","init_utc")}
    result.update(status="checked", bundles={})
    for bundle in (240,360):
        name = f"{row['date']}/{row['cycle']:02}z/{row['model']}/0p25/enfo/{row['date']}{row['cycle']:02}0000-{bundle}h-enfo-ep"
        response = request_with_retry(client,"GET",ROOT+"/"+name+".index")
        response.raise_for_status()
        records = [json.loads(line) for line in response.text.splitlines() if line.strip()]
        info, selected = check_index(records,row["model"],bundle)
        result["bundles"][str(bundle)] = info
        if not info["complete"]:
            result["status"]="fields_or_windows_incomplete"
            return result
        size = int(row["objects"][name+".grib2"]["size"])
        if any(int(r["_offset"])<0 or int(r["_length"])<16 or int(r["_offset"])+int(r["_length"])>size for r in selected):
            result["status"]="index_out_of_bounds"
            return result
        record = next(r for r in selected if r["param"]=="tpg50")
        info["header_sample"] = sample_header(client,ROOT+"/"+name+".grib2",record)
        if not info["header_sample"]["valid"]:
            result["status"]="index_or_header_mismatch"
            return result
    return result


def intervals(times):
    dates = sorted(datetime.fromisoformat(t) for t in times)
    out=[]
    for t in dates:
        if not out or t-out[-1][1] != timedelta(hours=12):out.append([t,t])
        else:out[-1][1]=t
    return [[a.isoformat(),b.isoformat()] for a,b in out]


def summarize(rows, checks):
    by_init={(x['model'],x['init_utc']):x for x in checks}
    result={}
    for model in MODELS:
        found=sorted((r for r in rows if r['model']==model and r['pipeline_cycle'] and r['objects_complete']),key=lambda r:r['init_utc'])
        first,last=(found[0]['init_utc'],found[-1]['init_utc']) if found else (None,None)
        expected=[r for r in rows if r['model']==model and r['pipeline_cycle'] and first and first<=r['init_utc']<=last]
        matched=[by_init[(model,r['init_utc'])] for r in found if (model,r['init_utc']) in by_init]
        good=[r['init_utc'] for r in matched if r['status']=='checked']
        missing=[dict(init_utc=r['init_utc'],missing_objects=r['missing_objects'],error=r['inventory_error']) for r in expected if not r['objects_complete']]
        result[model]=dict(first_object_complete_init=first,last_object_complete_init=last,
            pipeline_object_complete_inits=len(found), pipeline_expected_between_first_last=len(expected),
            pipeline_object_gaps=missing, inventory_error_days=sorted({r['date'] for r in rows if r['model']==model and r['inventory_error']}),
            index_header_checked_inits=len(matched),statuses=dict(Counter(r['status'] for r in matched)),
            index_header_pass_intervals_12h=intervals(good),
            additional_cycles_object_complete={str(c):sum(r['model']==model and r['cycle']==c and r['objects_complete'] for r in rows) for c in (6,18)})
    return result


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start',default='20240101')
    parser.add_argument('--end',required=True)
    parser.add_argument('--out',type=Path,required=True)
    parser.add_argument('--workers',type=int,default=8)
    parser.add_argument('--phase',choices=['inventory','validate','all'],default='all')
    args=parser.parse_args(argv)
    if not 1<=args.workers<=12:parser.error('workers must be 1..12')
    start,end=(datetime.strptime(x,'%Y%m%d') for x in [args.start,args.end])
    if end<start:parser.error('end before start')
    args.out.mkdir(parents=True,exist_ok=True)
    begun=datetime.now(timezone.utc).isoformat()
    def read_cache(path,key):
        return {key(r):r for line in path.read_text().splitlines() if line.strip() for r in [json.loads(line)] if not r.get('error')} if path.exists() else {}
    inventory_path,checks_path=args.out/'inventory-days.jsonl',args.out/'init-checks.jsonl'
    cached=read_cache(inventory_path,lambda r:r['date'])
    dates=[(start+timedelta(days=n)).strftime('%Y%m%d') for n in range((end-start).days+1)]
    with build_client(timeout=30) as client:
        if args.phase in ('inventory','all'):
            todo=[d for d in dates if d not in cached]
            with ThreadPoolExecutor(max_workers=args.workers) as pool, inventory_path.open('a') as output:
                jobs={pool.submit(list_day,client,d):d for d in todo}
                for i,future in enumerate(as_completed(jobs),1):
                    try:r=future.result()
                    except Exception as e:r=dict(date=jobs[future],error=f'{type(e).__name__}: {e}')
                    cached[r['date']]=r;output.write(json.dumps(r)+'\n');output.flush()
                    if i%50==0 or i==len(jobs):print(json.dumps(dict(phase='inventory',completed=i,total=len(jobs))),flush=True)
        days=[cached.get(d,dict(date=d,error='not inventoried')) for d in dates]
        rows=init_rows(days)
        checked=read_cache(checks_path,lambda r:(r['model'],r['init_utc']))
        if args.phase in ('validate','all'):
            todo=[r for r in rows if r['pipeline_cycle'] and r['objects_complete'] and (r['model'],r['init_utc']) not in checked]
            with ThreadPoolExecutor(max_workers=args.workers) as pool, checks_path.open('a') as output:
                jobs={pool.submit(check_init,client,r):r for r in todo}
                for i,future in enumerate(as_completed(jobs),1):
                    try:r=future.result()
                    except Exception as e:
                        r={k:jobs[future][k] for k in ('date','cycle','model','init_utc')}
                        r.update(status='request_or_parse_error',error=f'{type(e).__name__}: {e}')
                    checked[(r['model'],r['init_utc'])]=r;output.write(json.dumps(r)+'\n');output.flush()
                    if i%50==0 or i==len(jobs):print(json.dumps(dict(phase='validate',completed=i,total=len(jobs))),flush=True)
    checks=[checked[(r['model'],r['init_utc'])] for r in rows if (r['model'],r['init_utc']) in checked]
    summary=dict(start=args.start,end=args.end,started_at_utc=begun,finished_at_utc=datetime.now(timezone.utc).isoformat(),
        scope='0.25 degree ep bundles 240/360; pipeline 00/12 UTC; extra 06/18 inventory only',
        verification='all requested index fields/windows plus one GRIB2 16-byte header/length per bundle; not full decode or Zarr acceptance',
        sources=summarize(rows,checks))
    (args.out/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n')
    with (args.out/'historical-inits.csv').open('w',newline='') as output:
        writer=csv.DictWriter(output,fieldnames=['model','init_utc','pipeline_cycle','objects_found','objects_complete','validation_status','missing_objects','error'])
        writer.writeheader()
        for r in rows:
            check=checked.get((r['model'],r['init_utc']),{})
            writer.writerow(dict(model=r['model'],init_utc=r['init_utc'],pipeline_cycle=r['pipeline_cycle'],objects_found=r['objects_found'],objects_complete=r['objects_complete'],validation_status=check.get('status','not_checked'),missing_objects=json.dumps(r['missing_objects']),error=r['inventory_error'] or check.get('error','')))
    print(json.dumps(summary,ensure_ascii=False),flush=True)
    return 0


if __name__=='__main__':
    raise SystemExit(main())
