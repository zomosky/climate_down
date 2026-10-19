#!/usr/bin/env python3
"""Inventory old pre-model-directory IFS ep packages, preserving exact object paths."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import json
from pathlib import Path

from climate_download.sources._http import build_client, request_with_retry


def day(date):
    params=dict(prefix=date+'/',matchGlob=date+'/*z/{0p25,0p4-beta}/enfo/*-{240,360}h-enfo-ep.{index,grib2}',
        fields='items(name,size,timeCreated),nextPageToken',maxResults=1000)
    result=[]
    with build_client(timeout=90) as client:
        while True:
            response=request_with_retry(client,'GET','https://storage.googleapis.com/storage/v1/b/ecmwf-open-data/o',params=params)
            response.raise_for_status(); payload=response.json();result.extend(payload.get('items',[]))
            token=payload.get('nextPageToken')
            if not token:break
            params['pageToken']=token
    return dict(date=date,objects=result)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start',default='20230712');parser.add_argument('--end',default='20240228')
    parser.add_argument('--out',type=Path,required=True);args=parser.parse_args()
    start,end=(datetime.strptime(s,'%Y%m%d') for s in (args.start,args.end))
    done={json.loads(s)['date'] for s in args.out.read_text().splitlines()} if args.out.exists() else set()
    dates=[(start+timedelta(days=i)).strftime('%Y%m%d') for i in range((end-start).days+1)]
    args.out.parent.mkdir(parents=True,exist_ok=True)
    with ThreadPoolExecutor(max_workers=4) as pool, args.out.open('a') as handle:
        futures={pool.submit(day,d):d for d in dates if d not in done}
        for future in as_completed(futures):
            result=future.result();handle.write(json.dumps(result)+'\n');handle.flush()
    rows=[json.loads(s) for s in args.out.read_text().splitlines()]
    print(json.dumps(dict(days=len(rows),objects=sum(len(r['objects']) for r in rows),
        first=min((o['name'] for r in rows for o in r['objects']),default=None))))
