#!/usr/bin/env python3
"""Freeze a reproducible queue from the saved GCS census, including whole-init gaps."""
import argparse
from datetime import datetime, timedelta
import hashlib
from itertools import zip_longest
import json
from pathlib import Path


def build_queue(days):
    found={"ifs":{}, "aifs-ens":{}}
    for day in days:
        if day.get("error"):
            raise ValueError(f'inventory has an unverified day: {day["date"]}')
        for obj in day.get("objects",[]):
            pieces=obj["name"].split("/")
            if len(pieces)<4: continue
            if pieces[2] in found:
                model,layout=pieces[2],'/'.join(pieces[2:4])
            elif pieces[2] in ('0p25','0p4-beta'):
                model,layout='ifs',pieces[2]
            else: continue
            stamp=datetime.strptime(pieces[0]+pieces[1][:2],"%Y%m%d%H")
            found[model].setdefault(stamp,{}).setdefault(layout,set()).add(pieces[-1])
    queues={}
    gaps=[]
    for model,actual in found.items():
        if not actual: continue
        stamp,last=min(actual),max(actual)
        cycles=(0,12) if model=="ifs" else (0,6,12,18)
        rows=[]
        while stamp<=last:
            if stamp.hour in cycles:
                item=dict(model=model,date=stamp.strftime("%Y%m%d"),cycle=stamp.hour)
                variants=actual.get(stamp,{})
                def preference(layout):
                    names=variants[layout]
                    complete=all(any(name.endswith(f'-{b}h-enfo-ep.{suffix}') for name in names)
                        for b in (240,360) for suffix in ('index','grib2'))
                    rank={'ifs/0p25':0,'aifs-ens/0p25':0,'0p25':1,'0p4-beta':2}.get(layout,99)
                    return (not complete,rank)
                layout=min(variants,key=preference) if variants else f'{model}/0p25'
                item.update(layout=layout,grid_resolution_degrees=0.4 if layout=='0p4-beta' else 0.25)
                rows.append(item)
                if stamp not in actual: gaps.append(dict(model=model,date=item['date'],cycle=item['cycle']))
            stamp+=timedelta(hours=6)
        queues[model]=rows
    return dict(object_inits={k:len(s) for k,s in found.items()},
        target_inits={k:len(s) for k,s in queues.items()},upstream_object_gaps=gaps,
        inits=[row for pair in zip_longest(*queues.values()) for row in pair if row])


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory",type=Path,required=True)
    parser.add_argument("--output",type=Path,required=True)
    parser.add_argument("--legacy-inventory",type=Path)
    args=parser.parse_args()
    raw=args.inventory.read_bytes()
    legacy=args.legacy_inventory.read_bytes() if args.legacy_inventory else b''
    result=build_queue([json.loads(s) for s in raw.splitlines()+legacy.splitlines()])
    result.update(inventory_file=args.inventory.name,inventory_sha256=hashlib.sha256(raw).hexdigest())
    if legacy:
        result.update(legacy_inventory_file=args.legacy_inventory.name,legacy_inventory_sha256=hashlib.sha256(legacy).hexdigest())
    args.output.write_text(json.dumps(result,indent=2)+"\n")
    print(json.dumps({k:v for k,v in result.items() if k!="inits"}))
