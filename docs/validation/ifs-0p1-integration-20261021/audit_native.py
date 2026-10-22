"""Independent ecCodes -> Zarr value audit, preserving native missing leads.

Run inside the restore environment, after run_day.py reports produced.
Only one Zarr variable and one global GRIB message are held at a time.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import datetime as dt
import hashlib
import json
from pathlib import Path
import resource
import time

import eccodes
import numpy as np
import xarray as xr

NAMES={"2t":"t2m","2d":"d2m","10u":"u10","10v":"v10",
       "100u":"u100","100v":"v100","10fg":"fg10"}


def field_name(short_name, step_type):
    name=NAMES.get(short_name,short_name)
    return name if step_type=="instant" else f"{name}_{step_type}"


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--date',required=True)
    parser.add_argument('--cycle',type=int,required=True)
    parser.add_argument('--store',type=Path)
    parser.add_argument('--steps',help='bounded prefix only for development checks')
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    began=time.perf_counter()
    key=f'{args.date}_{args.cycle:02d}z'
    raw_root=Path(f'/climate_data/ifs-hres-0p1/{args.date}/{args.cycle:02d}z')
    store=args.store or Path(f'/climate_data/climate_data_storage/zarr/ifs-hres-0p1/{key}_ifs-hres-0p1.zarr')
    expected_leads=list(range(0,145,3))+list(range(150,361,6))
    if args.steps:
        leads=[int(s) for s in args.steps.split(',')]
        files=[{'path':str(raw_root/f'f{s:03d}.subset.grib2'),'step_hours':s} for s in leads]
    else:
        manifest=raw_root/f'{key}_ifs-hres-0p1.manifest.json'
        data=json.loads(manifest.read_text())
        assert data['source']['name']=='ifs-hres-0p1' and not data.get('failures')
        files=data['files'];leads=sorted(x['step_hours'] for x in files)
        assert leads==expected_leads, 'not the full 85 native leads'
    inventory=defaultdict(list)
    previews=[]
    hashes={}
    for entry in sorted(files,key=lambda x:x['step_hours']):
        path=Path(entry['path'])
        with path.open('rb') as handle:digest=hashlib.file_digest(handle,'sha256').hexdigest()
        assert digest==entry.get('sha256',digest),path.name
        hashes[path.name]=digest
        with path.open('rb') as handle:
            while True:
                offset=handle.tell()
                message=eccodes.codes_grib_new_from_file(handle)
                if message is None:break
                try:
                    short=eccodes.codes_get(message,'shortName')
                    step_type=eccodes.codes_get(message,'stepType')
                    field=field_name(short,step_type)
                    pressure=eccodes.codes_get(message,'typeOfLevel')=='isobaricInhPa'
                    level=int(eccodes.codes_get(message,'level')) if pressure else None
                    row={'path':str(path),'offset':offset,'file_lead':entry['step_hours'],
                         'field':field,'level':level,'paramId':int(eccodes.codes_get(message,'paramId')),
                         'start':int(eccodes.codes_get(message,'startStep')),
                         'end':int(eccodes.codes_get(message,'endStep')),
                         'stepType':step_type,'units':eccodes.codes_get(message,'units')}
                    assert int(eccodes.codes_get(message,'dataDate'))==int(args.date)
                    assert int(eccodes.codes_get(message,'dataTime'))==args.cycle*100
                    assert eccodes.codes_get(message,'gridType')=='regular_ll'
                    assert (eccodes.codes_get(message,'Ni'),eccodes.codes_get(message,'Nj'))==(3600,1801)
                    assert abs(eccodes.codes_get(message,'iDirectionIncrementInDegrees')-0.1)<1e-9
                    assert abs(eccodes.codes_get(message,'jDirectionIncrementInDegrees')-0.1)<1e-9
                    if row['end']!=entry['step_hours']:
                        assert (entry['step_hours']==0 and row['end']==3 and row['start']==0
                                and short in ['mx2t3','mn2t3']),f'unexpected noncanonical raw field: {row}'
                        assert np.count_nonzero(eccodes.codes_get_values(message))==0,'nonzero preview requires investigation'
                        row['verified_all_zero']=True
                        previews.append(row)
                    else:
                        inventory[(field,level,row['end'])].append(row)
                finally:eccodes.codes_release(message)
    with xr.open_zarr(store,consolidated=True) as dataset:
        actual_fields=set(dataset.data_vars)
        lat=dataset.latitude.values.copy();lon=dataset.longitude.values.copy()
        steps=(dataset.step.values/np.timedelta64(1,'h')).astype(int)
        expected_init=np.datetime64(dt.datetime.strptime(args.date,'%Y%m%d').replace(hour=args.cycle),'ns')
        assert dataset.time.values==expected_init
        np.testing.assert_array_equal(dataset.valid_time.values,expected_init+dataset.step.values)
        attributes={name:dict(dataset[name].attrs) for name in actual_fields}
    for (field,_,_),rows in inventory.items():
        for row in rows:
            assert attributes[field]['units']==row['units'],field+' units differ from GRIB'
            assert int(attributes[field]['GRIB_paramId'])==row['paramId'],field+' paramId differs from GRIB'
    assert set(steps)==set(leads),f'unexpected native axis: {steps}'
    assert actual_fields=={field for field,_,_ in inventory},(actual_fields,{x[0] for x in inventory})
    assert (len(lat),len(lon))==(401,701)
    assert np.allclose(np.diff(lat),0.1) and np.allclose(np.diff(lon),0.1)
    assert np.allclose([lat.min(),lat.max(),lon.min(),lon.max()],[15,55,70,140])
    summaries=[];compared=0
    for field in sorted(actual_fields):
        levels={level for name,level,_ in inventory if name==field}
        for level in sorted(levels,key=lambda value:-1 if value is None else value):
            with xr.open_zarr(store,consolidated=True) as dataset:
                array=dataset[field]
                if level is not None:array=array.sel(pressure_level=level)
                actual=array.transpose('step','latitude','longitude').load().values
            present=[];missing=[];native_windows=[]
            for position,lead in enumerate(steps):
                rows=inventory.get((field,level,int(lead)),[])
                if not rows:
                    assert np.isnan(actual[position]).all(),f'non-native values filled: {field}/{lead}'
                    missing.append(int(lead));continue
                # A preview in f000 is checked too; the canonical file is
                # preferred, matching the native product's actual lead.
                rows=sorted(rows,key=lambda row:(row['file_lead']==lead,row['file_lead']))
                for row in rows:
                    with Path(row['path']).open('rb') as handle:
                        handle.seek(row['offset']);message=eccodes.codes_grib_new_from_file(handle)
                        try:
                            ni=eccodes.codes_get(message,'Ni');nj=eccodes.codes_get(message,'Nj')
                            assert not eccodes.codes_get(message,'jPointsAreConsecutive')
                            assert not eccodes.codes_get(message,'alternativeRowScanning')
                            lat0=eccodes.codes_get(message,'latitudeOfFirstGridPointInDegrees')
                            lon0=eccodes.codes_get(message,'longitudeOfFirstGridPointInDegrees')
                            dy=0.1 if eccodes.codes_get(message,'jScansPositively') else -0.1
                            dx=-0.1 if eccodes.codes_get(message,'iScansNegatively') else 0.1
                            iy=np.rint((lat-lat0)/dy).astype(int)
                            ix=np.rint(((lon-lon0)%360)/dx).astype(int)
                            represented_lon=((lon0+ix*dx+180)%360)-180
                            target_lon=((lon+180)%360)-180
                            assert np.allclose(lat0+iy*dy,lat) and np.allclose(represented_lon,target_lon)
                            values=eccodes.codes_get_values(message)
                            if eccodes.codes_get(message,'numberOfMissing'):
                                bitmap=eccodes.codes_get_array(message,'bitmap')
                                values[bitmap==0]=np.nan
                            wanted=values.reshape(nj,ni)[np.ix_(iy,ix)].astype(actual.dtype)
                            np.testing.assert_array_equal(actual[position],wanted,
                                err_msg=f'{field}/{level}/{lead} differs from native GRIB {Path(row["path"]).name}')
                        finally:eccodes.codes_release(message)
                    compared+=1
                present.append(int(lead));native_windows.append([row['start'],row['end']])
            summaries.append({'field':field,'level':level,'native_samples':len(present),
                              'native_leads':present,'upstream_missing_leads':missing,
                              'raw_windows':native_windows,'all_values_match_native':True,
                              'zarr_values_sha256':hashlib.sha256(np.ascontiguousarray(actual).view(np.uint8)).hexdigest()})
            print(json.dumps({'event':'field_verified','field':field,'level':level,
                              'native_samples':len(present),'missing':len(missing)}),flush=True)
            del actual
    report={'status':'ok','source':'ifs-hres-0p1','date':args.date,'cycle':args.cycle,
            'checked_at_utc':dt.datetime.now(dt.timezone.utc).isoformat(),
            'elapsed_s':round(time.perf_counter()-began,3),'development_prefix':bool(args.steps),
            'files':len(files),'raw_sha256':hashes,'grid':[401,701],'spacing_degrees':0.1,
            'steps':steps.tolist(),'variables':len(actual_fields),'compared_native_planes':compared,
            'all_values_match_native':True,'missingness_preserved':True,'fields':summaries,
            'init_and_valid_time_verified':True,'units_and_parameter_ids_verified':True,
            'noncanonical_preview_messages':previews,
            'peak_rss_MiB':round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024,2)}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({k:v for k,v in report.items() if k not in ['raw_sha256','fields','steps']},indent=2))


if __name__=='__main__':main()
