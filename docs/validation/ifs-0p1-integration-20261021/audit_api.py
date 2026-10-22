"""Real 0p1 API acceptance against native Zarr values and export readback.

Run inside climate-watcher after the chosen init's consolidated store exists.
"""
from __future__ import annotations

import argparse
import copy
import datetime as dt
import hashlib
import io
import json
from pathlib import Path
import tempfile
import time
import urllib.request
import urllib.error

import numpy as np
import pandas as pd
import pyarrow.ipc
import xarray as xr


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--date',required=True);parser.add_argument('--cycle',type=int,required=True)
    parser.add_argument('--output-dir',type=Path,required=True)
    parser.add_argument('--site-id',default='exact')
    parser.add_argument('--reuse-json',action='store_true')
    args=parser.parse_args();args.output_dir.mkdir(parents=True,exist_ok=True)
    began=time.perf_counter()
    source='ifs-hres-0p1';key=f'{args.date}_{args.cycle:02d}z'
    init=dt.datetime.strptime(args.date,'%Y%m%d').replace(hour=args.cycle,tzinfo=dt.timezone.utc)
    start=init+dt.timedelta(hours=6)
    site_id=args.site_id
    site={'id':site_id,'lat':35.0,'lon':116.0}
    spec={'source':source,'variables':['t2m','u100','v100','ssrd_accum','t_850'],
          'time':{'type':'operation','start':start.isoformat(),'horizon_hours':24,'tz':'UTC',
                  'init_date':init.date().isoformat(),'init_cycle':args.cycle},
          'space':{'kind':'sites','sites':[site],'method':'nearest'},
          'postprocess':{'deaccumulate':False}}
    report={'source':source,'date':args.date,'cycle':args.cycle,'started_at_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'cases':[]}
    metrics_path=args.output_dir/'case-timings.json'
    previous_metrics=json.loads(metrics_path.read_text()) if args.reuse_json and metrics_path.exists() else []
    def request(name,path,body=None):
        before=time.perf_counter()
        requested_at=dt.datetime.now(dt.timezone.utc).isoformat()
        req=urllib.request.Request('http://127.0.0.1:8090'+path,
            data=json.dumps(body).encode() if body is not None else None,
            headers={'Content-Type':'application/json'} if body is not None else {})
        try:
            with urllib.request.urlopen(req,timeout=130) as response:
                payload=response.read();headers={k.lower():v for k,v in response.headers.items()};status=response.status
        except urllib.error.HTTPError as error:
            (args.output_dir/(name+'-http-error.json')).write_bytes(error.read())
            raise
        row={'name':name,'path':path,'status':status,'seconds':round(time.perf_counter()-before,4),
             'bytes':len(payload),'sha256':hashlib.sha256(payload).hexdigest(),
             'warning_count':headers.get('x-watcher-warning-count')}
        row.update(started_at_utc=requested_at,finished_at_utc=dt.datetime.now(dt.timezone.utc).isoformat())
        report['cases'].append(row)
        metrics_path.write_text(json.dumps(report['cases'],indent=2)+'\n')
        print(json.dumps(row),flush=True)
        return payload,headers
    def json_case(name,path,body=None):
        cached=args.output_dir/(name+'.json')
        prior=next((row for row in previous_metrics if row['name']==name),None)
        if args.reuse_json and cached.exists() and prior:
            row=dict(prior,previous_success_reused=True)
            report['cases'].append(row);print(json.dumps(row),flush=True)
            return json.loads(cached.read_text())
        payload,_=request(name,path,body)
        value=json.loads(payload)
        (args.output_dir/(name+'.json')).write_text(json.dumps(value,ensure_ascii=False)+'\n')
        return value
    visibility_started=time.perf_counter()
    while True:
        with urllib.request.urlopen('http://127.0.0.1:8090/api/v1/catalog/inits?source='+source,timeout=15) as response:
            visible=json.load(response)
        if any(row['consolidated'] and dt.datetime.fromisoformat(row['init_time'])==init for row in visible['inits']):break
        if time.perf_counter()-visibility_started>90:raise TimeoutError('product not visible after catalog scan cache expiry')
        time.sleep(2)
    report['catalog_visibility_wait_s']=round(time.perf_counter()-visibility_started,3)
    native=json_case('native_sites','/api/v1/query',spec)
    expected_start=np.datetime64(start.replace(tzinfo=None),'ns')
    expected_end=expected_start+np.timedelta64(24,'h')
    store=Path(f'/climate_data/climate_data_storage/zarr/{source}/{key}_{source}.zarr')
    with xr.open_zarr(store,consolidated=True) as dataset:
        point=dataset.sel(latitude=site['lat'],longitude=site['lon'],method='nearest').load()
        valid=point.valid_time.values.astype('datetime64[ns]')
        mask=(valid>=expected_start)&(valid<expected_end)
        actual_times=pd.to_datetime(native['index'],utc=True).tz_localize(None).to_numpy()
        np.testing.assert_array_equal(actual_times,valid[mask])
        for public,raw in [('t2m','t2m'),('u100','u100'),('v100','v100'),('ssrd_accum','ssrd_accum'),('t_850','t')]:
            expected=point[raw].sel(pressure_level=850).values if raw=='t' else point[raw].values
            np.testing.assert_array_equal(native['values'][f'{source}:{public}:{site_id}'],expected[mask])
        flux_spec=copy.deepcopy(spec);flux_spec['variables']=['ssrd_accum'];flux_spec['postprocess']={'deaccumulate':True}
        flux=json_case('native_radiation_flux','/api/v1/query',flux_spec)
        accumulated=point.ssrd_accum.values.astype(float)
        delta=np.diff(accumulated)/np.diff(valid).astype('timedelta64[s]').astype(float)
        positions=np.flatnonzero(mask)
        np.testing.assert_allclose(flux['values'][f'{source}:ssrd_accum:{site_id}'],delta[positions-1],rtol=1e-5,atol=1e-4)
    extreme=copy.deepcopy(spec);extreme['variables']=['mx2t3_max','mn2t3_min']
    extreme['time']['start']=init.isoformat();extreme['time']['horizon_hours']=361
    extrema=json_case('native_extreme_missingness','/api/v1/query',extreme)
    np.testing.assert_array_equal(pd.to_datetime(extrema['index'],utc=True).tz_localize(None).to_numpy(),valid)
    for variable in extreme['variables']:
        values=extrema['values'][f'{source}:{variable}:{site_id}']
        np.testing.assert_allclose(np.asarray(values,dtype=float),point[variable].values,rtol=0,atol=0,equal_nan=True)
        assert any(value is None for value in values),'upstream non-native extremes were filled'
    resampled=copy.deepcopy(spec)
    resampled['variables']=['t2m','u100','v100','ssrd_accum','dhi','dni','kt','t_850']
    resampled['time']['start']=(init+dt.timedelta(hours=12)).isoformat()
    resampled['space']={'kind':'sites','sites':[site,{'id':'cd','lat':30.67,'lon':104.06}]}
    resampled['postprocess']={'resample':{'freq':'15min'}}
    fifteen=json_case('sites_15min','/api/v1/query',resampled)
    assert len(fifteen['index'])==96
    axis=pd.to_datetime(fifteen['index'],utc=True)
    assert np.all(axis[1:]-axis[:-1]==pd.Timedelta(minutes=15))
    for variable in resampled['variables']:
        for point_id in [site_id,'cd']:
            column=f'{source}:{variable}:{point_id}'
            assert column in fifteen['values'],column
            assert all(value is not None and np.isfinite(value) for value in fifteen['values'][column]),column
    grid_spec=copy.deepcopy(spec);grid_spec['variables']=['t2m'];grid_spec['time']['horizon_hours']=6
    grid_spec['space']={'kind':'bbox','bbox':[103.9,104.2,30.5,30.8]}
    grid=json_case('grid','/api/v1/query/grid',{'spec':grid_spec,'variable':'t2m','valid_time':start.isoformat(),'stride':1})
    assert len(grid['lats'])==len(grid['lons'])==4
    assert np.allclose(np.diff(grid['lats']),0.1) and np.allclose(np.diff(grid['lons']),0.1)
    assert np.isfinite(np.array(grid['values'],dtype=float)).all()
    with xr.open_zarr(store,consolidated=True) as dataset:
        expected_grid=dataset.t2m.sel(step=np.timedelta64(6,'h'),latitude=grid['lats'],longitude=grid['lons']).values
        np.testing.assert_allclose(grid['values'],expected_grid,rtol=0,atol=0.000501)
    for mode in ['train','train_mix']:
        train=copy.deepcopy(spec);train['variables']=['t2m','u100']
        target=init.date()+dt.timedelta(days=1 if args.cycle else 0)
        train['time']={'type':'train','start':target.isoformat(),'end':target.isoformat(),'n':0,
                       'cycle':'mix' if mode.endswith('mix') else args.cycle,'tz':'UTC'}
        result=json_case(mode,'/api/v1/query',train)
        assert len(result['index'])>0
    pooled=copy.deepcopy(spec);pooled['variables']=['t2m','u100','ssrd_accum']
    pooled['space']={'kind':'mask','regions':['河北'],'pooling':'wmean',
                     'weights':{'t2m':'ntl','u100':'wind','ssrd_accum':'solar'}}
    region=json_case('province_weighted','/api/v1/query',pooled)
    assert len(region['index'])>0
    assert any('河北' in column for column in region['columns'])
    def verify_frame(frame):
        assert len(frame)==96
        for variable in resampled['variables']:
            for point_id in [site_id,'cd']:
                column=f'{source}:{variable}:{point_id}'
                np.testing.assert_allclose(frame[column],fifteen['values'][column],rtol=1e-11,atol=1e-10)
    for fmt in ['csv','parquet','arrow','netcdf']:
        payload,headers=request('export_'+fmt,'/api/v1/query?format='+fmt,resampled)
        path=args.output_dir/('sites_15min.'+('nc' if fmt=='netcdf' else fmt));path.write_bytes(payload)
        if fmt=='csv':
            verify_frame(pd.read_csv(io.BytesIO(payload)))
        elif fmt=='parquet':
            verify_frame(pd.read_parquet(io.BytesIO(payload)))
        elif fmt=='arrow':
            try:table=pyarrow.ipc.open_file(io.BytesIO(payload)).read_all()
            except pyarrow.ArrowInvalid:table=pyarrow.ipc.open_stream(io.BytesIO(payload)).read_all()
            verify_frame(table.to_pandas())
        else:
            with xr.open_dataset(path) as exported:
                assert exported.sizes['valid_time']==96 and exported.sizes['site']==2
                assert 'W' in exported.ssrd_accum.attrs['units']
                assert np.isfinite(exported[['t2m','u100','ssrd_accum']].to_array().values).all()
                for variable in resampled['variables']:
                    for point_id in [site_id,'cd']:
                        np.testing.assert_allclose(exported[variable].sel(site=point_id).values,
                            fifteen['values'][f'{source}:{variable}:{point_id}'],rtol=1e-11,atol=1e-10)
        assert int(headers['x-watcher-warning-count'])==len(fifteen['meta']['warnings'])
    health=json_case('health','/api/v1/health');assert health['status']=='ok'
    detail=json_case('monitor',f'/api/v1/sources/{source}/inits/{args.date}/{args.cycle}')
    assert detail['status']['download']=='complete' and detail['status']['restore']=='done'
    assert detail['status']['files_count']==85 and not detail['status']['failures_count']
    metadata=json_case('source_metadata',f'/api/v1/sources/{source}')
    assert metadata['profile_registered'] and '0.1°' in metadata['profile_description']
    assert len(metadata['zarr_variables'])==30
    catalog=json_case('variable_catalog',f'/api/v1/catalog/variables?source={source}')
    report.update(status='ok',finished_at_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
                  elapsed_s=round(time.perf_counter()-began,3),native_values_match_zarr=True,
                  radiation_difference_matches_native=True,grid_spacing_degrees=0.1,
                  upstream_extreme_missingness_preserved=True,
                  all_export_formats_read_back=True)
    (args.output_dir/'api-acceptance.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({key:value for key,value in report.items() if key!='cases'},indent=2))


if __name__=='__main__':main()
