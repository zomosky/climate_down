#!/usr/bin/env python3
"""Reproducible real-data acceptance, run with the appropriate project's uv.

restorage: --phase raw; watcher: --phase query / --phase api --base-url URL.
The raw phase compares every GRIB message with a cropped Zarr grid point.
The query phase compares the same records after default query processing,
then reads back every supported export format. No large input is embedded.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


class ApiClient:
    """Stdlib transport so acceptance also runs in a runtime-only image."""
    def __init__(self, base_url, timeout=180):
        self.base_url, self.timeout = base_url, timeout

    def __enter__(self): return self
    def __exit__(self, *_): return False

    def send(self, path, payload=None):
        request=Request(self.base_url+path,
                        data=None if payload is None else json.dumps(payload).encode(),
                        headers={"Content-Type":"application/json"})
        try: response=urlopen(request,timeout=self.timeout)
        except HTTPError as exc: response=exc
        with response:
            content=response.read(); status=response.status
        class Response:
            status_code=status
            def json(self): return json.loads(content)
            def raise_for_status(self):
                assert status < 400, (path,status,content[:500])
        result=Response(); result.content=content
        return result

    def get(self, path): return self.send(path)
    def post(self, path, json): return self.send(path,json)

SOURCES = {"ifs-ens-probabilities": (5,145), "aifs-ens-probabilities": (25,313)}


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str)+"\n")


def raw(args):
    import eccodes as ec
    import numpy as np
    import xarray as xr
    report = {}
    for source, (nvars, nsamples) in SOURCES.items():
        path = args.zarr_root/source/f"{args.date}_{args.cycle:02d}z_{source}.zarr"
        ds = xr.open_zarr(path, consolidated=True)
        assert len(ds.data_vars) == nvars and ds.attrs["probability_complete"] == 1
        assert ds.sizes["latitude"] == 161 and ds.sizes["longitude"] == 281
        point = ds.sel(latitude=31., longitude=101.).load()
        records, files = [], []
        for grib in sorted((args.raw_root/source/args.date/f"{args.cycle:02d}z").glob("*.grib2")):
            files.append({"path":str(grib), "sha256":hashlib.sha256(grib.read_bytes()).hexdigest()})
            with grib.open("rb") as handle:
                while (gid := ec.codes_grib_new_from_file(handle)) is not None:
                    try:
                        param = ec.codes_get(gid,"shortName")
                        start, end = (int(ec.codes_get(gid,k)) for k in ("startStep","endStep"))
                        stat = ec.codes_get(gid,"stepType")
                        width = end-start
                        var = param if width == 0 or (width == 24 and (param.startswith("tpg") or param.startswith("10fgg"))) else f"{param}_{stat}{width}h"
                        nearest = min(ec.codes_grib_find_nearest(gid,31.,101.), key=lambda x:x["distance"])
                        assert nearest["lat"] == 31. and nearest["lon"] == 101.
                        val = point[var].sel(step=np.timedelta64(end,"h"))
                        assert abs(float(val)-nearest["value"]) < 1e-6, (source,var,end)
                        assert val.attrs["units"] == "%" and val.attrs["probability_type"] == ec.codes_get(gid,"probabilityType")
                        for side, hour in (("start",start),("end",end)):
                            coord = point[val.attrs[f"probability_window_{side}_coord"]].sel(step=np.timedelta64(end,"h"))
                            assert coord.values == point.time.values + np.timedelta64(hour,"h")
                        records.append({"variable":var,"start":start,"end":end,"value":float(val)})
                    finally:
                        ec.codes_release(gid)
        assert len(records) == nsamples
        for name in ds.data_vars:
            values = ds[name].values
            assert np.isfinite(values).any() and not np.isinf(values).any()
            assert float(np.nanmin(values)) >= 0 and float(np.nanmax(values)) <= 100
        report[source] = {"variables":nvars,"samples":len(records),"files":files,"records":records}
        ds.close()
    save(args.out/"raw-comparison.json",report)


def request(args, source, variables, space=None):
    init = datetime.strptime(args.date,"%Y%m%d")+timedelta(hours=args.cycle)
    return {"source":source,"variables":variables,
        "time":{"type":"operation","start":init.isoformat(),"tz":"UTC","horizon_hours":361,
                "init_date":init.date().isoformat(),"init_cycle":args.cycle},
        "space": space or {"kind":"sites","sites":[{"id":"reference","lat":31.,"lon":101.}],"method":"nearest"}}


def query(args):
    import numpy as np
    import pandas as pd
    import xarray as xr
    from climate_zrread.catalog.index import Catalog
    from climate_zrread.query.engine import run_query
    from climate_zrread.query.spec import QuerySpec
    from climate_zrread.io.export import export_result
    catalogue = json.loads((args.out/"raw-comparison.json").read_text())
    report = {}
    specs = {}
    for source, raw_result in catalogue.items():
        names = sorted({r["variable"] for r in raw_result["records"]})
        spec = request(args,source,names)
        specs[source] = spec
        result = run_query(QuerySpec.model_validate(spec),catalog=Catalog(args.zarr_root))
        init = np.datetime64(spec["time"]["start"])
        for row in raw_result["records"]:
            da = result.dataset[row["variable"]].sel(valid_time=init+np.timedelta64(row["end"],"h"))
            assert abs(float(da.squeeze())-row["value"]) < 1e-6
            assert da.attrs["units"] == "%"
            for side in ("start","end"):
                coord = da.attrs[f"probability_window_{side}_coord"]
                assert result.dataset[coord].sel(valid_time=da.valid_time).values == init+np.timedelta64(row[side],"h")
        assert sum(int(result.dataset[v].count()) for v in names) == raw_result["samples"]
        report[source] = {"variables":len(names),"samples_compared":raw_result["samples"]}
    multi = {"sources":{s:sp["variables"] for s,sp in specs.items()},"time":next(iter(specs.values()))["time"],
             "space":next(iter(specs.values()))["space"]}
    result = run_query(QuerySpec.model_validate(multi),catalog=Catalog(args.zarr_root))
    exports = []
    for fmt, suffix in (("json","json"),("csv","csv"),("parquet","parquet"),("arrow","arrow"),("netcdf","nc"),("zarr","zarr")):
        path = export_result(result,args.out/f"multi-source.{suffix}",fmt=fmt,layout="features")
        if fmt in ("netcdf","zarr"):
            with (xr.open_dataset(path) if fmt=="netcdf" else xr.open_zarr(path,consolidated=True)) as ds:
                for name in ds.data_vars:
                    assert ds[name].attrs["units"] == "%"
                    assert ds[name].attrs["probability_window_start_coord"] in ds.coords
                    np.testing.assert_allclose(ds[name],result.dataset[name],equal_nan=True)
        else:
            if fmt=="json":
                payload=json.loads(path.read_text()); frame=pd.DataFrame(payload["data"])
                assert len(payload["meta"]["variable_metadata"])==30
            elif fmt=="csv": frame=pd.read_csv(path)
            elif fmt=="parquet": frame=pd.read_parquet(path)
            else: frame=pd.read_feather(path)
            assert sum("probability_window_start__" in c for c in frame.columns)==30
            assert sum("probability_window_end__" in c for c in frame.columns)==30
            for source,raw_result in catalogue.items():
                for row in raw_result["records"]:
                    i = list(result.dataset.valid_time.values).index(np.datetime64(specs[source]["time"]["start"])+np.timedelta64(row["end"],"h"))
                    assert abs(frame[f"{source}:{row['variable']}:reference"].iloc[i]-row["value"])<1e-6
        exports.append(fmt)
    # A region query retains the original event window and percent unit too.
    region = request(args,"aifs-ens-probabilities",["tpg50","2tl273_max120h"],
                     {"kind":"mask","regions":["四川"],"pooling":"mean"})
    from climate_watcher.config import WatcherConfig
    from climate_watcher.zr import build_zr_settings
    settings=build_zr_settings(WatcherConfig(download_roots=[args.raw_root],zarr_root=args.zarr_root))
    regional=run_query(QuerySpec.model_validate(region),catalog=Catalog(args.zarr_root),settings=settings)
    for v in regional.dataset.data_vars:
        assert regional.dataset[v].attrs["units"]=="%"
        assert regional.dataset[v].attrs["probability_window_start_coord"] in regional.dataset.coords
    save(args.out/"query-specs.json",{**specs,"multi":multi,"region":region})
    save(args.out/"query-comparison.json",{"sources":report,"exports_read_back":exports,"region":"passed"})


def api(args):
    import numpy as np
    import pandas as pd
    import xarray as xr
    specs=json.loads((args.out/"query-specs.json").read_text())
    reference_path=args.out/"raw-comparison.json"
    reference=json.loads(reference_path.read_text()) if reference_path.exists() else {}
    report={}
    with ApiClient(args.base_url) as client:
        for source,(nvars,nsamples) in SOURCES.items():
            response=client.post("/api/v1/query",json=specs[source]); response.raise_for_status()
            payload=response.json(); save(args.out/f"api-{source}.json",payload)
            assert len(payload["meta"]["variable_metadata"])==nvars
            assert sum("probability_window_start__" in c for c in payload["columns"])==nvars
            if source in reference:
                init=pd.Timestamp(specs[source]["time"]["start"],tz="UTC")
                times=[pd.Timestamp(t) for t in payload["index"]]
                for record in reference[source]["records"]:
                    i=times.index(init+pd.Timedelta(hours=record["end"]))
                    variable=record["variable"]
                    assert abs(payload["values"][f"{source}:{variable}:reference"][i]-record["value"])<1e-6
                    for side in ("start","end"):
                        c=payload["meta"]["variable_metadata"][variable][f"probability_window_{side}_coord"]
                        assert pd.Timestamp(payload["values"][f"{source}:{c}"][i])==init+pd.Timedelta(hours=record[side])
            detail=client.get(f"/api/v1/sources/{source}/inits/{args.date}/{args.cycle}"); detail.raise_for_status()
            data=detail.json(); assert data["status"]["restore"]=="done"
            assert data["probability"]["complete"] and data["probability"]["sample_count"]==nsamples
            save(args.out/f"monitor-{source}.json",data)
            report[source]={"query":response.status_code,"monitor":"done","samples":nsamples}
            if source in reference: report[source]["samples_compared"]=len(reference[source]["records"])
            automatic=json.loads(json.dumps(specs[source]))
            automatic["time"].pop("init_date")
            automatic["time"].pop("init_cycle")
            automatic["time"]["cycle"]=args.cycle
            automatic["time"]["start"]=(pd.Timestamp(specs[source]["time"]["start"])+pd.Timedelta(hours=10)).isoformat()
            response=client.post("/api/v1/query",json=automatic); response.raise_for_status()
            assert response.json()["index"]
            report[source]["automatic_operation"]=response.status_code
        for fmt in ("csv","parquet","arrow","netcdf"):
            response=client.post(f"/api/v1/query?format={fmt}",json=specs["multi"]); response.raise_for_status()
            path=args.out/f"api-multi.{'nc' if fmt=='netcdf' else fmt}"; path.write_bytes(response.content)
            if fmt=="netcdf":
                with xr.open_dataset(path) as ds:
                    assert len(ds.data_vars)==30
                    assert all(ds[v].attrs["units"]=="%" for v in ds.data_vars)
                    assert all(ds[v].attrs["probability_window_start_coord"] in ds.coords for v in ds.data_vars)
            else:
                frame=pd.read_csv(path) if fmt=="csv" else pd.read_parquet(path) if fmt=="parquet" else pd.read_feather(path)
                assert sum("probability_window_start__" in c for c in frame.columns)==30
            report[fmt]="read back"
        invalid={**specs["ifs-ens-probabilities"],"postprocess":{"resample":{"freq":"15min"}}}
        response=client.post("/api/v1/query",json=invalid); assert response.status_code==422
        grid={"spec":{**specs["ifs-ens-probabilities"],"space":{"kind":"bbox","bbox":[100,102,30,32]}},"variable":"tpg50"}
        response=client.post("/api/v1/query/grid",json=grid); response.raise_for_status()
        assert response.json()["meta"]["probability_windows"]["tpg50"][0]["start"]
        save(args.out/"api-grid.json",response.json())
        multiday={"spec":{**specs["aifs-ens-probabilities"],"variables":["2tl273_max120h"],
                  "space":{"kind":"bbox","bbox":[100,102,30,32]}},"variable":"2tl273_max120h"}
        response=client.post("/api/v1/query/grid",json=multiday); response.raise_for_status()
        grid=response.json(); available=grid["meta"]["probability_valid_times"]["2tl273_max120h"]
        assert len(available)==2 and grid["valid_time"]==available[0]
        assert (pd.Timestamp(available[0])-pd.Timestamp(specs["aifs-ens-probabilities"]["time"]["start"],tz="UTC")).total_seconds()==240*3600
        assert all(v is not None for row in grid["values"] for v in row)
        multiday["valid_time"]=available[1]
        response=client.post("/api/v1/query/grid",json=multiday); response.raise_for_status()
        assert response.json()["valid_time"]==available[1]
        report["multiday_map"]="first/next original event windows passed"
    save(args.out/"api-acceptance.json",report)


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--phase",choices=("raw","query","api"),required=True)
    p.add_argument("--raw-root",type=Path,required=True)
    p.add_argument("--zarr-root",type=Path,required=True)
    p.add_argument("--out",type=Path,required=True)
    p.add_argument("--date",default="20261015")
    p.add_argument("--cycle",type=int,default=12)
    p.add_argument("--base-url",default="http://127.0.0.1:8099")
    a=p.parse_args(); a.out.mkdir(parents=True,exist_ok=True)
    globals()[a.phase](a)
