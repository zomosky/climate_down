"""WeatherNext 2 Mean -> China Zarr reader (bypasses download+restore).

It's already Zarr, so we open one per-init `predictions.zarr`, slice China, rename
variables to our shortName convention, restructure dims to our layout, and write a
local Zarr matching the other sources.

Auth/network (local Mac): Clash proxy + ADC with quota project stripped (see the
project memory). Run with:
    proxy_on   # or export https_proxy/http_proxy=http://127.0.0.1:7890
    uv run --with gcsfs --with zarr --with xarray --with fsspec --with google-auth \
        python weathernext_reader.py 20250101 12
"""
from __future__ import annotations
import os, sys, time
import numpy as np
import xarray as xr

SURFACE_ONLY = bool(os.environ.get("WN_SURFACE_ONLY"))  # skip pressure-level vars (fast)

BUCKET = "gs://weathernext/weathernext_2_0_0_mean/zarr"
PERIODS = ["2025_to_present", "2024_to_2025", "2023_to_2024", "2022_to_2023"]
CHINA = dict(lat=slice(15.0, 55.0), lon=slice(70.0, 140.0))  # lat asc, lon 0-360
LEVELS = [925, 1000]  # low-level only (near hub height): u/v/t/w wind profile
OUT_ROOT = "/Users/zmy/pycharm/climate_pipeline/climate_data_storage/zarr"

# WeatherNext name -> our shortName. Pressure-level vars carry a pressure_level dim.
RENAME = {
    "10m_u_component_of_wind": "u10", "10m_v_component_of_wind": "v10", "10m_wind_speed": "ws10",
    "100m_u_component_of_wind": "u100", "100m_v_component_of_wind": "v100", "100m_wind_speed": "ws100",
    "2m_temperature": "t2m", "mean_sea_level_pressure": "prmsl",
    "sea_surface_temperature": "sst", "total_precipitation_6hr": "tp",
    "geopotential": "z", "specific_humidity": "q", "temperature": "t",
    "u_component_of_wind": "u", "v_component_of_wind": "v", "vertical_velocity": "w",
}
SURFACE = ["u10","v10","ws10","u100","v100","ws100","t2m","prmsl","sst","tp"]
PRESSURE = ["u","v","t","w"]   # wind profile + temp + vertical velocity @ 925/1000

STORAGE = {"token": "google_default", "session_kwargs": {"trust_env": True}}


def resolve_store(fs, date: str, cyc: int) -> str:
    name = f"{date}_{cyc:02d}hr_01_preds/predictions.zarr"
    for p in PERIODS:
        cand = f"weathernext/weathernext_2_0_0_mean/zarr/{p}/{name}"
        if fs.exists(cand + "/.zmetadata") or fs.exists(cand + "/zarr.json") or fs.exists(cand):
            return "gs://" + cand
    raise FileNotFoundError(f"no store for {date} {cyc:02d}z in any period")


def main(date: str, cyc: int, leads: int | None):
    import gcsfs
    fs = gcsfs.GCSFileSystem(token="google_default", session_kwargs={"trust_env": True})
    store = resolve_store(fs, date, cyc)
    print("store:", store)
    ds = xr.open_zarr(store, storage_options=STORAGE, consolidated=True, chunks=None)

    # China slice + variable/level select
    ds = ds.sel(lat=CHINA["lat"], lon=CHINA["lon"])
    if leads:  # optional: cap forecast steps for a lean verification
        ds = ds.isel(time=slice(0, leads))
    keep_surf = [k for k, v in RENAME.items() if v in SURFACE and k in ds.data_vars]
    keep_pres = [] if SURFACE_ONLY else [k for k, v in RENAME.items() if v in PRESSURE and k in ds.data_vars]
    ds = ds[keep_surf + keep_pres]
    if "level" in ds.coords:
        ds = ds.sel(level=[l for l in LEVELS if l in ds.level.values])

    # rename to our convention + layout
    ds = ds.rename({k: v for k, v in RENAME.items() if k in ds.data_vars})
    ds = ds.rename({"lat": "latitude", "lon": "longitude"})
    if "level" in ds.dims:
        ds = ds.rename({"level": "pressure_level"})
    ds = ds.rename({"time": "step"})                 # lead (timedelta) -> step
    ds = ds.rename({"init_time": "time"})            # scalar init
    if "datetime" in ds.coords:
        ds = ds.rename({"datetime": "valid_time"})

    print("OUT dims:", dict(ds.sizes))
    print("OUT vars:", sorted(map(str, ds.data_vars)))
    # sanity values
    for v in ("t2m", "ws100", "tp"):
        if v in ds:
            a = ds[v].isel(step=0).load()
            print(f"  {v}[step0] min/mean/max = {float(a.min()):.3g}/{float(a.mean()):.3g}/{float(a.max()):.3g}")

    out = f"{OUT_ROOT}/weathernext/{date}_{cyc:02d}z_weathernext.zarr"
    # Clear inherited encoding: the source zarr's Blosc compressor is not reusable
    # when writing (esp. under zarr v3). Load into memory, then write as zarr v2
    # (matching our other sources) with zstd.
    import numcodecs
    ds = ds.load()
    for v in list(ds.data_vars) + list(ds.coords):
        ds[v].encoding.clear()
    comp = numcodecs.Zstd(level=3)
    enc = {v: {"compressor": comp} for v in ds.data_vars}
    t = time.time()
    ds.to_zarr(out, mode="w", consolidated=True, zarr_format=2, encoding=enc)
    print(f"WROTE {out} in {time.time()-t:.1f}s")


if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else "20250101"
    cyc = int(sys.argv[2]) if len(sys.argv) > 2 else 12
    leads = int(sys.argv[3]) if len(sys.argv) > 3 else None
    main(date, cyc, leads)
