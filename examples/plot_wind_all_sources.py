#!/usr/bin/env python3
"""Plot one wind-speed map per source for a given (date, cycle, step).

For each downloaded source under ``output/<source>/<date>/<cycle>z/`` we pick
the best available wind level (100 m if present, else 10 m, else the lowest
pressure level — used for GraphCast-pres which carries only pl winds) and
write ``<out_dir>/<source>_f<step>_wind.png``.
"""

from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path

import numpy as np
import xarray as xr

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

warnings.filterwarnings("ignore", category=FutureWarning, module="cfgrib")

# (source dir, label, level descriptor) — descriptor is a (typeOfLevel, level)
# tuple used to filter cfgrib.  100 m preferred where available, 10 m for
# graphcast-sfc, 1000 hPa for graphcast-pres (no near-surface winds in pl).
SOURCES: list[tuple[str, str, tuple[str, float], str]] = [
    ("aifs-single",    "AIFS-Single",    ("heightAboveGround", 100), "100 m"),
    ("ifs-hres",       "IFS-HRES",       ("heightAboveGround", 100), "100 m"),
    ("gfs-0p25",       "GFS 0.25°",      ("heightAboveGround", 100), "100 m"),
    ("graphcast-pres", "GraphCast-pres", ("isobaricInhPa",    1000), "1000 hPa"),
    ("graphcast-sfc",  "GraphCast-sfc",  ("heightAboveGround",  10), "10 m"),
]

BBOX = (70.0, 140.0, 15.0, 55.0)  # China-centred


def _open_wind(path: Path, type_of_level: str, level: float) -> xr.Dataset:
    ds = xr.open_dataset(
        path, engine="cfgrib",
        backend_kwargs={
            "filter_by_keys": {"typeOfLevel": type_of_level, "level": level},
            "indexpath": "",
        },
    )
    # Different sources use different short names: pick u/v pair.
    pairs = [("u100", "v100"), ("u10", "v10"), ("u", "v")]
    for ucand, vcand in pairs:
        if ucand in ds.data_vars and vcand in ds.data_vars:
            return ds.rename({ucand: "u", vcand: "v"})[["u", "v"]]
    raise RuntimeError(f"{path}: no u/v pair found in {list(ds.data_vars)}")


def _crop(ds: xr.Dataset, bbox: tuple[float, float, float, float]) -> xr.Dataset:
    west, east, south, north = bbox
    lon = ds["longitude"]
    if float(lon.max()) > 180.0:
        ds = ds.assign_coords(longitude=(((lon + 180) % 360) - 180)).sortby("longitude")
    ds = ds.sortby("latitude")
    return ds.sel(longitude=slice(west, east), latitude=slice(south, north))


def _plot(ds: xr.Dataset, out: Path, title: str, level_label: str) -> tuple[float, float, float]:
    u, v = ds["u"].values, ds["v"].values
    speed = np.hypot(u, v)
    lon, lat = ds["longitude"].values, ds["latitude"].values

    fig, ax = plt.subplots(figsize=(10, 6.5))
    vmax = float(np.nanpercentile(speed, 99))
    mesh = ax.pcolormesh(lon, lat, speed, cmap="viridis",
                         vmin=0.0, vmax=max(vmax, 1.0), shading="auto")

    step_lon = max(1, len(lon) // 24)
    step_lat = max(1, len(lat) // 16)
    ax.barbs(
        lon[::step_lon], lat[::step_lat],
        u[::step_lat, ::step_lon], v[::step_lat, ::step_lon],
        length=5, linewidth=0.5, color="white", alpha=0.7,
    )

    cbar = fig.colorbar(mesh, ax=ax, shrink=0.85, pad=0.02)
    cbar.set_label(f"{level_label} wind speed (m s$^{{-1}}$)")
    ax.set_xlabel("Longitude (°E)")
    ax.set_ylabel("Latitude (°N)")
    ax.set_xlim(BBOX[0], BBOX[1])
    ax.set_ylim(BBOX[2], BBOX[3])
    ax.set_aspect("equal")
    ax.grid(True, linestyle=":", linewidth=0.4, alpha=0.6)
    ax.set_title(title)

    out.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(out, dpi=140)
    plt.close(fig)
    return float(speed.min()), float(speed.mean()), float(speed.max())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--date",  default="20260501")
    parser.add_argument("--cycle", default="12")
    parser.add_argument("--step",  default="024", help="zero-padded step, e.g. 024")
    parser.add_argument("--root",  type=Path, default=Path("output"))
    parser.add_argument("--out-dir", type=Path, default=Path("output/_figs"))
    args = parser.parse_args()

    cycle = f"{int(args.cycle):02d}"
    step  = f"{int(args.step):03d}"
    print(f"# plotting {args.date} {cycle}z f{step} across {len(SOURCES)} sources")
    print(f"# bbox = {BBOX}")
    rc = 0
    for src, label, (tol, lvl), lvl_label in SOURCES:
        grib = args.root / src / args.date / f"{cycle}z" / f"f{step}.subset.grib2"
        out  = args.out_dir / f"{src}_f{step}_wind.png"
        if not grib.is_file():
            print(f"  [skip] {src}: missing {grib}")
            rc = 1
            continue
        try:
            ds = _open_wind(grib, tol, lvl)
        except Exception as e:
            print(f"  [skip] {src}: {e}")
            rc = 1
            continue
        cropped = _crop(ds, BBOX)
        valid = str(ds["valid_time"].values) if "valid_time" in ds.coords else "?"
        title = f"{label} {lvl_label} wind — init {args.date} {cycle}z, f{step} (valid {valid[:16]})"
        mn, mean, mx = _plot(cropped, out, title, lvl_label)
        print(f"  [ok]   {src:18s} -> {out}  min={mn:5.2f}  mean={mean:5.2f}  max={mx:5.2f} m/s"
              f"  grid={cropped['latitude'].size}x{cropped['longitude'].size}")
    return rc


if __name__ == "__main__":
    sys.exit(main())
