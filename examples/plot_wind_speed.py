#!/usr/bin/env python3
"""Plot 100 m wind speed from a downloaded AIFS GRIB subset.

Reads ``u100`` / ``v100`` with cfgrib, computes ``sqrt(u^2 + v^2)``,
crops to the configured bbox, and writes a PNG. No cartopy: a plain
``pcolormesh`` keeps dependencies light while still giving a usable map.

Examples
--------
    uv run python examples/plot_wind_speed.py \\
        --grib examples_output/20260507_00z_aifs-single_20260507000000-0h-oper-fc.subset.grib2

    uv run python examples/plot_wind_speed.py \\
        --grib path/to.grib2 --bbox 70,140,15,55 --out wind100.png
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import xarray as xr

# Headless backend so the script also runs in CI / over SSH.
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from climate_download.logging_setup import configure_logging


def _parse_bbox(text: str) -> tuple[float, float, float, float]:
    parts = [float(x) for x in text.split(",")]
    if len(parts) != 4:
        raise argparse.ArgumentTypeError("bbox must be 'west,east,south,north'")
    west, east, south, north = parts
    if not (-180 <= west < east <= 180):
        raise argparse.ArgumentTypeError("bbox lon out of range or west>=east")
    if not (-90 <= south < north <= 90):
        raise argparse.ArgumentTypeError("bbox lat out of range or south>=north")
    return west, east, south, north


def _load_100m_wind(grib_path: Path) -> xr.Dataset:
    ds = xr.open_dataset(
        grib_path,
        engine="cfgrib",
        backend_kwargs={
            "filter_by_keys": {"typeOfLevel": "heightAboveGround", "level": 100},
            "indexpath": "",
        },
    )
    if "u100" not in ds.data_vars or "v100" not in ds.data_vars:
        raise RuntimeError(
            f"{grib_path}: expected u100/v100, found {list(ds.data_vars)}"
        )
    return ds


def _crop_bbox(ds: xr.Dataset, bbox: tuple[float, float, float, float]) -> xr.Dataset:
    west, east, south, north = bbox
    lon = ds["longitude"]
    if float(lon.max()) > 180.0:
        ds = ds.assign_coords(longitude=(((lon + 180) % 360) - 180)).sortby("longitude")
    # AIFS latitudes typically descend from +90 to -90; sortby ascending so the
    # slice() bounds work regardless of source orientation.
    ds = ds.sortby("latitude")
    return ds.sel(longitude=slice(west, east), latitude=slice(south, north))


def _plot(ds: xr.Dataset, bbox: tuple[float, float, float, float], out: Path,
          title: str | None) -> None:
    speed = np.hypot(ds["u100"].values, ds["v100"].values)
    lon = ds["longitude"].values
    lat = ds["latitude"].values

    fig, ax = plt.subplots(figsize=(10, 6.5))
    vmax = float(np.nanpercentile(speed, 99))
    mesh = ax.pcolormesh(lon, lat, speed, cmap="viridis",
                         vmin=0.0, vmax=max(vmax, 1.0), shading="auto")

    # Sparse barbs to hint wind direction without overplotting.
    step_lon = max(1, len(lon) // 24)
    step_lat = max(1, len(lat) // 16)
    ax.barbs(
        lon[::step_lon], lat[::step_lat],
        ds["u100"].values[::step_lat, ::step_lon],
        ds["v100"].values[::step_lat, ::step_lon],
        length=5, linewidth=0.5, color="white", alpha=0.7,
    )

    cbar = fig.colorbar(mesh, ax=ax, shrink=0.85, pad=0.02)
    cbar.set_label("100 m wind speed (m s$^{-1}$)")
    ax.set_xlabel("Longitude (°E)")
    ax.set_ylabel("Latitude (°N)")
    ax.set_xlim(bbox[0], bbox[1])
    ax.set_ylim(bbox[2], bbox[3])
    ax.set_aspect("equal")
    ax.grid(True, linestyle=":", linewidth=0.4, alpha=0.6)
    if title:
        ax.set_title(title)

    out.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--grib", required=True, type=Path, help="path to downloaded GRIB subset")
    parser.add_argument(
        "--bbox", type=_parse_bbox, default=(70.0, 140.0, 15.0, 55.0),
        help="west,east,south,north in degrees (default: China and surroundings)",
    )
    parser.add_argument("--out", type=Path, default=Path("examples_output/wind100.png"))
    parser.add_argument("--title", default=None)
    args = parser.parse_args()
    configure_logging()

    if not args.grib.is_file():
        print(f"GRIB not found: {args.grib}", file=sys.stderr)
        return 2

    ds = _load_100m_wind(args.grib)
    valid_time = str(ds["valid_time"].values) if "valid_time" in ds.coords else "unknown"
    title = args.title or f"AIFS 100 m wind speed — valid {valid_time}"

    cropped = _crop_bbox(ds, args.bbox)
    if cropped["latitude"].size == 0 or cropped["longitude"].size == 0:
        print(f"empty crop for bbox {args.bbox}", file=sys.stderr)
        return 3
    _plot(cropped, args.bbox, args.out, title)

    speed = np.hypot(cropped["u100"].values, cropped["v100"].values)
    print(
        f"plotted {args.out} grid={cropped['latitude'].size}x{cropped['longitude'].size}"
        f" wind100 min={speed.min():.2f} mean={speed.mean():.2f} max={speed.max():.2f} m/s"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
