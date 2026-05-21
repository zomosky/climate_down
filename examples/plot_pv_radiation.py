#!/usr/bin/env python3
"""Plot AIFS surface short-wave (or long-wave) radiation as PV-side input.

``ssrd`` / ``strd`` in AIFS are *accumulated* fluxes from the start of the
forecast (units: J m^-2). Step=0 is therefore identically zero. To get a
useful map we either:

  * divide by the elapsed seconds since the cycle start to obtain the mean
    flux in W m^-2 over the [0, step] interval, or
  * pass two GRIBs (``--grib`` and ``--prev-grib``) and difference them to
    obtain the mean flux over [prev_step, step] (more representative of
    "the past 6 h" for short steps).

Examples
--------
    # mean SSRD over [0h, 6h]
    uv run python examples/plot_pv_radiation.py \\
        --grib examples_output/..._0006-6h-oper-fc.subset.grib2

    # mean SSRD over [6h, 12h]
    uv run python examples/plot_pv_radiation.py \\
        --grib examples_output/..._0012-12h-oper-fc.subset.grib2 \\
        --prev-grib examples_output/..._0006-6h-oper-fc.subset.grib2
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import xarray as xr

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from climate_download.logging_setup import configure_logging

VAR_LABELS = {
    "ssrd": "Surface short-wave radiation downward",
    "strd": "Surface long-wave radiation downward",
}


def _parse_bbox(text: str) -> tuple[float, float, float, float]:
    parts = [float(x) for x in text.split(",")]
    if len(parts) != 4:
        raise argparse.ArgumentTypeError("bbox must be 'west,east,south,north'")
    w, e, s, n = parts
    if not (-180 <= w < e <= 180 and -90 <= s < n <= 90):
        raise argparse.ArgumentTypeError("bbox out of range")
    return w, e, s, n


def _open(grib: Path, var: str) -> xr.Dataset:
    ds = xr.open_dataset(
        grib, engine="cfgrib",
        backend_kwargs={"filter_by_keys": {"shortName": var}, "indexpath": ""},
    )
    if var not in ds.data_vars:
        raise RuntimeError(f"{grib}: variable {var!r} not present (have {list(ds.data_vars)})")
    return ds


def _step_seconds(ds: xr.Dataset) -> float:
    step_ns = ds["step"].values.astype("timedelta64[ns]").astype(np.int64)
    seconds = float(step_ns) / 1e9
    if seconds <= 0:
        raise RuntimeError(
            "step is 0; accumulated radiation field is identically zero. "
            "Pass a GRIB at step >= 6 (or supply --prev-grib for a difference)."
        )
    return seconds


def _crop(ds: xr.Dataset, bbox: tuple[float, float, float, float]) -> xr.Dataset:
    lon = ds["longitude"]
    if float(lon.max()) > 180.0:
        ds = ds.assign_coords(longitude=(((lon + 180) % 360) - 180)).sortby("longitude")
    ds = ds.sortby("latitude")
    w, e, s, n = bbox
    return ds.sel(longitude=slice(w, e), latitude=slice(s, n))


def _plot(field: np.ndarray, lon: np.ndarray, lat: np.ndarray,
          bbox: tuple[float, float, float, float], out: Path,
          title: str, units: str) -> None:
    fig, ax = plt.subplots(figsize=(10, 6.5))
    vmax = float(np.nanpercentile(field, 99))
    mesh = ax.pcolormesh(lon, lat, field, cmap="inferno",
                         vmin=0.0, vmax=max(vmax, 1.0), shading="auto")
    cbar = fig.colorbar(mesh, ax=ax, shrink=0.85, pad=0.02)
    cbar.set_label(units)
    ax.set_xlabel("Longitude (°E)")
    ax.set_ylabel("Latitude (°N)")
    ax.set_xlim(bbox[0], bbox[1])
    ax.set_ylim(bbox[2], bbox[3])
    ax.set_aspect("equal")
    ax.grid(True, linestyle=":", linewidth=0.4, alpha=0.6, color="white")
    ax.set_title(title)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--grib", required=True, type=Path)
    parser.add_argument("--prev-grib", type=Path, default=None,
                        help="optional earlier-step GRIB to difference against")
    parser.add_argument("--variable", default="ssrd", choices=sorted(VAR_LABELS))
    parser.add_argument("--bbox", type=_parse_bbox, default=(70.0, 140.0, 15.0, 55.0))
    parser.add_argument("--out", type=Path, default=Path("examples_output/ssrd.png"))
    args = parser.parse_args()
    configure_logging()

    if not args.grib.is_file():
        print(f"GRIB not found: {args.grib}", file=sys.stderr)
        return 2

    ds = _open(args.grib, args.variable)
    step_s = _step_seconds(ds)
    accum = ds[args.variable].values  # J m^-2

    if args.prev_grib is not None:
        prev = _open(args.prev_grib, args.variable)
        prev_step_s = float(prev["step"].values.astype("timedelta64[ns]").astype(np.int64)) / 1e9
        if prev_step_s >= step_s:
            print(f"--prev-grib step ({prev_step_s}s) must be < --grib step ({step_s}s)", file=sys.stderr)
            return 3
        accum = accum - prev[args.variable].values
        elapsed = step_s - prev_step_s
        window = f"[{int(prev_step_s/3600)}h, {int(step_s/3600)}h]"
    else:
        elapsed = step_s
        window = f"[0h, {int(step_s/3600)}h]"

    flux = accum / elapsed  # W m^-2
    cropped = _crop(ds.assign(flux=(("latitude", "longitude"), flux)), args.bbox)
    field = cropped["flux"].values
    valid_time = str(ds["valid_time"].values)
    title = f"AIFS mean {args.variable.upper()} over {window} — valid {valid_time}"
    _plot(field, cropped["longitude"].values, cropped["latitude"].values,
          args.bbox, args.out, title, "W m$^{-2}$")
    print(
        f"plotted {args.out} grid={cropped['latitude'].size}x{cropped['longitude'].size}"
        f" {args.variable} window={window} min={field.min():.1f} mean={field.mean():.1f} max={field.max():.1f} W/m^2"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
