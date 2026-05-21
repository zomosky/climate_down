#!/usr/bin/env python3
"""Refresh ``config/s2s/_capabilities.json`` from the live ECDS constraints.

Pulls the s2s-forecasts collection's ``constraints`` blob (a JSON array of
allowed (origin × variable × level × leadtime …) records), folds it into a
per-origin per-level_type summary, and writes the snapshot used by both
``config/s2s_catalogue.yaml`` (human-curated) and the validation tests.

Run from the repo root:

    uv run --with httpx python scripts/build_s2s_catalogue.py

Origins covered match :data:`CURATED_ORIGINS`; bump that tuple to widen
the snapshot. The constraints URL must be refreshed manually if ECDS
publishes a new hash (the catalogue endpoint advertises the current href).
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

import httpx

CONSTRAINTS_URL = (
    "https://object-store.os-api.cci2.ecmwf.int:443/cci2-prod-catalogue/"
    "resources/s2s-forecasts/"
    "constraints_359685220d295d1def042ba41752c2198252766d30fb6dcd1c54cbed8c049fa4.json"
)

CURATED_ORIGINS: tuple[str, ...] = ("cma", "ecmwf", "iap_cas", "ncep", "ukmo")

OUT_PATH = Path(__file__).resolve().parent.parent / "config" / "s2s" / "_capabilities.json"


def _fetch_constraints() -> list[dict]:
    with httpx.Client(timeout=httpx.Timeout(connect=30, read=300, write=30, pool=30)) as cli:
        r = cli.get(CONSTRAINTS_URL, headers={"Accept-Encoding": "gzip"})
        r.raise_for_status()
        return r.json()


def _summarise(records: list[dict]) -> dict:
    per: dict = defaultdict(lambda: defaultdict(lambda: {
        "variables": set(), "levels": set(),
        "leadtime_inst": set(), "leadtime_daily": set(),
        "forecast_types": set(), "times": set(),
    }))
    for rec in records:
        for o in rec.get("origin", []):
            if o not in CURATED_ORIGINS:
                continue
            for lt in rec.get("level_type", []):
                d = per[o][lt]
                d["variables"].update(rec.get("variable", []))
                d["levels"].update(rec.get("level_value", []))
                d["times"].update(rec.get("time", []))
                d["forecast_types"].update(rec.get("forecast_type", []))
                for lh in rec.get("leadtime_hour", []):
                    if "_" in lh:
                        d["leadtime_daily"].add(lh)
                    else:
                        d["leadtime_inst"].add(lh)

    out = {
        "_meta": {
            "source": "ECDS s2s-forecasts constraints endpoint",
            "constraints_url": CONSTRAINTS_URL,
            "origins_covered": list(CURATED_ORIGINS),
            "schema": "per origin -> per level_type -> {variables, levels, "
                      "leadtime_inst, leadtime_daily, times, forecast_types}",
        },
        "origins": {},
    }
    for o in CURATED_ORIGINS:
        origin_obj: dict = {}
        for lt in sorted(per[o]):
            d = per[o][lt]
            leads_inst = sorted(d["leadtime_inst"], key=int)
            leads_daily = sorted(d["leadtime_daily"], key=lambda s: int(s.split("_")[1]))
            step_inst = (int(leads_inst[1]) - int(leads_inst[0])) if len(leads_inst) >= 2 else None
            origin_obj[lt] = {
                "variables": sorted(d["variables"]),
                "levels": sorted(d["levels"], key=lambda s: (len(s), s)),
                "leadtime_inst": {
                    "step_hours": step_inst,
                    "max_hours": int(leads_inst[-1]) if leads_inst else None,
                    "count": len(leads_inst),
                },
                "leadtime_daily": {
                    "windows": leads_daily,
                    "max_hours": int(leads_daily[-1].split("_")[1]) if leads_daily else None,
                    "count": len(leads_daily),
                },
                "times": sorted(d["times"]),
                "forecast_types": sorted(d["forecast_types"]),
            }
        out["origins"][o] = origin_obj
    return out


def main() -> None:
    print(f"fetching {CONSTRAINTS_URL}")
    records = _fetch_constraints()
    print(f"  records: {len(records)}")
    snap = _summarise(records)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(snap, indent=2, ensure_ascii=False))
    print(f"wrote {OUT_PATH} ({OUT_PATH.stat().st_size:,} bytes)")
    for o, lts in snap["origins"].items():
        n_vars = sum(len(lts[lt]["variables"]) for lt in lts)
        max_h = max((lts[lt]["leadtime_inst"]["max_hours"] or
                     lts[lt]["leadtime_daily"]["max_hours"] or 0)
                    for lt in lts)
        print(f"  {o:10s} level_types={list(lts)} #vars={n_vars} max={max_h}h")


if __name__ == "__main__":
    main()
