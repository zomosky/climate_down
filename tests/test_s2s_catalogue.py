"""Validate the shipped per-centre S2S job YAMLs against the local snapshot.

Each ``config/jobs/s2s_renewables_<centre>.yaml`` may only request
combinations the ECDS constraints endpoint allows for its centre. The
snapshot at ``config/s2s/_capabilities.json`` is the source of truth;
refresh it via ``scripts/build_s2s_catalogue.py`` when ECDS rev's the
constraints hash.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from climate_download.s2s.config import load_s2s_job
from climate_download.s2s.jobs import render_leadtimes

REPO = Path(__file__).resolve().parents[1]
SNAPSHOT = REPO / "config" / "s2s" / "_capabilities.json"
CATALOGUE = REPO / "config" / "s2s_catalogue.yaml"
JOBS = sorted((REPO / "config" / "jobs").glob("s2s_renewables_*.yaml"))


@pytest.fixture(scope="module")
def snapshot() -> dict:
    return json.loads(SNAPSHOT.read_text())["origins"]


def test_snapshot_present_and_covers_curated_origins() -> None:
    raw = json.loads(SNAPSHOT.read_text())
    expected = {"cma", "ecmwf", "iap_cas", "ncep", "ukmo"}
    assert expected.issubset(raw["origins"].keys()), \
        f"snapshot missing origins: {expected - raw['origins'].keys()}"


def test_catalogue_yaml_loads() -> None:
    cat = yaml.safe_load(CATALOGUE.read_text())
    assert cat["meta"]["collection"] == "s2s-forecasts"
    expected = {"cma", "ecmwf", "iap_cas", "ncep", "ukmo"}
    assert set(cat["origins"]) == expected


def test_catalogue_origin_max_leadtimes_match_snapshot(snapshot: dict) -> None:
    cat = yaml.safe_load(CATALOGUE.read_text())
    for origin, meta in cat["origins"].items():
        snap = snapshot[origin]
        snap_max = max(
            (snap[lt]["leadtime_inst"]["max_hours"] or 0,
             snap[lt]["leadtime_daily"]["max_hours"] or 0)[
                (snap[lt]["leadtime_daily"]["max_hours"] or 0) >
                (snap[lt]["leadtime_inst"]["max_hours"] or 0)
            ]
            for lt in snap
        )
        assert meta["max_leadtime_h"] == snap_max, (
            f"{origin}: catalogue says max_leadtime_h={meta['max_leadtime_h']} "
            f"but snapshot says {snap_max}"
        )


@pytest.mark.parametrize("job_path", JOBS, ids=[p.stem for p in JOBS])
def test_job_yaml_is_subset_of_snapshot(job_path: Path, snapshot: dict) -> None:
    cfg = load_s2s_job(job_path)
    origin = cfg.source.origin
    assert origin in snapshot, f"snapshot has no entry for origin {origin!r}"

    for group in cfg.groups:
        allowed = snapshot[origin].get(group.level_type)
        assert allowed is not None, (
            f"{job_path.name}: origin {origin} does not publish "
            f"level_type={group.level_type}"
        )

        # Variables must all be in the catalogue.
        bad_vars = sorted(set(group.variables) - set(allowed["variables"]))
        assert not bad_vars, (
            f"{job_path.name}/{group.name}: variables not allowed for "
            f"{origin}/{group.level_type}: {bad_vars}"
        )

        # Pressure / isentropic levels must be in the catalogue.
        if group.levels:
            wanted = {f"{lvl}_hpa" for lvl in group.levels}
            bad_lvls = sorted(wanted - set(allowed["levels"]))
            assert not bad_lvls, (
                f"{job_path.name}/{group.name}: levels not allowed for "
                f"{origin}: {bad_lvls}"
            )

        # All leadtime values must be valid.
        leads = render_leadtimes(group, cfg.time.leadtime)
        if group.leadtime_kind == "instant":
            grid = allowed["leadtime_inst"]
            max_h = grid["max_hours"]
            assert max_h is not None, (
                f"{job_path.name}/{group.name}: snapshot exposes no instant "
                f"leadtimes for {origin}/{group.level_type}"
            )
            bad = [h for h in leads if int(h) > max_h]
            assert not bad, (
                f"{job_path.name}/{group.name}: leadtimes exceed catalogue "
                f"max ({max_h}h): {bad[:5]}"
            )
        else:
            allowed_set = set(allowed["leadtime_daily"]["windows"])
            bad = [h for h in leads if h not in allowed_set]
            assert not bad, (
                f"{job_path.name}/{group.name}: daily windows not in "
                f"catalogue: {bad[:5]}"
            )
