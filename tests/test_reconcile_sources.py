from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "deploy" / "reconcile_sources.py"
BOOT_SCRIPT = Path(__file__).parents[1] / "deploy" / "recover_all_on_boot.sh"
TIMER_UNIT = Path(__file__).parents[1] / "deploy" / "systemd" / "climate-source-reconcile.timer"


def _load_module():
    assert SCRIPT.exists(), "deploy/reconcile_sources.py must be implemented"
    spec = importlib.util.spec_from_file_location("reconcile_sources", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_policies_cover_every_operational_nwp_source_and_prioritize_dwd():
    module = _load_module()

    policies = module.POLICIES

    assert [(p.source, p.cycle) for p in policies] == [
        ("dwd-icon", 12),
        ("dwd-icon", 0),
        ("gfs-0p25", 12),
        ("aifs-single", 12),
        ("ifs-hres", 12),
        ("ifs-hres", 0),
        ("ifs-hres-0p1", 12),
        ("ifs-hres-0p1", 0),
    ]


def test_latest_init_date_uses_cycle_and_publish_lag_after_reboot():
    module = _load_module()
    now = datetime(2026, 9, 10, 1, 52, tzinfo=timezone.utc)
    dwd_12z = module.POLICIES[0]

    assert module.latest_init_date(now, dwd_12z) == "20260909"


def test_latest_init_date_normalizes_host_local_time_to_utc():
    module = _load_module()
    china_time = datetime(2026, 9, 11, 4, 0, tzinfo=timezone(timedelta(hours=8)))
    dwd_00z = module.POLICIES[1]

    assert module.latest_init_date(china_time, dwd_00z) == "20260910"


def test_candidate_dates_include_cloud_archive_lookback_oldest_first():
    module = _load_module()
    now = datetime(2026, 9, 10, 1, 52, tzinfo=timezone.utc)
    gfs_12z = module.POLICIES[2]

    assert module.candidate_dates(now, gfs_12z) == (
        "20260907",
        "20260908",
        "20260909",
    )


def _write_manifest(tmp_path: Path, *, failures=None, corrupt_step=None) -> tuple[Path, Path]:
    data_root = tmp_path / "climate_data"
    init_dir = data_root / "sample" / "20260909" / "12z"
    init_dir.mkdir(parents=True)
    files = []
    for step in (0, 1):
        raw = init_dir / f"f{step:03d}.subset.grib2"
        raw.write_bytes(b"BROKEN!!" if step == corrupt_step else b"GRIB7777")
        files.append(
            {
                "step_hours": step,
                "path": f"/climate_data/sample/20260909/12z/{raw.name}",
                "size_bytes": 8,
            }
        )
    manifest = init_dir / "20260909_12z_sample.manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "completed_at": "2026-09-09T20:00:00Z",
                "files": files,
                "failures": failures or [],
            }
        )
    )
    return manifest, data_root


def test_manifest_health_accepts_complete_files_with_grib_boundaries(tmp_path):
    module = _load_module()
    manifest, data_root = _write_manifest(tmp_path)

    assert module.manifest_health(manifest, data_root, 2) == (True, "complete")


def test_manifest_health_rejects_failures_and_corrupt_raw_files(tmp_path):
    module = _load_module()
    failed, data_root = _write_manifest(tmp_path / "failed", failures=[{"step": 1}])
    corrupt, corrupt_root = _write_manifest(tmp_path / "corrupt", corrupt_step=1)

    assert module.manifest_health(failed, data_root, 2) == (False, "manifest has 1 failure(s)")
    ok, reason = module.manifest_health(
        corrupt, corrupt_root, 2, verify_boundaries=True
    )
    assert not ok
    assert reason == "invalid GRIB boundary: f001.subset.grib2"


def test_manifest_health_default_avoids_raw_content_reads(tmp_path):
    module = _load_module()
    corrupt, data_root = _write_manifest(tmp_path, corrupt_step=1)

    assert module.manifest_health(corrupt, data_root, 2) == (True, "complete")


def test_cli_dry_run_reports_missing_selected_source_without_running(tmp_path):
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--now",
            "2026-09-10T01:52:00Z",
            "--data-root",
            str(tmp_path / "data"),
            "--lock",
            str(tmp_path / "reconcile.lock"),
            "--only-source",
            "dwd-icon",
            "--dry-run",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert "REPAIR dwd-icon 20260909 12z: manifest missing" in result.stdout
    assert "REPAIR dwd-icon 20260909 00z: manifest missing" in result.stdout
    assert "gfs-0p25" not in result.stdout


def test_cli_invokes_runner_with_explicit_init_environment_oldest_first(tmp_path):
    calls = tmp_path / "calls.log"
    runner = tmp_path / "fake-download-run.sh"
    runner.write_text(
        "#!/bin/sh\n"
        'printf "%s|%s|%s|%s\\n" "$INIT_DATE" "$CYCLE" '
        '"$PUBLISH_LAG_HOURS" "$1" >> "$CALLS"\n'
    )
    runner.chmod(0o755)
    env = os.environ.copy()
    env["CALLS"] = str(calls)

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--now",
            "2026-09-10T01:52:00Z",
            "--data-root",
            str(tmp_path / "data"),
            "--runner",
            str(runner),
            "--log-dir",
            str(tmp_path / "logs"),
            "--lock",
            str(tmp_path / "reconcile.lock"),
            "--only-source",
            "aifs-single",
        ],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert calls.read_text().splitlines() == [
        "20260907|12|6|config/jobs/aifs_renewables_ser.yaml",
        "20260908|12|6|config/jobs/aifs_renewables_ser.yaml",
        "20260909|12|6|config/jobs/aifs_renewables_ser.yaml",
    ]


def test_boot_recovery_prioritizes_dwd_then_checks_every_pipeline_stage(tmp_path):
    assert BOOT_SCRIPT.exists(), "deploy/recover_all_on_boot.sh must be implemented"
    operation = tmp_path / "operation"
    operation.mkdir()
    calls = tmp_path / "calls.log"
    fake_python = tmp_path / "fake-python"
    fake_python.write_text(
        "#!/bin/sh\n"
        'shift\n'
        'printf "reconcile|%s\\n" "$*" >> "$CALLS"\n'
    )
    fake_python.chmod(0o755)
    for name in (
        "hima-realtime.sh",
        "hima-rebuild-recent.sh",
        "restore_scan.sh",
        "era5-production.sh",
    ):
        hook = operation / name
        hook.write_text(
            "#!/bin/sh\n"
            'printf "%s|%s|%s\\n" "$(basename "$0")" "${SOURCE:-}" "${LOG:-}" >> "$CALLS"\n'
        )
        hook.chmod(0o755)
    env = os.environ.copy()
    env.update(
        CALLS=str(calls),
        OPERATION_DIR=str(operation),
        PYTHON_BIN=str(fake_python),
        BOOT_LOG=str(tmp_path / "boot.log"),
        BOOT_LOCK=str(tmp_path / "boot.lock"),
        SKIP_LOCK="1",
        SKIP_CONTAINER_WAIT="1",
    )

    result = subprocess.run(
        ["bash", str(BOOT_SCRIPT)], env=env, text=True, capture_output=True, check=False
    )

    assert result.returncode == 0
    lines = calls.read_text().splitlines()
    assert lines[0].endswith("--only-source dwd-icon")
    assert lines[1] == "hima-realtime.sh||"
    assert lines[2].endswith("--exclude-source dwd-icon")
    assert lines[3] == (
        f"hima-rebuild-recent.sh||{operation / 'logs' / 'hima-rebuild.log'}"
    )
    assert lines[4:8] == [
        "restore_scan.sh|gfs-0p25|",
        "restore_scan.sh|aifs-single|",
        "restore_scan.sh|ifs-hres|",
        "restore_scan.sh|dwd-icon|",
    ]
    assert lines[8] == "era5-production.sh||"


def test_persistent_timer_uses_calendar_schedule_that_can_replay_missed_runs():
    unit = TIMER_UNIT.read_text()

    assert "OnCalendar=*:17,47" in unit
    assert "Persistent=true" in unit
    assert "OnUnitActiveSec" not in unit
