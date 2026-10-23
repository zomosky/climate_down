#!/usr/bin/env bash
# Recover every production data path after the host/container becomes available.
set -uo pipefail

OPERATION_DIR="${OPERATION_DIR:-/home/zhangmingyu/operation}"
PYTHON_BIN="${PYTHON_BIN:-/usr/bin/python3}"
DOCKER_BIN="${DOCKER_BIN:-/usr/bin/docker}"
CONTAINER="${CONTAINER:-zhangmy-dev}"
BOOT_LOG="${BOOT_LOG:-$OPERATION_DIR/logs/boot-recovery.log}"
BOOT_LOCK="${BOOT_LOCK:-$OPERATION_DIR/lock/boot-recovery.lock}"
WAIT_SECONDS="${WAIT_SECONDS:-300}"
WAIT_INTERVAL="${WAIT_INTERVAL:-10}"

mkdir -p "$(dirname "$BOOT_LOG")" "$(dirname "$BOOT_LOCK")"
if [[ "${SKIP_LOCK:-0}" != "1" ]]; then
  exec 9>"$BOOT_LOCK"
  if ! flock -n 9; then
    exit 0
  fi
fi
exec >>"$BOOT_LOG" 2>&1

timestamp() { date -u '+%FT%TZ'; }
failures=0

run_step() {
  local label="$1"
  shift
  echo "[$(timestamp)] START $label"
  if "$@"; then
    echo "[$(timestamp)] DONE  $label"
  else
    local rc=$?
    echo "[$(timestamp)] FAIL  $label rc=$rc"
    failures=$((failures + 1))
  fi
}

if [[ "${SKIP_CONTAINER_WAIT:-0}" != "1" ]]; then
  waited=0
  until [[ "$($DOCKER_BIN inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null || true)" == "true" ]]; do
    if (( waited >= WAIT_SECONDS )); then
      echo "[$(timestamp)] container $CONTAINER unavailable after ${WAIT_SECONDS}s"
      exit 1
    fi
    sleep "$WAIT_INTERVAL"
    waited=$((waited + WAIT_INTERVAL))
  done
fi

# DWD is deliberately first: its per-cycle upstream window is only about 24 h.
run_step "DWD recent-init reconciliation" \
  "$PYTHON_BIN" "$OPERATION_DIR/reconcile_sources.py" --only-source dwd-icon

# Himawari commands are idempotent; their own realtime lock prevents overlap.
run_step "Himawari realtime catch-up" \
  /bin/bash "$OPERATION_DIR/hima-realtime.sh"

run_step "remaining NWP recent-init reconciliation" \
  "$PYTHON_BIN" "$OPERATION_DIR/reconcile_sources.py" --exclude-source dwd-icon

run_step "Himawari recent-day rebuild" \
  env REBUILD_DAYS="${REBUILD_DAYS:-5}" \
  LOG="${HIMA_REBUILD_LOG:-$OPERATION_DIR/logs/hima-rebuild.log}" \
  /bin/bash "$OPERATION_DIR/hima-rebuild-recent.sh"

run_step "GFS restore scan" \
  env SOURCE=gfs-0p25 /bin/bash "$OPERATION_DIR/restore_scan.sh"
run_step "AIFS restore scan" \
  env SOURCE=aifs-single JOB=config/jobs/aifs_zarr_china.yaml \
  LOCK="$OPERATION_DIR/lock/restore_scan_aifs.lock" /bin/bash "$OPERATION_DIR/restore_scan.sh"
run_step "IFS restore scan" \
  env SOURCE=ifs-hres JOB=config/jobs/ifs_zarr_china.yaml \
  LOCK="$OPERATION_DIR/lock/restore_scan_ifs.lock" /bin/bash "$OPERATION_DIR/restore_scan.sh"
run_step "DWD restore scan" \
  env SOURCE=dwd-icon JOB=config/jobs/icon_zarr_china.yaml \
  LOCK="$OPERATION_DIR/lock/restore_scan_icon.lock" /bin/bash "$OPERATION_DIR/restore_scan.sh"

run_step "ERA5 production reconciliation" \
  /bin/bash "$OPERATION_DIR/era5-production.sh"

echo "[$(timestamp)] boot recovery finished failures=$failures"
(( failures == 0 ))
