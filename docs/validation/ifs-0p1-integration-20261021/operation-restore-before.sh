#!/usr/bin/env sh
# restore_scan.sh — host-side cron entry for the climate_restore slicing step.
#
# Structure mirrors download/deploy/download_run.sh: the HOST cron drives the
# long-running dev container (`zhangmy-dev`) via `docker exec`. The one difference
# from a plain container job is the lock placement:
#
#   host crontab -> flock (lock ON THE HOST) -> docker exec zhangmy-dev -> scan-once
#
# i.e. flock wraps `docker exec` on the host side, so the lock file lives on the
# host — not inside the container. This keeps overlapping cron ticks from stacking
# two scans while the host `docker exec` client is alive (it stays attached until
# the in-container run exits). scan-once is idempotent anyway, so the lock is a
# resource guard, not a correctness requirement.
#
# It idempotently sweeps every *.manifest.json under DATA_ROOT (inside the
# container) and turns each finished init into a per-source Zarr store. Re-runs are
# cheap: already-built outputs are skipped (keyed off the on-disk .zarr).
#
# Install (host crontab), every 10 minutes:
#   */10 * * * * /srv/climate/restore_scan.sh >> /var/log/climate/restore_scan.log 2>&1
#
# Prerequisites:
#   - the dev container is running and stays up (cron drives it via `docker exec`)
#   - the cron user can run docker (in the `docker` group or root)
#   - restore deps synced once inside it:
#       docker exec -w /workspace/climate_restorage zhangmy-dev uv sync
#
# Any value below can be overridden from the environment if your layout differs.
set -eu

CONTAINER="${CONTAINER:-zhangmy-dev}"
RESTORE_DIR="${RESTORE_DIR:-/workspace/climate_restorage}"        # inside the container
DATA_ROOT="${DATA_ROOT:-/climate_data}"                           # inside: common parent of all sources
OUT_DIR="${OUT_DIR:-/climate_data/climate_data_storage/zarr}"     # inside the container
JOB="${JOB:-config/jobs/gfs_zarr_china.yaml}"                     # relative to RESTORE_DIR (inside)
SOURCE="${SOURCE:-gfs-0p25}"                        # empty = scan all sources; e.g. gfs-0p25 to scope one
LOCK="${LOCK:-/home/zhangmingyu/operation/lock/restore_scan.lock}"      # ON THE HOST (flock runs host-side, see below)

# flock creates the lock FILE but not its parent dir — ensure the dir exists on
# the host so a custom LOCK path (e.g. /home/<user>/operation/restore_scan.lock)
# doesn't fail with "No such file or directory".
mkdir -p "$(dirname "$LOCK")"

# Container down -> clean skip (exit 0); the next tick resumes, losing nothing.
if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  echo "restore_scan: container '$CONTAINER' not running; skipping this tick." >&2
  exit 0
fi

SRC_ARG=""
[ -n "$SOURCE" ] && SRC_ARG="--source $SOURCE"

# flock -n -E 0 ON THE HOST: if a previous scan is still running, exit 0 instead
# of stacking a second one (two in-place .zarr rewrites of one init would corrupt
# it). `-E 0` needs util-linux >= 2.27 on the HOST; drop it if the host's flock is
# older.
rc=0
flock -n -E 0 "$LOCK" \
  docker exec -w "$RESTORE_DIR" "$CONTAINER" \
    uv run climate_restore scan-once \
      --download-root "$DATA_ROOT" \
      --output-dir    "$OUT_DIR" \
      --output-format zarr \
      $SRC_ARG \
      --config "$JOB" \
  || rc=$?

# scan-once exit codes: 0 = all processed/skipped (incl. container down or lock
# held by a concurrent run), 1 = at least one *ready* manifest failed to process.
case "$rc" in
  0) echo "restore_scan: OK" ;;
  1) echo "restore_scan: FAILED — a ready manifest failed to process; check the log" >&2 ;;
  *) echo "restore_scan: exited $rc" >&2 ;;
esac
exit "$rc"
