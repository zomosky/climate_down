#!/usr/bin/env sh
# download_run.sh — host-side cron entry for one climate_download job.
#
# The HOST decides which init to fetch and passes --date/--cycle explicitly, so:
#   * the container's timezone / clock is irrelevant (we trust only the host clock);
#   * the *_ser.yaml never pins a date, so a historical backfill running inside
#     the container can't collide with the operational schedule.
#
# It runs the job *inside the long-running dev container* (`zhangmy-dev`),
# writing per-step GRIB subsets + a per-init manifest.json under /climate_data.
# The restore side (climate_restorage/deploy/restore_scan.sh) slices each new
# manifest to Zarr on its next tick — the manifest is the only cross-component
# signal, so the two cron schedules need no coordination.
#
# Usage (from cron):
#   download_run.sh <job-yaml> [extra climate_download args...]
#
# Backfill a specific init (skips the host date computation):
#   INIT_DATE=20260701 CYCLE=12 download_run.sh <job-yaml>
#
# Example crontab line (times are CHINA local — see deploy/README.md):
#   30 2 * * *  /srv/climate/download_run.sh config/jobs/gfs_renewables_ser.yaml >> /var/log/climate/download_gfs.log 2>&1
set -eu

JOB="${1:?usage: download_run.sh <job-yaml> [extra climate_download args...]}"
shift

CONTAINER="${CONTAINER:-zhangmy-dev}"
DOWNLOAD_DIR="${DOWNLOAD_DIR:-/workspace/climate_down}"
CYCLE="${CYCLE:-12}"                       # which forecast cycle (UTC hour)
PUBLISH_LAG_HOURS="${PUBLISH_LAG_HOURS:-5}" # hrs after CYCLE:00 UTC when the run is fully out

# --- Decide the init date(s) on the HOST --------------------------------------
# Pick the most recent <cycle>z run that has already published: shift "now" back
# by (cycle + lag) hours and take that UTC date. This is robust to WHEN the cron
# fires — it always resolves to the latest available <cycle>z, so the exact cron
# time stops being critical (a late/extra run just re-resolves and resumes).
#
# LOOKBACK_DAYS>0 also re-verifies the previous N days' <cycle>z in the same run
# (as a date RANGE, which climate_download expands init-by-init). Combined with
# per-step resume this is a cheap self-heal: a run interrupted on an earlier day
# (container restart / network drop, no manifest written) is completed by the
# next day's run instead of being stranded. Complete inits skip in milliseconds.
if [ -n "${INIT_DATE:-}" ]; then
  DATE="$INIT_DATE"                        # explicit override (backfill)
else
  BACK=$(( CYCLE + PUBLISH_LAG_HOURS ))
  # GNU date (Linux host). BSD/macOS host: use `date -u -v-"${BACK}"H +%Y%m%d`.
  END=$(date -u -d "${BACK} hours ago" +%Y%m%d)
  if [ "${LOOKBACK_DAYS:-0}" -gt 0 ]; then
    START=$(date -u -d "$(( BACK + LOOKBACK_DAYS * 24 )) hours ago" +%Y%m%d)
    DATE="${START}-${END}"                 # range: re-verify recent inits + latest
  else
    DATE="$END"
  fi
fi

NAME="$(basename "$JOB" .yaml)_${CYCLE}z"
LOCK="${LOCK:-/home/zhangmingyu/operation/lock/download_${NAME}.lock}"   # ON THE HOST

# flock creates the lock FILE but not its parent dir — ensure the dir exists on
# the host so a custom LOCK path doesn't fail with "No such file or directory".
mkdir -p "$(dirname "$LOCK")"

# Container down -> clean skip (exit 0); the next run resumes, losing nothing.
if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  echo "download_run: container '$CONTAINER' not running; skipping $NAME." >&2
  exit 0
fi

echo "download_run: $NAME -> init ${DATE} ${CYCLE}z (host-computed UTC)"

# flock -n -E 0 ON THE HOST: if a previous run of THIS (job, cycle) is still
# going, exit 0 instead of stacking a second downloader on the same init.
# (-E needs util-linux >= 2.27 on the HOST; drop it if the host's flock is older.)
rc=0
flock -n -E 0 "$LOCK" \
  docker exec -w "$DOWNLOAD_DIR" "$CONTAINER" \
    uv run climate_download run --config "$JOB" \
      --date "$DATE" --cycle "$CYCLE" --no-progress "$@" \
  || rc=$?

# climate_download exit codes: 0 all-ok, 1 partial, 2 all-failed.
# Partial is usually benign (a step not yet published) — log it but do not fail
# the cron line, so monitoring only alerts on a true all-failed run.
case "$rc" in
  0) echo "download_run: $NAME OK" ;;
  1) echo "download_run: $NAME PARTIAL (some steps missing/failed — often a late publish; a later run fills them)" >&2; rc=0 ;;
  2) echo "download_run: $NAME ALL-FAILED — investigate (upstream outage? disk full?)" >&2 ;;
  *) echo "download_run: $NAME exited $rc" >&2 ;;
esac
exit "$rc"
