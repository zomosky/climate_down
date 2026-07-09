# deploy/ — scheduling the download step (NWP → GRIB subsets + manifest)

`download_run.sh` is a **host-side cron entry** that drives one
`climate_download run` *inside the long-running dev container* (`zhangmy-dev`).
It mirrors the restore side (`climate_restorage/deploy/restore_scan.sh`) — same
"host crontab → `.sh` → `docker exec`" pattern, same shared data root.

The two components need **no coordination**: download writes a
`*.manifest.json` per init; the restore `scan-once` cron sees it on its next
tick and slices it to Zarr. The manifest is the only signal.

## The host owns the date (why this is robust)

The script computes the **init date on the host** and passes `--date/--cycle`
explicitly to the tool. Consequences:

- **Container timezone is irrelevant.** We never rely on the tool's `today`
  (which resolves against the container's UTC clock); the only clock trusted is
  the host's, which you control.
- **No collision with historical runs.** The `*_ser.yaml` never pins a date, so
  a manual backfill running inside the container at the same time uses its own
  date and can't clash with the operational schedule.
- **Cron timing is forgiving.** It resolves to *the most recent cycle that has
  already published* (shift `now` back by `cycle + PUBLISH_LAG_HOURS` and take
  that UTC date). Whether the cron fires at 02:30 or 14:00 China time, it picks
  the same latest-available 12z — a late/extra run just re-resolves and resumes.

The only real requirement: run **after** the cycle publishes (GFS 12z ≈ 17:00
UTC = 01:00 China time). Running before merely re-fetches the previous day
(already on disk → resume no-op) until the new run is out.

## Server config lives in `*_ser.yaml` (local job files stay untouched)

The local `config/jobs/gfs_renewables.yaml` etc. are kept **unchanged** for local
backfills / validation. The server runs self-contained copies —
`config/jobs/*_ser.yaml` — which pin `steps: all`, `output_dir: /climate_data`,
and `cycle: 12`, but **not** a date (the host supplies it). Currently deployed,
both **12z only**:
`gfs_renewables_ser.yaml`, `dwd_icon_operation_renewables_ser.yaml`.

## Prerequisites

- The dev container (`zhangmy-dev`) is running and **stays up**.
- Download deps synced once inside it (runtime only — **no** `--extra viz`):
  ```sh
  docker exec -w /workspace/climate_down zhangmy-dev uv sync
  ```
- `/climate_data` exists and both components point at it.
- Host has GNU `date` (Linux — the script uses `date -u -d "N hours ago"`; a BSD
  fallback is noted inline).

## Install

1. Extract the script to the host and make it executable:
   ```sh
   docker cp zhangmy-dev:/workspace/climate_down/deploy/download_run.sh /srv/climate/download_run.sh
   chmod +x /srv/climate/download_run.sh
   mkdir -p /var/log/climate
   ```
2. Add to the host crontab (`crontab -e`). Times are **China local** (the host's
   timezone); pick any time after the cycle publishes — 02:30 China time is a
   safe default:
   ```cron
   # NOAA GFS 0.25° (renewable subset), 12z only
   30 2 * * *  /srv/climate/download_run.sh config/jobs/gfs_renewables_ser.yaml >> /var/log/climate/download_gfs.log 2>&1

   # DWD ICON global (operational, near-real-time only), 12z only
   50 2 * * *  /srv/climate/download_run.sh config/jobs/dwd_icon_operation_renewables_ser.yaml >> /var/log/climate/download_icon.log 2>&1
   ```
   Because the date is host-computed, extra "catch-up" lines are safe and cheap
   (they resolve to the same init and resume), e.g. add `30 5 * * *` for GFS.
   Other cycles = same script with `CYCLE=`:
   ```cron
   30 14 * * *  CYCLE=0 /srv/climate/download_run.sh config/jobs/gfs_renewables_ser.yaml >> /var/log/climate/download_gfs.log 2>&1
   ```

## Configuration (environment-overridable)

| var                 | default                   | meaning                                        |
|---------------------|---------------------------|------------------------------------------------|
| `CONTAINER`         | `zhangmy-dev`             | dev container name                             |
| `DOWNLOAD_DIR`      | `/workspace/climate_down` | download project dir inside the container      |
| `CYCLE`             | `12`                      | forecast cycle (UTC hour) — also names the lock/log |
| `PUBLISH_LAG_HOURS` | `5`                       | hrs after `CYCLE`:00 UTC when the run is fully out (date-boundary only) |
| `INIT_DATE`         | *(host-computed)*         | set `YYYYMMDD` to fetch a specific init (backfill) |
| `LOCK`              | `/tmp/download_<job>_<cc>z.lock` | per-(job,cycle) flock (in-container)     |

Output dir and steps are baked into the `*_ser.yaml`; date/cycle come from the
host (env above). Extra CLI args after the job path pass straight through.

## Behaviour & exit codes

- **Resume / idempotent**: valid GRIB already on disk is skipped; corrupt files
  are deleted and re-fetched.
- **Overlap-safe**: `flock -n -E 0` makes a second run of the same (job, cycle)
  exit 0 while the first is still going.
- **Container down** → exit 0 (skip); the next run catches up.
- **Exit code**: `0` ok, `1` partial (downgraded to 0 + logged — usually a step
  not yet published), `2` all-failed (propagated → alert on it in the log).

## Run it once by hand / backfill

```sh
# operational, right now (host computes the latest published 12z)
/srv/climate/download_run.sh config/jobs/gfs_renewables_ser.yaml

# a specific past init (GFS only — DWD keeps ~24 h)
INIT_DATE=20260701 CYCLE=12 /srv/climate/download_run.sh config/jobs/gfs_renewables_ser.yaml

# a range, straight through the CLI inside the container
docker exec -w /workspace/climate_down zhangmy-dev \
  uv run climate_download run --config config/jobs/gfs_renewables_ser.yaml \
    --date "20260701-20260707" --cycle 0,12 --no-progress
```
Check the `job_done` log line and that a `…_12z_gfs-0p25.manifest.json` appeared
under `/climate_data/gfs-0p25/<date>/12z/`.

## Retention (housekeeping)

`steps: all` for GFS is ~209 files (~6 GB) per 12z init. Once restore has sliced
an init to Zarr, its raw GRIB can be pruned:
```cron
30 4 * * *  find /climate_data/gfs-0p25 -mindepth 2 -maxdepth 2 -type d -mtime +7 -exec rm -rf {} + >> /var/log/climate/cleanup.log 2>&1
```
(Window must exceed the restore lag; the `.zarr` under
`climate_data_storage/zarr` is the durable product.)
