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
and `cycle: 12`, but **not** a date (the host supplies it). Currently shipped,
all **12z only**: `gfs_renewables_ser.yaml`,
`dwd_icon_operation_renewables_ser.yaml`, `ifs_renewables_ser.yaml`.

## Publish timing & recommended schedule (China time, 12z run)

Each source becomes fully available at a different time after the 12:00 UTC init,
so each needs its own China-time cron window and `PUBLISH_LAG_HOURS` (which tells
the host date logic when the run is "out"). Times below are for the **12z** run;
China time = UTC+8, so the data lands early next China morning.

| source (`source.name`)   | 12z full ≈ (UTC) | ≈ China time | first run | catch-up | `PUBLISH_LAG_HOURS` | `*_ser.yaml` |
|--------------------------|------------------|--------------|-----------|----------|---------------------|--------------|
| `dwd-icon-operation`     | ~16:00           | ~00:00 +1d   | 02:30     | 04:30    | 5 (default)         | shipped |
| `gfs-0p25`               | ~17:00           | ~01:00 +1d   | 02:30     | 06:30 (+04:30 optional) | 5 (default) | shipped |
| `ifs-hres`               | ~21:30           | ~05:30 +1d   | 06:30     | 08:30, 10:30 | 10               | shipped |
| `aifs-single` *(est.)*   | ~19:00–20:00     | ~03:00–04:00 +1d | 04:30 | 06:30    | 8                   | make one |
| `graphcast` / aigfs *(est.)* | ~17:00–18:00 | ~01:00–02:00 +1d | 03:00 | 05:00    | 6                   | make one |

- **Verified**: GFS (NOAA S3 ~+5 h), DWD ICON (~+4 h), ECMWF IFS (real-time
  dissemination +7.5 h, then +2 h for the 0.25° open-data stream ⇒ ~+9.5 h).
- **Estimates** (AIFS, GraphCast) — *verify before trusting*: run
  `uv run climate_download list-steps --source <name> --cycle 12` at a few times
  and watch when the full step list appears; then set the first-run time just
  after that and `PUBLISH_LAG_HOURS ≈ (that UTC hour − 12)`.
- AIFS/GraphCast need their own `*_ser.yaml` first (copy the `gfs`/`ifs` one,
  swap `source:` + variable groups). GraphCast on the China-relevant grid also
  needs the aigfs pressure/surface split — see the local `graphcast_*` jobs.
- HRRR is CONUS-only and S2S is a separate sub-seasonal pipeline — neither is
  scheduled here.

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
   # NOAA GFS 0.25° (renewable subset), 12z only. 02:30 is already past full
   # publish (~01:00 China); 06:30 is the delay/interruption catch-up (re-resolves
   # to the same 12z and resumes). LOOKBACK_DAYS=2 (first line) self-heals a day
   # missed earlier. Add a 04:30 line too if you want tighter delay coverage.
   30 2 * * *  LOOKBACK_DAYS=2 /home/zhangmingyu/operation/download_run.sh config/jobs/gfs_renewables_ser.yaml >> /home/zhangmingyu/operation/logs/download_gfs.log 2>&1
   30 6 * * *                  /home/zhangmingyu/operation/download_run.sh config/jobs/gfs_renewables_ser.yaml >> /home/zhangmingyu/operation/logs/download_gfs.log 2>&1

   # DWD ICON global (operational, near-real-time only), 12z only — 2 tries.
   # No LOOKBACK_DAYS — DWD only keeps ~24 h, so older days can't be re-fetched.
   50 2 * * *  /home/zhangmingyu/operation/download_run.sh config/jobs/dwd_icon_operation_renewables_ser.yaml >> /home/zhangmingyu/operation/logs/download_icon.log 2>&1
   50 4 * * *  /home/zhangmingyu/operation/download_run.sh config/jobs/dwd_icon_operation_renewables_ser.yaml >> /home/zhangmingyu/operation/logs/download_icon.log 2>&1

   # ECMWF IFS-HRES 0.25° (oper/fc, 00z+12z; this is 12z). Publishes ~+9.5 h,
   # much later than GFS — hence the morning window + PUBLISH_LAG_HOURS=10.
   30 6  * * *  PUBLISH_LAG_HOURS=10 LOOKBACK_DAYS=2 /home/zhangmingyu/operation/download_run.sh config/jobs/ifs_renewables_ser.yaml >> /home/zhangmingyu/operation/logs/download_ifs.log 2>&1
   30 8  * * *  PUBLISH_LAG_HOURS=10                 /home/zhangmingyu/operation/download_run.sh config/jobs/ifs_renewables_ser.yaml >> /home/zhangmingyu/operation/logs/download_ifs.log 2>&1
   30 10 * * *  PUBLISH_LAG_HOURS=10                 /home/zhangmingyu/operation/download_run.sh config/jobs/ifs_renewables_ser.yaml >> /home/zhangmingyu/operation/logs/download_ifs.log 2>&1
   ```
   See **Publish timing & recommended schedule** above for the per-source times /
   `PUBLISH_LAG_HOURS`. GFS 12z lines drop to 3 tries only for delay insurance; a
   day interrupted earlier is covered by `LOOKBACK_DAYS=2` on the first line.
   Multiple runs are **safe end-to-end**: an early/delayed run may write a
   manifest with only the steps published so far; when a later run adds the rest,
   restore's `scan-once` sees the manifest is newer than the Zarr and rebuilds it
   (`_output_fresh` in `climate_restorage/src/climate_restore/cli.py`). The
   download side skips rewriting the manifest when nothing changed (unchanged
   product signature), so a no-op re-run triggers no rebuild. Keep it to a few
   tries. Other cycles = same script with `CYCLE=`:
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
| `LOOKBACK_DAYS`     | `0`                       | also re-verify the previous N days' `CYCLE`z this run (self-heal; resume makes it cheap). GFS: use `2`; DWD ICON: keep `0` (no history) |
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

## Recovery from an interrupted download

Resilience is layered — nothing is re-downloaded that already completed:

1. **Transient errors** (SSL EOF / timeout / 408·429·5xx) retry 4× with
   exponential backoff, per request. A short network blip never fails a step.
2. **One bad step/init** is captured, not fatal — the rest of the run continues.
3. **Re-run resumes**: a step whose local GRIB is already valid (`GRIB…7777`) is
   skipped in milliseconds; a truncated/corrupt file is deleted and re-fetched.
   So re-running the *same* `(date, cycle)` only fills the gaps.

The only thing a single daily cron can't do alone is **re-target an init that
was interrupted on a previous day** (the next day's run points at a new date).
Two ways to cover that, both cheap because of resume:

- **`LOOKBACK_DAYS=2`** on the GFS cron line (recommended): each run also
  re-verifies the last 2 days' 12z. A day that died mid-run (even before any
  manifest was written) is completed by the next run automatically.
- **Extra same-day catch-up line** (e.g. `30 5 * * *`): re-resolves to the same
  latest 12z and finishes it, in case the first run was cut short.

How it heals end-to-end: an interrupted run leaves partial GRIB (and either no
manifest, or one carrying `failures`). Restore **skips** such an init (its
`scan-once` requires `completed_at` set and `failures` empty), so no partial
Zarr is ever built. When a later run completes the init, it atomically rewrites
a clean manifest → restore's next tick builds the full Zarr. DWD ICON is the
exception: it keeps only ~24 h upstream, so keep `LOOKBACK_DAYS=0` there and
rely on same-day catch-up only.

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
