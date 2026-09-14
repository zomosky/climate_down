# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Scope of this file

This is the **`download/` sub-project** (package `climate_download`, **Python 3.11**) of the
`climate_pipeline` monorepo. The monorepo-level `../AGENTS.md` covers how the three projects
connect (download → climate_restorage); read it for cross-project context. **This file is the
download deep-dive.** Authoritative spec is the (Chinese) `README.md` / `PROJECT.md` here.

Note: `download/` is its own git repository (`main` branch), even though the monorepo root is not.

## Commands

All commands run from inside `download/` via `uv`.

```bash
uv sync                       # runtime deps only (httpx/pydantic/pyyaml/structlog/tenacity/tqdm)
uv sync --extra viz           # + cfgrib/eccodeslib/xarray/matplotlib/numpy (for examples/ plots)
uv sync --extra s2s           # + cdsapi (only for the s2s subcommand)

uv run pytest                 # full suite — offline, all HTTP mocked with respx
uv run pytest tests/test_index.py
uv run pytest tests/test_jobs.py::<test_name>     # single test

uv run climate_download list-sources                                   # registry self-check
uv run climate_download run --config config/jobs/gfs_renewables.yaml \
    --date 20260101 --cycle 0,12 --steps all                          # CLI overrides time.*
uv run climate_download list-steps     --source gfs                    # forecast hours published
uv run climate_download list-variables --source gfs --yaml            # scaffold a variables: block
uv run climate_download s2s --config config/jobs/s2s_renewables_ecmwf.yaml
```

`run` exit codes: all steps OK `0`, partial `1`, all-failed `2` (`jobs.JobOutcome.all_failed`).
`pytest` config (`asyncio_mode=auto`, `testpaths=["tests"]`) lives in `pyproject.toml`; no network
or system eccodes is ever required — `eccodeslib` ships the binary `libeccodes`.

## Architecture (the parts that span files)

### Two-layer config: source YAML + job YAML
- **`config/sources/<name>.yaml`** — *who/where*: a `type:` discriminator + that adapter's URL
  templates. Reusable across jobs.
- **`config/jobs/<name>.yaml`** — *what/when*: references a source (by `source: <name>` string,
  resolved against `../sources/<name>.yaml`, or inlined as a mapping with `type:`), plus
  `variables:` groups, `time:`, and `download:` knobs.
- Loading: `config.py:load_job` / `load_source` → `load_source_dict` pops `type:` and dispatches
  to the registered adapter's pydantic model (`JobConfig._coerce_source` does the same for inline
  sources). All models are `extra="forbid"`, so a misspelled YAML key is a validation error.

### Source plugins are THE extension point (`src/climate_download/sources/`)
A source = one adapter file + one `@register("name")` + one import line in `sources/__init__.py`.
**No edits to `config.py`, `jobs.py`, or `manifest.py` are ever needed** — the orchestrator only
calls the `Source` protocol (`sources/base.py`).

- `base.py` defines the `Source` Protocol and `BaseSource` mixin. Concrete adapters subclass
  `BaseSource, BaseModel` — pydantic side carries the per-source schema (templates), `BaseSource`
  supplies the shared byte-range behaviour (`probe_step` HEADs the sidecar, `download_step` merges
  ranges and calls `PartialDownloader`, `list_available_variables` projects `fetch_records`).
- `registry.py` — `@register(name)` populates `SOURCE_REGISTRY`; **registering a name twice raises**
  (guards against typo collisions). `get_source` / `list_sources` are the lookups.
- **Live registry** (`uv run climate_download list-sources`): `aifs`, `gfs`, `hrrr`, `icon`, `ifs`.
  `graphcast` / `graphcast-merged` are **retired** (commit `ef29274`) — see Gotchas before touching
  anything named graphcast.
- **Adapters are protocol-bound, not model-bound.** If a new source shares an existing source's
  bucket dialect + sidecar format, just stack another `@register("alias")` on the existing class —
  that is how `ifs` aliases `AifsSource` (locked in by
  `tests/test_sources.py::test_protocol_aliases_share_implementation`). And if the *URL shape* is
  unchanged too, no code is needed at all: **a second `config/sources/*.yaml` under the same `type:`
  is a new source.** `gfs-gcs.yaml` is exactly that — `type: gfs` pointed at Google's
  `storage.googleapis.com/global-forecast-system` mirror instead of `noaa-gfs-bdp-pds.s3`, used by
  `gfs_backfill_gcs.yaml` because the GCS mirror is much faster from China (its header comment still
  says "AWS S3" — copy-paste leftover; the URL templates are what count). URL quirks (e.g. HRRR's
  per-product basename) justify a new file even when the parser is shared — `hrrr.py`.
- To add a genuinely new protocol: write `class X(BaseSource, BaseModel)` overriding
  `build_index_url` / `build_data_url` / `fetch_records` (+ `list_available_steps` if the bucket is
  S3/GCS-listable), `@register` it, add the import in `__init__.py`, add a `config/sources/*.yaml`.
  Whole-file (non-byte-range) sources also override `download_step` — see `README.md` §5.4 and the
  custom-source test in `test_sources.py`. **`icon.py` (DWD ICON, `type: icon`) is the built-in
  whole-file example**: no sidecar at all (one bz2 GRIB file *per variable per step*), so
  `fetch_records` projects a **static catalogue** (`DEFAULT_ICON_RENEWABLE_CATALOG`, overridable via
  a YAML `catalog:` block) into synthetic records, and `download_step` fetches each selected file,
  `bz2`-decompresses it, and concatenates the messages into one GRIB2 — bypassing `PartialDownloader`
  entirely. `list_available_steps` scrapes the DWD Apache autoindex HTML (not the S3/GCS XML helper).

### Sidecar dialects (`grib/index.py`)
ECMWF/NOAA sources read a per-step sidecar; DWD ICON has none (see the whole-file note above).
Both sidecar parsers emit `IndexRecord`s so everything downstream is source-agnostic:
- **ECMWF JSONL `.index`** (`parse_index_text`, used by `AifsSource`): one JSON object per message
  with `_offset`/`_length`. URL is one `url_template` with a `{suffix}` placeholder swapped between
  `index` and `grib2`.
- **NOAA wgrib2 `.idx`** (`parse_wgrib2_idx_text`, used by `GfsSource`/`HrrrSource`): colon-separated
  text with **no length field** → the adapter HEADs the GRIB URL for `Content-Length` and infers
  each message length from the next offset (last = `total_size - offset`). Split index/data URL
  templates because the `.idx` and GRIB suffixes are unrelated.
- wgrib2 levels are classified into `levtype` by `_classify_wgrib2_level`:
  `pl`/`hag`(above ground)/`hbg`(below ground/soil)/`atm`(MSL, column, tropopause…)/`sfc`/`other`.
  This is GFS-only and matters when writing `variables:` groups (e.g. cloud-layer slabs → `other`).

### The download path (`jobs.py`)
`run_job` → one *init* per `(date × cycle)` cartesian product; inits run concurrently
(`download.init_concurrency`, default 2), **steps within an init run sequentially** (keeps the
per-init manifest boundary simple). Per step (`_run_one_step`):
1. `_resume_check` — if the local file already starts `GRIB` and ends `7777` (reads only 8 bytes),
   skip the download; an existing-but-corrupt file is deleted and re-fetched.
2. `source.fetch_records` → `select_records` (one `IndexFilter` per `VariableGroup`, results
   concatenated, deduped by `id()`) → `merge_ranges` (coalesce byte ranges, `gap_tolerance`) →
   `source.download_step` (`PartialDownloader`, threaded `Range` GETs) → `_validate_grib`.
- **Steps in a requested range that the bucket doesn't publish are HEAD-probed (`probe_available_steps`)
  and skipped with a warning — never a failure.** So wide ranges like `0-360` are safe for backfill.
- `steps: all` → `expanded_steps()` returns `None` → orchestrator enumerates via
  `source.list_available_steps` (returns `None`/skips with a warning if the source can't list).
- Fault tolerance: per-step / per-init exceptions are captured into `JobFailure` (with a `phase`:
  probe/list/index/select/download/validate/manifest), never raised — a long backfill keeps going.
  `run_job` returns `JobOutcome(succeeded, failed)` and writes `{output_dir}/_runs/run_<ts>.json`.

### Bucket listing (`sources/_listing.py`)
`list_remote_steps` powers `steps: all` and the `list-steps` CLI. It renders the index template with
the real `(date, cycle)`, leaves `{step}` as a marker, then pages an XML LIST endpoint. Two backends
with nuisance differences are normalized: **S3 v2** (`?list-type=2`, `continuation-token`, bucket in
host) vs **GCS** (`marker`, bucket in first path segment); XML namespaces are stripped before walking.
Returns `None` for an unrecognized host (caller falls back to a static range).

### Manifest = the cross-project ready signal (`manifest.py`)
One `*.manifest.json` per `(source, date, cycle)`, written **atomically** (`tmp` + `os.replace`)
by `_run_init` after the steps complete. It is the ONLY signal `climate_restorage` watches — never
the `_runs/` audit reports. Written if ≥1 step succeeded **or** there are failures; carries a
top-level `failures: []` array so a consumer can tell "step never attempted" from "attempted but
errored". Each file entry has a `sha256`. Default path sits beside the GRIB in the per-init subdir.

`write_manifest` is a **no-op when the product signature is unchanged** — `_product_signature` is
each step's `(step_hours, filename, sha256)` plus each failure's `(step, phase)`; `completed_at` and
the per-file download stats are deliberately excluded because a resume-only re-run changes them
without changing what a consumer would build. Leaving the mtime untouched is a cross-project
contract: restore's `scan-once` rebuilds when the manifest is newer than the Zarr, so an extra
catch-up cron pass that found nothing new must not re-trigger a rebuild.

### Time expansion (`config.py:TimeConfig`)
`date`, `cycle`, `steps` each accept single value / list / range-mapping / short-string:
- `date: "20260501-20260507"` (range), `["20260501","20260502"]`, or `today`/`yesterday`
  (resolved against **UTC at expansion time**).
- `steps: "0-120:6"` or MARS-style `"0/120/6"` or `"all"` or `[0,6,12]` or `6`.
- `cycle` validated to one of `0/6/12/18`. CLI `--date/--cycle/--steps` re-validate through
  `TimeConfig` so they accept the same syntax (`cli.py:cmd_run`).

### Retry contract (`sources/_http.py` + `grib/partial.py`)
**All metadata HTTP (probe HEAD / sidecar GET / bucket LIST) must go through
`request_with_retry`**; byte-range payload fetches go through `PartialDownloader`'s own tenacity
loop. Both share `max_attempts=4` and the same trigger set: `httpx.TransportError` (incl. SSL EOF) /
`TimeoutException` / status 408/425/429/5xx. `PartialDownloader` also verifies each range's byte
length and accepts only 200/206.

**Never build a bare `httpx.Client()` in library code — use `_http.build_client`**, which disables
connection keepalive (`limits=httpx.Limits(max_keepalive_connections=0)`); `PartialDownloader` sets
the same limit on the client it owns. Why (commit `e142e0e`): after a network blip a pooled half-open
connection gets handed to the next request and dies with `SSL: UNEXPECTED_EOF_WHILE_READING` /
`RemoteProtocolError`, and once every keepalive slot holds a dead connection the pool wedges on
`PoolTimeout` — a "dead pool" that never self-heals until the process is restarted. Downloads are
bandwidth-bound (~1 MB/s international egress), so the extra TCP+TLS handshake per request (~200 ms)
is noise. Pinned by `test_sources.py::test_build_client_disables_keepalive`.

### S2S is a separate pipeline (`src/climate_download/s2s/`)
Sub-seasonal forecasts from ECMWF Data Store use **`cdsapi` submit-poll-download**, not byte-range:
no `.index` sidecar (server-side subsetting via form fields), one `retrieve` per variable group
returns a multi-message GRIB. It has its own config/jobs/manifest/orchestration (mirroring the main
`JobOutcome`/`_runs` shapes) and is reached via the `s2s` subcommand, which **lazily imports cdsapi**
so the rest of the CLI works without the `[s2s]` extra. Credentials: `~/.ecdsapirc`.

### Production scheduling lives in `deploy/` + `config/jobs/*_ser.yaml`
`deploy/download_run.sh` is a **host-side** cron entry that `docker exec`s one `climate_download run`
inside the long-running `zhangmy-dev` container — the same shape as
`climate_restorage/deploy/restore_scan.sh`. The two crons need no coordination: download writes a
manifest, restore's next tick sees it. Two conventions to preserve:
- **The host owns the date.** The script resolves the init on the host (shift `now` back by
  `CYCLE + PUBLISH_LAG_HOURS`, take that UTC date) and passes `--date/--cycle` explicitly, so the
  container's clock/timezone is irrelevant and a manual backfill running inside the container can't
  collide with the operational schedule. `LOOKBACK_DAYS=N` re-verifies the previous N days in the
  same run — cheap thanks to per-step resume, and it is what heals an init that died mid-run on an
  earlier day. Exit 1 (partial) is downgraded to 0 so monitoring only alerts on exit 2.
- **Server jobs are separate files.** `config/jobs/*_ser.yaml` pin `steps: all`,
  `output_dir: /climate_data` and `cycle: 12` but **never a date**; the plain `*.yaml` twins stay
  untouched for local backfill/validation. Changing what the server downloads means editing the
  `_ser` twin (and usually both).
`deploy/README.md` holds the per-source publish windows / `PUBLISH_LAG_HOURS` table, the crontab, and
the GRIB retention sweep — read it before changing anything about scheduling.

## Gotchas (download-specific)

- **`levels` is an AND filter**, so surface (`sfc`) and pressure-level (`pl`) params must be in
  **separate `variables:` groups** — one group is `levtype` + `params` (+ optional `levels`), all ANDed.
- GFS forecast hour is zero-padded `{step:03d}` (f000…f384); steps appear as ints everywhere else.
- Radiation fluxes (DSWRF/USWRF/…) are time-averaged and published **from f001 onward — absent in
  the f000 analysis**. Accumulated vars (APCP/tp/ssrd…) are 0 at step 0 by definition (diff two steps).
- ECMWF (`.index`) ships no human level descriptor, so `VariableInfo.level_desc` / `IndexRecord.level_desc`
  is `None` for AIFS/IFS but populated for GFS/HRRR (the wgrib2 string).
- Default output layout is `{output_dir}/{source}/{date}/{cycle:02d}z/f{step:03d}.subset.grib2`
  (`DownloadConfig`); the committed `gfs_renewables.yaml` etc. point `output_dir` at an absolute
  monorepo path — override with `--output-dir` for local runs.
- A job's `source: <name>` resolves to `config/sources/<name>.yaml` by **filename stem**, not the
  source's `name:` field (e.g. `source: icon_global` → `icon_global.yaml`, whose `name:` is `icon-global`).
- **DWD ICON specifics**: global ICON is on the **icosahedral grid** (not lat/lon), so the
  downstream China-bbox crop in `climate_restorage` will not work unmodified (needs regridding).
  DWD retains only the latest run or two, so ICON is **near-real-time only — no historical backfill**.
  Global ICON has no pressure levels and no single-level 80/100 m wind, so the renewable set is
  10 m wind + near-surface fields (`icon_renewables.yaml`). PV GHI ≈ `ASWDIR_S + ASWDIFD_S`.
- **`graphcast` / `graphcast-merged` are retired but still on disk.**
  `sources/graphcast_merged.py` still carries `@register("graphcast-merged")`, but the module is no
  longer imported from `sources/__init__.py`, and `gfs.py` no longer carries `@register("graphcast")`
  — so **neither type is in the runtime registry**, and the still-shipped
  `config/sources/graphcast_{history,merged}.yaml` + `config/jobs/graphcast_2025_*.yaml` now fail
  with `unknown source type`. **No test catches this** (`test_shipped_source_yaml_loads` only
  parametrizes aifs/ifs/gfs/hrrr). To revive: re-add the `sources/__init__.py` import line and/or the
  `@register("graphcast")` on `GfsSource` — the adapters and YAML were kept precisely for that.
- `docs/weathernext2/` is **exploration only**: Google WeatherNext 2 Mean is native Zarr on a private
  (auth-gated) GCS bucket, so it does not fit the GRIB + byte-range pipeline at all. The notes and a
  validated reader prototype are parked there; nothing is registered as a source or job.

## Testing conventions

- **Offline by design**: HTTP is mocked with `respx` (`test_sources.py`, `test_partial.py`,
  `test_jobs.py`). Never add a test that hits the network.
- `conftest.py` fixtures (`aifs_index_path` / `aifs_grib_path`) read a sample from
  `../cliamte_data/aifs/` (note the recurring `cliamte_data` typo) and **`pytest.skip` when it is
  absent**, so a fresh checkout still passes.
- `test_s2s_catalogue.py` enforces that every `config/jobs/s2s_renewables_*.yaml` is a subset of the
  cached ECDS constraints snapshot `config/s2s/_capabilities.json`. Refresh the snapshot with
  `scripts/build_s2s_catalogue.py` when ECDS revs its constraints hash, then re-run the test.
- `test_sources.py::test_shipped_source_yaml_loads` is **parametrized by source-file stem** — add a
  row when you ship a new `config/sources/*.yaml`, otherwise a YAML whose `type:` is unregistered
  ships green (that is how the retired graphcast configs slipped through).
- Baseline: `uv run pytest -q` → **102 passed, 1 skipped, ~5 s** (the skip is the AIFS sample fixture).


## 部署服务器
本处理管道：climate_pipeline最终以两个docker部署在服务器上目前为zhangmingyu@192.168.1.241需要注意部署docker时的内存和cpu限制，防止耗完资源。
这两个docker项目为两种：
一个是zhangmy-dev，此放下载和处理气象数据/卫星数据的项目，项目更新使用git，挂载宿主机的/training_data/climate_data和/training_data/satelite_data，作为下载后的存储。下载切割的定时任务则通过宿主机的/home/zhangmingyu/operation/下的sh配合crontab调用。
另一个对外服务为climate_zrread和climate_watcher，项目直接git到/home/zhangmingyu/src/pipeline下

相关的git仓库：
https://g.huamod.com/mingyu.zhang/climate_watcher.git
https://g.huamod.com/mingyu.zhang/climate_zrread.git
https://g.huamod.com/mingyu.zhang/climate_restorage.git
https://g.huamod.com/mingyu.zhang/climate_down.git
https://g.huamod.com/mingyu.zhang/hima-download.git
