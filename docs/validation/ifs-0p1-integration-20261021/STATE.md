# Completed execution state

User requested Google IFS0p1 integration while preserving0p25, then explicitly scoped acceptance to20261020 12UTC only. That init has passed full pipeline, native-value, API/export, UI and production-operation readback. No other init is part of this acceptance. Normal future00/12 scheduling is now active.

## Verified result

- Download85 native leads,5142525089bytes,5976.932s including manifest generation; all85SHA256 correct, independent raw readback2.676s.
- Restore110.743s; core elapsed6094.501s=101.575min; ended2026-10-21T09:46:50.067476Z.
- Native audit:30variables,2458canonical planes,401x701 native0.1 grid, everyChina cell exact vs independent ecCodes; init/validtime,paramIds,units and missing positions correct. Native audit78.043s.
- API16cases passed; original native request0.3936s,15min2sites8vars0.7143s. Each CSV/Parquet/Arrow/NetCDF variable value read back, warnings preserved, native grid orientation,train/dpn,mix,weightedprovince,monitor,metadata/catalog passed.
- Native strict-parameter coverage:27variables all85;fg10_max(param49,10fg)67 samples, missing93..144;two3h temperature extrema48 samples each(3..144),missing0and150..360. All18gust indices93..144 have10fg3 instead; decodedf09310fg3 isparam228028,last3h90..93. At150 source hasmx2t6/mn2t6. These three separate parameters are NOT in the unchanged30-variable job; not filled into existing fields. See upstream-gust-full-interval.json,gust3-field-definition.json,README.md.
- Source-specific fixes:round coordinates toGRIB microdegrees beforecrop;dropf000twozero+3h extrema previews bycanonicalfilenamelead. Raw0p25adapter unchanged.
- Producer cgroup5s sampled restore working-set peak8.515GiB(includes other producer jobs);OOMkill delta0. Limits remain~18CPU/36GiB producer,12CPU/24GiB service. Effective memory guard excludes inactive_file cache.

## Actual deployment

- Producer/workspace newsource/configs and adapter deployed with hashes/backups under /logs/ifs-0p1-release-20261021. Full timing/logs/native audit under /logs/ifs-0p1-integration-20261021.
- New raw /climate_data/ifs-hres-0p1/20261020/12z; published /climate_data/climate_data_storage/zarr/ifs-hres-0p1/20261020_12z_ifs-hres-0p1.zarr. Host mount parent /training_data/climate_data. Zarr591392808bytes.
- Live reader image sha256:5ce85582d71a44d5f7822b15c89471915fb4f9bbb7e18f1da86677c9f26b8c6c(tagclimate-watcher:ifs0p1-20261021). Rollback climate-watcher:pre-ifs0p1-20261021(baseb18949804aeb44f44e3a9f71e3afbf12e42911a8dbb953fe75696440374ad85e). No frontend or dependency changes. Existing settings/weights unchanged;0p1 inherits IFS profile,12h provisionaldelay/default12z.
- Host /home/zhangmingyu/operation/releases/ifs-0p1-20261021 contains source/config/service baseline,crontab-before,script backups and all acceptance attempts.
- New operation reconcile_source policies00/12,lag12h,lookback1d,first_init2026102012. Existing policies unchanged. resourceguardrestore_ifs_0p1.py delegates oldsource-scoped scanner with2workerjob andindependentlock.
- Cron active17:55:26BJT:12z06:05/07:35,00z18:05/19:35;all use lag10/lookback0;restore every10min. Oldcrontablines and olddownload/restore/bootwrapper hashes preserved. Existing systemdtimer/bootrecovery enabled.
- verify_runtime.py PASSED:old0p25query96rows exact original hash409b5aff844303ed7023f689ce592e5e859d52c405cc6468f9ae1275431a0321;health/homepage/assets good;limits nominal18/12 intact(producer rawNanoCpus17999998976 rounding);cronblock andpriorlines unchanged;reconcileHEALTHY20/12;newrealrestorewrapper idempotent,noZarrmetadata mtimechange.
- No coldmove or raw deletion enabled. NoGitcommit/push;preserved unrelated existing changes in allrepos.

## Acceptance trace and tools

- Initial timing runnerPID625937 used raw cgroup usage including filecache. One-shotfinish_firstPID629219 handed off ONLY its waiting_memory phase after download/SHA, preserved timing, wait4.149s. Both finished. Resource samplerPID632398 finished.
- Acceptance attempt1 immediate postpublication404 fits10s catalog cache default; same request later200, no service change. Attempt2 native/flux/missingness/15minHTTP succeeded; harness incorrectly assumed PandasDatetimeIndex.asi8 alwaysns. Fixed physicalTimedelta comparison. Attempt3 reused successfulJSON and original HTTPtimes, then passed remainingAPI and fullnativeaudit at09:54:50.845806Z. All attempts/logs saved. No data re-download/restore due to harness issues.
- Canonical final acceptance hostrelease/acceptance.json statusok attempt3. Old readonlywatch session61268 stopped after attempt1failure; final progress.jsonl/runtime/evidence show success. No still-active task-owned control process required; futureproductioncron isintentional.
- CUA nativeAX calls timedout. Recovered with documented browser.tabs.get plus DOM APIs. Currentbrowser3/querytab1 holds a successful0p1ssrd+t2mJinan24h15minquery; markedDeliverable. Temporarymonitor tab closed. ui-0p1-query.png,ui-0p1-monitor.png andDOMstate txt saved. Actualgraph visible;monitor85complete/done0failures.
- 65focused tests passed(download25,restore25,reader15),XMLsaved; no unrelated full suite. Source split tests previously passed unchanged in speed assessment.
- evidence/ has42smallreceipts/logs/exports(818746bytes), transferredSHA verified. Largeraw/Zarr remain241. Final README contains measured timing, exact field scope, original-index alternatives and all acceptance attempts. Local source/artifact hashes are sealed in SHA256SUMS.json; no further required implementation or acceptance work remains. Relevantmemory citation required final:MEMORY.md49-52,585-586 plusrollout_summaries/2026-10-15T10-00-15-edP9-ecmwf_ens_probability_download_deployment_and_integrity_audi.md16-17;rolloutUUID01a13f01-5a55-7da3-8550-a2cbd5573847.
