# All-Source Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在服务器启动后补检全部生产数据源，并以持久定时器持续发现和修复近期缺失的 NWP 初始化，同时保留卫星、ERA5 和切片链路的幂等恢复入口。

**Architecture:** 新增一个标准库 Python 调度器，根据各源的 cycle、发布延迟、保留窗口和期望步数检查 manifest 与原始 GRIB 边界，只有缺失或损坏时才调用现有 `download_run.sh`。启动脚本按 DWD ICON、Himawari、其余 NWP、切片、ERA5 的顺序调用已有幂等入口；systemd timer 提供 `Persistent=true` 与半小时巡检，cron 仅作为无 root 权限时的回退。

**Tech Stack:** Python 3.11 标准库、POSIX shell、systemd、pytest、现有 `climate_download` CLI 与宿主机 Docker 调度脚本。

**Spec:** `deploy/README.md`

## Global Constraints

- 不并发启动同一数据源；复用现有每源 `flock`，并给全源补检增加独立锁。
- DWD ICON 必须优先处理，因为上游按 cycle 仅保留约 24 小时。
- 自动完整性检查必须满足 `completed_at`、零 failures、期望 step 集及文件大小；下载落盘强制检查 `GRIB...7777`，手工深检用 `--verify-boundaries`，避免定时器被内核 I/O 等待永久钉死。
- 完整 init 不访问上游、不改 manifest mtime，也不触发重复切片。
- 不修改用户已有的 `config/jobs/gfs_backfill_gcs.yaml` 改动，不提交或推送 Git。

---

### Task 1: Recent-init reconciliation

**Files:**
- Create: `deploy/reconcile_sources.py`
- Test: `tests/test_reconcile_sources.py`

**Interfaces:**
- Consumes: host data root, `download_run.sh`, current UTC time, per-source production policy.
- Produces: `manifest_health(path, data_root, expected_steps) -> tuple[bool, str]`, deterministic candidate inits, and exit code 0 only when every actionable init is complete.

- [x] **Step 1: Write failing tests** for date selection, healthy/corrupt manifests, policy coverage/order, dry-run behavior, and runner environment.
- [x] **Step 2: Run** `uv run pytest tests/test_reconcile_sources.py -q` and confirm failure because the module is absent.
- [x] **Step 3: Implement** the standard-library reconciler with `fcntl.flock`, `--dry-run`, `--only-source`, `--exclude-source`, `--now`, and configurable paths.
- [x] **Step 4: Run** `uv run pytest tests/test_reconcile_sources.py -q` and confirm all focused tests pass.

### Task 2: Boot recovery and persistent scheduling

**Files:**
- Create: `deploy/recover_all_on_boot.sh`
- Create: `deploy/systemd/climate-source-reconcile.service`
- Create: `deploy/systemd/climate-source-reconcile.timer`
- Create: `deploy/systemd/climate-boot-recovery.service`

**Interfaces:**
- Consumes: `reconcile_sources.py` plus existing host hooks `hima-realtime.sh`, `hima-rebuild-recent.sh`, `restore_scan.sh`, and `era5-production.sh`.
- Produces: one boot recovery unit and one persistent 30-minute NWP reconciliation timer.

- [x] **Step 1: Add boot script** that waits for `zhangmy-dev`, prioritizes DWD, invokes the remaining idempotent hooks, and holds a host lock.
- [x] **Step 2: Add systemd units** with `After=docker.service network-online.target`, `User=zhangmingyu`, `OnBootSec=5min`, `OnCalendar=*:17,47`, and `Persistent=true`.
- [x] **Step 3: Validate** scripts with `bash -n` and units with `systemd-analyze verify` when available.

### Task 3: Operational documentation

**Files:**
- Modify: `deploy/README.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: deployment files from Tasks 1-2.
- Produces: installation, rollback, verification, cron fallback, source coverage, and failure-semantics documentation.

- [x] **Step 1: Document** the four-layer recovery contract: HTTP retry, per-step resume, recent-init reconciliation, boot/persistent scheduling.
- [x] **Step 2: Document** systemd installation and rollback commands plus cron fallback for hosts without root access.
- [x] **Step 3: Add** a main README pointer to the production recovery section.

### Task 4: Production rollout and verification

**Files:**
- Deploy under: `/home/zhangmingyu/operation/`
- Install when root is available: `/etc/systemd/system/climate-*.{service,timer}`

**Interfaces:**
- Consumes: validated repository artifacts.
- Produces: live boot recovery, periodic missing-manifest repair, and auditable logs.

- [x] **Step 1: Back up** the live crontab and affected operation files with timestamped names.
- [x] **Step 2: Deploy** scripts without changing container CPU/memory settings or existing per-source schedules.
- [x] **Step 3: Enable** systemd units when sudo/root is available; otherwise install documented `@reboot` and 30-minute cron fallback.
- [x] **Step 4: Run dry-run and one live reconciliation**, proving complete sources skip and the known irrecoverable DWD 20260909 12z is not retried forever.
- [x] **Step 5: Verify** service/timer or cron state, logs, locks, manifest/Zarr status, and repository diff.
