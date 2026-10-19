# ENS 事件概率下载与调度

接入源为 `ifs-ens-probabilities` 和 `aifs-ens-probabilities`，日常均取 00/12 UTC 起报，
0.25° 全球原始概率，经 restorage 裁剪为中国区域。历史 AIFS 包含 06/18 UTC；
更早的 IFS 还有原始 0.4° 网格，按原网格保存并提示。概率原值单位为百分比。

IFS 首批下载 `tpg20/tpg50/tpg100/10fgg15/10fgg25`。每个起报下载 240h 与 360h
两个 **多时间包**，合计 145 个样本（5 字段 × 29 个 24h 窗口，每 12h 滚动）。
AIFS 下载 14 个公开事件概率参数，按统计类型及时长展开为 25 个输出字段、313 个样本。
两个包不是只有两个预报时刻。初次下载时缺包或索引缺少字段/窗口会记入失败，不能视为完整产品。

阈值比较与原始 GRIB probabilityType 一致，保留原始类型和阈值编码。
ECMWF 参数标题中的 “at least” 与编码表述不完全一致；当前数据使用类型 3
（above lower limit，输出 `gt`）或类型 0（below lower limit，输出 `lt`）。
来源：[ECMWF 概率类型表](https://codes.ecmwf.int/grib/format/grib2/ctables/4/9/)、
[公开数据目录](https://www.ecmwf.int/en/forecasts/datasets/open-data)。

## 手动下载

在 `download/` 内运行：

```sh
uv run climate_download run --config config/jobs/ifs_ens_probabilities.yaml --date 20261015 --cycle 12 --output-dir ../climate_data --no-progress
uv run climate_download run --config config/jobs/aifs_ens_probabilities.yaml --date 20261015 --cycle 12 --output-dir ../climate_data --no-progress
```

沿用每个 `(date, cycle)` 一个 manifest 的契约。`steps: all` 在此源固定为
两个包 240/360，不使用普通 NWP 的逐时步长探测。原始文件正常时重跑不重复下载
GRIB；仍读取索引检查上游字段，完整且内容不变的 manifest 保持原 mtime。

## 每日运行

宿主机使用 `deploy/probabilities_run.py`（Python 标准库），下载与重整均进入
现有 `zhangmy-dev`，两个源依次运行。下载 workers=4、init_concurrency=1；
重整 workers=1，并限制数值库线程。无新增常驻生产容器。

**时区必须为 Asia/Shanghai**。本次已核对服务器 `/etc/localtime`；旧 crontab
头部“UTC”说明已过时，不能按其注释换算时间。

```cron
15 5 * * * /usr/bin/python3 /home/zhangmingyu/operation/probabilities_run.py --cycle 12 >> /home/zhangmingyu/operation/logs/ens-probabilities-12z.log 2>&1
15 17 * * * /usr/bin/python3 /home/zhangmingyu/operation/probabilities_run.py --cycle 0 >> /home/zhangmingyu/operation/logs/ens-probabilities-00z.log 2>&1
```

05:15 下载前一 UTC 日的 12z；17:15 下载当日 00z。默认再检查前一天同 cycle，
每次最多尝试 3 轮、间隔 15 分钟，只重试失败目标。缺包不作为成功，下载成功后才
定向 `scan-once --source ...`；各源失败相互隔离，最终仍有失败时退出码为 1。
全局文件锁防止重叠运行。ECMWF 官网入口仅有短期滚动窗口；当前 Google 镜像
有更长历史，但部分旧索引与 GRIB 不一致，见下文。补查机制不代表完整历史回补。

可用环境变量：`CONTAINER`、`DOWNLOAD_DIR`、`RESTORE_DIR`、`DOWNLOAD_ROOT`、
`ZARR_ROOT`、`PROBABILITY_LOCK`。默认项目路径分别为 `/workspace/climate_down`、
`/workspace/climate_restorage`，数据为 `/climate_data` 及
`/climate_data/climate_data_storage/zarr`。

```sh
python3 deploy/probabilities_run.py --cycle 12 --dry-run
# 明确指定一个起报，单次运行（运维补拉）
python3 deploy/probabilities_run.py --date 20261015 --cycle 12 --attempts 1
```

## 验收与回滚

实际验收脚本为 `scripts/validate_ens_probabilities.py`：在 restorage 环境跑 `--phase raw`，
在 watcher 环境跑 `--phase query` 与 `--phase api`，统一传入 `--raw-root`、
`--zarr-root`、`--out`，可指定 `--date`、`--cycle`、`--base-url`。
报告、校验和和浏览器记录见 `docs/validation/ens-probabilities-20261016/`。

服务更新以当前线上镜像为基础，只叠加本次 Python 文件及前端产物，保留原有修复。
原镜像标签 `climate-watcher:pre-ens-20261016`；代码、容器配置与旧 crontab 备份在
`/home/zhangmingyu/operation/backups/ens-probabilities-20261016`。
回滚服务时将原镜像重新标为 `climate-watcher:latest`，在 watcher 项目内执行
`docker compose up -d --no-build watcher`；调度可只移除新加的 ENS 段。
代码回滚只恢复备份清单中的文件并移除本次新增文件，不覆盖其他工作。

## 30 天原始数据冷存

两个概率源已纳入宿主机现有 `forecast-coldmove.sh`，每天北京时间 07:00 执行。
UTC 日期超过 30 天的原始 GRIB 和 manifest 转至
`/save_data/climate_data/<source>/<date>/`；Zarr 继续保留在热盘
`/training_data/climate_data/climate_data_storage/zarr/<source>/`。

迁移前逐起报核验 manifest、原始 SHA-256、完整字段/原生时刻/窗口、所有概率
分块存在且可读；复制后再次核验，并逐文件比较冷、热目录 SHA-256，一致才删热端。
与概率下载共用 `ens_probabilities.lock`，避免处理与冷存重叠。具体安装、路径和
回滚见 [restorage 冷存说明](../../climate_restorage/deploy/README.md)。

## 发布时间与历史可获取范围（2026-10-16 核查）

**历史起报全量清点已完成**：截至 2026-10-15 的 00/12 UTC，IFS 文件齐全
1,921 批，其中 1,919 批通过当前字段/窗口及头部核查；AIFS 文件齐全 943 批，
其中 229 批通过原样索引检查，另 83 批可按累计长度重建旧索引偏移后通过恢复性检查。
AIFS 当前完整字段的候选可前推至 2026-05-12 12 UTC，但需排除 2026-05-13 00 UTC；
生产下载器尚未增加旧索引兼容逻辑。IFS 需排除 2025-10-01 12 UTC、2025-10-02 00 UTC。
完整清单、额外时次及验收边界见
[历史起报报告](../docs/validation/probability-history-20261016/RESULTS.md)。
下文保留首次抽查和发布时间结果，历史数量以上述全量清点为准。

官方 [派发时刻表](https://confluence.ecmwf.int/spaces/DAC/pages/272310483/Dissemination+schedule)
列出 `enfo/ep` 的 240/360 两包：00 UTC 起报约 07:41/08:01 UTC，12 UTC 起报
约 19:41/20:01 UTC。换算北京时间分别为 15:41/16:01、次日 03:41/04:01。
这是派发参考时刻，并非 Google 镜像的固定到达承诺；AIFS 公开数据可在生成后提前释放。

抽查当前使用的 Google 公共桶 2026-10-13～15 三天，取两包 GRIB 和索引的
最后对象创建时间，得到以下北京时间到齐区间（分钟显示）：

| 源 | 00 UTC 起报 | 12 UTC 起报 |
|---|---|---|
| IFS ENS 概率 | 当日 16:48～16:51 | 次日 04:44～04:45 |
| AIFS ENS 概率 | 当日 14:40～15:00 | 次日 02:51～03:09 |

维持 05:15 / 17:15 调度，较最近三天最晚 IFS 到齐时刻约有 24～29 分钟余量。
以上为抽样对象元数据的观测结果，不保证每天同一时刻可用；以两包及其索引均可读、
字段/窗口检查通过为最终依据，现有失败重试继续生效。

[ECMWF 官网说明](https://www.ecmwf.int/en/forecasts/datasets/open-data)中的最近
12 次起报、约 2～3 天滚动窗口，不能直接等同于 Google 桶的实际历史保有量。
本次只读探测发现 Google 桶有更早对象，并检查了选定字段/窗口及 GRIB 消息：

- IFS：`2024-02-28 12 UTC` 的两包存在，当前 5 字段的 145 个窗口记录匹配，
  两包中的 GRIB 消息抽取有效；当天 00 UTC 对应的当前 0.25° 路径为 404。
- AIFS：`2025-07-01 12 UTC` 已有概率对象，但早期并不具备当前全部事件字段。
  `2026-05-12 12 UTC` 索引已包含当前 14 参数/313 记录，但该样本及部分更早/6 月
  样本的索引字节位置与 GRIB 不一致，不能直接视为当前下载方式可用。
- AIFS 的 `2026-06-23 12 UTC`、`2026-06-24 00/12 UTC` 及 7～10 月多个抽样
  起报通过 GRIB 消息抽检；`2026-06-23 00 UTC` 的两包消息抽检失败。这个边界
  仅是已验证样本，未普查全部日期，不能据此承诺期间无缺包或其他旧索引问题。

历史探测没有批量回填，也没有把早期不完整字段降级当成完整产品。是否可以
逐期处理仍应沿用 manifest、概率适配器和真实读取验收，不能仅凭目录/索引存在判断。
探测输出保存在 `docs/validation/ens-probabilities-20261016/upstream-availability-*.jsonl`。
## 历史全量回填（2026-10-16）

上节为初期抽检记录。全量回填已重建旧索引偏移并补查旧目录，历史范围以下述完整清单为准。

`probability_history_run.py` 是宿主机执行器；从保存的 GCS 对象清单构建不可变队列，
调用容器内 `scripts/backfill_probability_init.py`，随后调用 restorage 的
`scripts/restore_probability_history.py`。不改变每日 05:15/17:15 的日常规则。

本次清单截止 UTC 2026-10-15：IFS 从 2023-07-12 12 UTC 起，2383 个起报；
其中406次使用旧目录原始0.4°网格，56次使用旧目录0.25°，1921次使用新目录0.25°。
最早0.25°起报为2024-01-31 12 UTC；同一起报有重复版本时优先较细网格，不重复入库。
AIFS 从 2025-07-01 06 UTC 起，历史包含 00/06/12/18 UTC，共 1887 个时次检查，
其中 2026-05-12 06 UTC 整次没有历史包（有包 1886 次）。只回填当前接入的事件概率字段。

- 旧 AIFS 索引包含 em/ep/es 分块偏移重置。先用完整索引的消息长度重建全局偏移，
  要求长度总和等于远端 GRIB 文件大小；再筛 ep 和目标事件。逐条验证下载消息的长度及首尾。
- 保存 `f240/f360.upstream.index` 和索引哈希、下载选择签名及文件 SHA-256；恢复时须匹配签名和哈希。
- 历史 manifest 的 `probability_availability` 分别列出当前目录、实际可用和缺失的字段/事件结束时效，
  每个包保存缺包/无概率字段/实际事件记录。404 与传输错误分开，网络失败不能冒充上游缺失。
- 只按已声明的实际事件处理；实际有效字段/窗口必须完全一致，并验证百分比范围、原始窗口边界。
  暂存目录全量读回后发布 Zarr。上游无所选概率数据时只留不可用回执，不生成空 Zarr。
- `probability_complete=1` 表示已完整处理声明的可用数据；`probability_catalog_complete=0`
  表示该历史起报不具备当前完整目录。日常未声明历史模式的产品仍须具备全部字段和窗口。
- 查询保留原始时间分辨率；缺变量沿用默认报错、`allow_missing=true` 才返回 NaN 的契约。
  页面主动允许缺变量并显示告警；缺事件窗口单独列明，绝不补零或时间插值。
- 状态落在热盘 `_probability_backfill/{progress.json,inits/,logs/}`。逐起报处理，共用日常概率锁；
  北京时间 05:00–06:30、17:00–18:30 优先让日常任务运行，热盘剩余不足 100 GiB 暂停。
  容器内单阶段360秒、终止宽限15秒，两阶段750秒以内，不让异常网络重试跨过15分钟优先窗口。
  宿主机十分钟保活检查只恢复未完成任务；运行中互斥，完成后快速退出。
  重启或进入下一轮时，优先重试已失败起报，再沿原队列补后续历史，避免失败缺口等到整批尾部。
  每个起报的尝试次数跨进程重启累计；已完成及上游明确不可用的回执保留。
- 冷存仍保留 30 天；历史部分可用产品须与上游回执一致、Zarr 全量可读、原始及冷副本哈希一致才迁移。
  整天全部排队时次均已下载/处理才允许迁移，避免回填途中拆开热冷目录。Zarr 和回填状态继续留热盘。
