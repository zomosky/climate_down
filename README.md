# climate_download

`climate_pipeline` 下的气象数据下载子项目，负责把外部数值预报产品（首批 ECMWF AIFS）从公开镜像拉到本地，**在下载阶段**就完成变量与字节级裁剪，并向下游 `cliamte_data` 加工管道交付干净、体量合适的 GRIB 文件 + manifest 触发凭据。

完整的项目目标、范围、路线图见 [`PROJECT.md`](./PROJECT.md)。本文聚焦 **配置（YAML）** 与 **函数（Python API）** 的使用方式。

---

## 1. 安装

要求 Python 3.11，依赖管理使用 [uv](https://docs.astral.sh/uv/)。

```bash
# 仅安装下载链路所需的运行依赖
uv sync

# 同时安装绘图额外组（cfgrib + eccodes 二进制 + xarray + matplotlib + numpy）
uv sync --extra viz

# 安装 S2S 次季节预报子链路所需依赖（cdsapi）
uv sync --extra s2s

# 跑测试
uv run pytest
```

`viz` 与 `s2s` 都是可选 extra：常驻 byte-range 下载调度只需 `uv sync` 即可；要本地解码 GRIB / 出图安装 `viz`；要拉 ECMWF Data Store 上的 S2S 次季节集合预报安装 `s2s`（详见 §10）。

---

## 2. 快速开始

```bash
# 列出已注册的源（注册表自检）
uv run climate-download list-sources

# 用业务示例配置下载昨天 00z 的 0h 预报
uv run climate-download run --config config/jobs/aifs_wind_pv.yaml

# 一次拉一段未来 120h 预报（每 6h 一帧）
uv run climate-download run --config config/jobs/aifs_wind_pv.yaml \
    --date 20260507 --cycle 0 --steps 0-120:6

# 历史回填：一次跑 7 天 × 2 个 cycle 的 0h 分析场（默认 init 并发 2）
uv run climate-download run --config config/jobs/aifs_wind_pv.yaml \
    --date 20260501-20260507 --cycle 0,12

# 改用 NOAA GFS 0.25°（同一 CLI，换 source/job 配置即可）
uv run climate-download run --config config/jobs/gfs_wind_pv.yaml \
    --date 20260507 --cycle 12 --steps 0,6

# 用 cfgrib 读取产物，画 100 m 风速
uv run python examples/plot_wind_speed.py \
    --grib examples_output/aifs-single/20260507/00z/f000.subset.grib2 \
    --bbox 70,140,15,55 --out examples_output/wind100_china.png

# 画过去 6h 的平均短波辐射（光伏侧验证）
uv run python examples/plot_pv_radiation.py \
    --grib examples_output/aifs-single/20260507/00z/f006.subset.grib2
```

默认输出布局为 `<output_dir>/<源>/<日期>/<起报时>z/f<step>.subset.grib2`，例如 `output/aifs-single/20260507/00z/f006.subset.grib2`。每个 `(date, cycle)` 会在同一子目录下额外产出一份 `{date}_{cycle:02d}z_{source}.manifest.json`；下游 sensor 监听这些文件即可触发后续加工。范围里若有源还没发布的 step，会被 HEAD 探测识别并跳过（warning 日志），作业不会失败。

---

## 3. 目录结构

```
download/
├── PROJECT.md                  # 项目目标 / 路线图
├── README.md                   # 本文档
├── pyproject.toml              # uv 项目配置
├── config/
│   ├── sources/
│   │   ├── aifs.yaml           # type: aifs — ECMWF AIFS 0.25°（GCS, JSONL .index）
│   │   ├── gfs.yaml            # type: gfs  — NOAA GFS 0.25° atmos（S3, wgrib2 .idx）
│   │   ├── hrrr.yaml           # type: hrrr — NOAA HRRR 3km CONUS（S3, wgrib2 .idx）
│   │   └── s2s_{ecmwf,cma,iap_cas,ncep,ukmo}.yaml  # type: s2s — ECDS 次季节集合预报
│   ├── jobs/
│   │   ├── aifs_wind_pv.yaml   # 业务作业（变量组 + 时间 + 下载参数）
│   │   ├── gfs_wind_pv.yaml    # GFS 等价业务作业（GFS shortName）
│   │   └── s2s_renewables_{ecmwf,cma,iap_cas,ncep,ukmo}.yaml  # 5 家 S2S 新能源 job
│   ├── s2s_catalogue.yaml      # 5 家 S2S 中心 × 变量索引（人读，附 CN 解释）
│   └── s2s/_capabilities.json  # ECDS constraints 快照（机读，校验 job 用）
├── src/climate_download/
│   ├── cli.py                 # `climate-download` 入口：run / list-sources / s2s
│   ├── config.py               # YAML schema + loader（pydantic v2）
│   ├── jobs.py                 # run_job：probe → 取 sidecar → 过滤 → 下载 → manifest
│   ├── manifest.py             # 写 (date, cycle) 级 manifest.json
│   ├── logging_setup.py        # structlog JSON + 第三方噪声静默
│   ├── sources/                # 每个气象源 = 一个文件 + @register
│   │   ├── base.py             # Source Protocol + BaseSource 默认实现
│   │   ├── registry.py         # SOURCE_REGISTRY + @register / get_source
│   │   ├── aifs.py             # AifsSource（jsonl idx, suffix-swap URL）
│   │   ├── gfs.py              # GfsSource （wgrib2 idx, split URL, step:03d）
│   │   └── hrrr.py             # HrrrSource（wgrib2 idx, split URL, step:02d）
│   ├── s2s/                    # S2S 子链路（不走 byte-range,走 cdsapi 提交-轮询-下载）
│   │   ├── client.py           # cdsapi 包装 + ~/.ecdsapirc 加载
│   │   ├── source.py           # S2SSource（13 个 origin 校验）
│   │   ├── config.py           # S2SJobConfig / S2SVariableGroup / S2STimeConfig
│   │   ├── jobs.py             # submit-poll-download 编排 + run report
│   │   └── manifest.py         # 多消息 GRIB 的 manifest 写入
│   └── grib/
│       ├── index.py            # .index/.idx 解析（jsonl + wgrib2）、过滤、合并
│       └── partial.py          # PartialDownloader：byte-range 并发下载
├── scripts/
│   └── build_s2s_catalogue.py  # 重抓 ECDS constraints,刷新 _capabilities.json
├── examples/
│   ├── aifs_partial_download.py   # CLI 向后兼容 shim（→ climate-download run）
│   ├── plot_wind_speed.py         # 100 m 风速可视化
│   └── plot_pv_radiation.py       # 短波辐射可视化（光伏侧）
└── tests/                      # pytest 用例（解析 / 过滤 / 合并 / 下载）
```

---

## 4. 配置详解（YAML）

配置分两层：**source 模板** 与 **job 作业**。job 通过名字引用 source，多个 job 可以复用同一个 source。

### 4.1 source 模板：`config/sources/<name>.yaml`

每个数据源 = **一个 Python 适配器文件**（`src/climate_download/sources/*.py`）+ **一个 YAML 配置**。
YAML 用顶部的 `type:` 字段决定加载哪个适配器，不同源**不再共用**字段集合 —— 想加 NetCDF / BUFR / 全文件下载源时，新写一个适配器即可，不必往公共 schema 塞字段。

````yaml path=config/sources/aifs.yaml mode=EXCERPT
type: aifs                        # ← discriminator: 决定走哪个适配器
name: aifs-single                 # 文件名片段 / 日志维度
url_template: >-
  https://.../{date}{cycle:02d}0000-{step}h-oper-fc.{suffix}
````

公共字段（每个适配器都有）：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `type` | str | ✓ | 适配器名，已注册的有 `aifs` / `ifs` / `gfs` / `graphcast` / `hrrr`（运行时 `list_sources()` 查看）|
| `name` | str | ✓ | 数据源唯一名，作为文件名片段与日志维度 |
| `description` | str | ✗ | 人读说明 |
| `supports_byte_range` | bool | ✗（默认 `true`）| 元数据，未来供"不支持 range 时 fallback 全量"逻辑使用 |

#### 内置适配器：`type: aifs` / `type: ifs`

ECMWF open-data（AIFS-Single 与 IFS-HRES）。同一模板含 `{suffix}` 占位符，`index` ↔ `grib2` 切换。
sidecar 解析器：JSONL（ECMWF `.index`）。两个 `type:` 别名共用 `AifsSource` 实现 —— 协议（GCS 桶 + JSONL sidecar + suffix-swap）相同，区别只在 URL 路径中的 `aifs-single/` vs `ifs/` 与流名（`oper`/`scda`），都通过 `url_template` 表达。

内置 YAML：`config/sources/aifs.yaml`（AIFS-Single）、`config/sources/ifs.yaml`（IFS-HRES oper，00z/12z）。06z/18z 走 `scda` 流，按相同结构复制一份并替换 `oper` 即可。

| 字段 | 必填 | 说明 |
|---|---|---|
| `url_template` | ✓ | 含 `{date}` `{cycle:02d}` `{step}` `{suffix}` |

#### 内置适配器：`type: gfs` / `type: graphcast`

NOAA wgrib2-idx S3 协议。sidecar 与 GRIB URL **不共用后缀**，所以拆成两个模板。
sidecar 解析器：wgrib2（解析时会 HEAD 数据 URL 拿 `Content-Length`，用 `total - last_offset` 推断末条 length）。两个 `type:` 别名共用 `GfsSource` 实现 —— `gfs` 指向 `noaa-gfs-bdp-pds`；`graphcast` 指向 `noaa-nws-graphcastgfs-pds`（NOAA NWS GraphCast，桶里命名为 `aigfs.*`），其压力层与地面层分两个 GRIB 文件发布，所以提供 `graphcast_pres.yaml` 与 `graphcast_sfc.yaml` 两份 source 配置。

````yaml path=config/sources/gfs.yaml mode=EXCERPT
type: gfs
name: gfs-0p25
index_url_template: >-
  https://.../gfs.t{cycle:02d}z.pgrb2.0p25.f{step:03d}.idx
data_url_template: >-
  https://.../gfs.t{cycle:02d}z.pgrb2.0p25.f{step:03d}
````

| 字段 | 必填 | 说明 |
|---|---|---|
| `index_url_template` | ✓ | sidecar URL，占位符 `{date}` `{cycle:02d}` `{step}`（可用 `{step:03d}` 零填充）|
| `data_url_template` | ✓ | GRIB 数据 URL，占位符同上 |

#### 内置适配器：`type: hrrr`

NOAA HRRR 3 km CONUS（`noaa-hrrr-bdp-pds` S3 桶）。字段与 GFS 完全一致 —— 单独一个适配器是因为 URL 模板差异大（`hrrr.YYYYMMDD/conus/hrrr.tHHz.wrfsfcfFF.grib2`，二位预报小时，多个 product 切片），分开维护后续要换 product 不影响 GFS。

#### 新增一个源（开发者视角）

1. 在 `src/climate_download/sources/` 新建 `myname.py`：

   ````python path=src/climate_download/sources/myname.py mode=EXCERPT
   from pydantic import BaseModel, ConfigDict
   from climate_download.grib.index import IndexRecord, parse_index_text
   from climate_download.sources import BaseSource, register

   @register("myname")
   class MyNameSource(BaseSource, BaseModel):
       model_config = ConfigDict(extra="forbid")
       name: str
       description: str | None = None
       my_url: str
       supports_byte_range: bool = True

       def build_index_url(self, *, date, cycle, step):
           return self.my_url.format(date=date, cycle=cycle, step=step) + ".idx"

       def build_data_url(self, *, date, cycle, step):
           return self.my_url.format(date=date, cycle=cycle, step=step)

       def fetch_records(self, client, *, date, cycle, step):
           resp = client.get(self.build_index_url(date=date, cycle=cycle, step=step))
           resp.raise_for_status()
           return parse_index_text(resp.text)
   ````

2. 在 `src/climate_download/sources/__init__.py` 末尾加 `from climate_download.sources import myname as _myname  # noqa: F401`（触发 `@register`）。
3. 写一份 `config/sources/myname.yaml`，顶部加 `type: myname`，剩余字段就是适配器的 pydantic 字段。
4. job YAML 里 `source: myname` 即可使用，**不需要改 `config.py` 也不需要改 `jobs.py`**。

**别名复用**：当一个新数据源与已有源**协议完全相同**（GCS + JSONL，或 S3 + wgrib2 idx），只是 URL 路径不同，**不需要新写适配器类**。在已有类上叠加一个 `@register("new_alias")` 装饰器，再写一份 `config/sources/new_alias.yaml` 即可。内置的 `ifs` ↔ `aifs`、`graphcast` ↔ `gfs` 就是这样实现的。

**listing / 描述字段约定**（让 `list-steps` / `list-variables` 开箱即用）：

- 若 sidecar 源在 S3 / GCS XML 兼容桶上，`list_available_steps` 不用手写 —— 在子类里 `return list_remote_steps(client, index_url_template=self.index_url_template, date=date, cycle=cycle)` 即可，`_listing.py` 会按 host 自动选 S3v2 / GCS XML 方言。
- `list_available_variables` 默认实现走 `fetch_records()` 投影，所以**只要 `fetch_records()` 正确填了 `IndexRecord`，CLI 就能自动跑**。
- 若源的 sidecar 里带"人类可读层描述"（wgrib2 idx 的第 5 列、未来 IFS-HRES 等同等字段），构建 `IndexRecord` 时一并写入 `level_desc=`，`list-variables` 文本表会显示这一列，`--yaml` 脚手架也会把它作为注释列写进去，方便配置者核对。ECMWF JSONL `.index` 不带这种字段就保留 `None`（pydantic 默认值）。
- **所有 metadata HTTP 调用必须走 `sources._http.request_with_retry`**：probe HEAD / sidecar GET / listing GET 都不要直接调 `client.get/head`。该 helper 对 `httpx.TransportError`（含 SSL EOF / ConnectError / ReadError / RemoteProtocolError）、`TimeoutException` 与 408/425/429/5xx 状态指数退避重试（默认 `max_attempts=4`），与 `PartialDownloader` 的 byte-range 重试参数一致 —— 这样一次瞬时网络抖动不会让整个 init 失败。404 等语义状态会原样返回，由调用方自己判定。byte-range 下载本身已经走 `PartialDownloader` 的 tenacity 循环，无需额外包装。

需要更彻底的覆盖（NetCDF / OPeNDAP 这类不走 byte-range 的源）？把 `download_step` 也覆盖成自己的下载逻辑即可，详见 §5.4。


### 4.2 job 作业：`config/jobs/<name>.yaml`

一个完整的下载作业 = source + 变量组 + 时间 + 下载参数。

````yaml path=config/jobs/aifs_wind_pv.yaml mode=EXCERPT
source: aifs              # 引用 config/sources/aifs.yaml；也可整段内联
time:
  date: yesterday         # YYYYMMDD / today / yesterday（UTC）
  cycle: 0                # 0 / 6 / 12 / 18
  steps: [0]              # 预报时效列表，单位小时
variables:
  - name: surface_wind_temp_pressure_cloud_radiation
    levtype: sfc
    params: [10u, 10v, 100u, 100v, 2t, 2d, msl, sp, tcc, lcc, mcc, hcc, ssrd, strd, tp]
  - name: pressure_level_wind_thermo
    levtype: pl
    levels: ["850", "925", "1000"]
    params: [u, v, t, q, z]
download:
  # Layout: examples_output/<source>/<date>/<cycle>z/f<step>.subset.grib2 (defaults).
  output_dir: examples_output
  workers: 6
  gap_tolerance: 0
  timeout_seconds: 120
  max_attempts: 4
````

#### `source`

字符串（推荐）：`source: aifs` → 自动加载 `config/sources/aifs.yaml`。
也可整段内联，写法与 source 模板相同。

#### `time`

`date / cycle / steps` 三个字段都接受 **单值 / 列表 / 范围** 三种写法，作业按 `(date) × (cycle)` 笛卡尔积展开成多个 *init*，每个 init 内部再下载列表里的所有 step。

| 字段 | 写法 | 示例 | 默认 |
|---|---|---|---|
| `date` | 单值 | `date: 20260507` 或 `date: yesterday` | `yesterday` |
| | 列表 | `date: [20260501, 20260502]` | |
| | 范围（mapping） | `date: { start: 20260501, end: 20260507 }` | |
| | 范围（短语法） | `date: "20260501-20260507"` | |
| `cycle` | 单值 | `cycle: 0` | `0` |
| | 列表 | `cycle: [0, 12]` 或 `cycle: "0,12"` | |
| `steps` | 单值 | `steps: 6` | `[0]` |
| | 列表 | `steps: [0, 6, 12]` | |
| | 范围（mapping） | `steps: { start: 0, end: 120, step: 6 }` | |
| | 范围（短语法） | `steps: "0-120"`（默认 6h 步长）/ `"0-120:3"` / `"0/120/6"`（MARS 风格） | |
| | 全量自动发现 | `steps: "all"` —— 由源 `list_available_steps` 枚举（GFS/HRRR 走 S3 LIST，AIFS 走 GCS XML LIST）| |

约束与行为：

- `cycle` 必须取自 `{0, 6, 12, 18}`（UTC），其它值 schema 校验立即报错。
- `today / yesterday` 在**展开时**才解析（按 UTC 当前时间），所以同一份配置在不同日期跑会自动滚动。
- `steps` 范围里那些**源还没发布的 step**（HEAD `.index` → 404）会被自动跳过并打 warning，不会让作业失败。这一行为对回填很关键 —— 写一个宽泛的 `0-360` 也能容错。
- `steps: "all"` 时不再做 HEAD 探测，而是直接拉 bucket LIST（AIFS ~61 step / GFS ~209 step），结果按整数升序去重返回。`_listing.py` 自动识别 S3 与 GCS 两种方言（命名空间、分页参数、bucket 位置）；新源若想接入只要 host 命中已知模式即可，否则 `list_available_steps` 返回 `None`，run_job 会日志 warning 并跳过该 init —— 不会阻塞同批其它 init。
- 历史日期可直接写 `date: 20260101`；可用范围取决于数据源 bucket 的保留策略（AIFS open-data 在 GCS 上滚动保留约几个月）。真正的多年回填请考虑 ERA5 / MARS（接入新 source 即可）。

#### `variables`

变量组列表，每组对应一次 `IndexFilter`，结果按出现顺序合并、去重。

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `name` | str | ✓ | 组内唯一名，用于 manifest 的 `selected_breakdown` 与日志 |
| `levtype` | str | ✓ | GRIB 层类型：`sfc`（地表）/ `pl`（等压面）/ `sol`（土壤层）等 |
| `params` | list[str] | ✓ | ECMWF shortName 列表，如 `100u, 2t, ssrd, u, v` |
| `levels` | list[str/int] | ✗ | 仅对 `pl/sol` 有意义，整数会自动转字符串 |

> **注意**：`levels` 是 AND 过滤器。如果一个组同时混入 `sfc` 与 `pl` 的参数并写了 `levels`，sfc 字段会因没有 levelist 被丢弃。**正确做法是把 sfc 与 pl 拆成两组**，示例配置已遵循该约定。

> **AIFS 没有 950 hPa**，最近的层是 925 hPa。示例用 925 作为 100 m 轮毂高度的近似替代。

#### `download`

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `output_dir` | path | `output` | 输出根目录（GRIB + manifest + `_runs/` 都落在这里）|
| `subdir_template` | str | `"{source}/{date}/{cycle:02d}z"` | Python `format`，可用占位符 `{source} {date} {cycle:02d} {step}`；默认按 **源/日期/起报时** 分层，方便并行多源/多 init 不互相覆盖。置 `""` 可回到平铺布局 |
| `filename_template` | str | `"f{step:03d}.subset.grib2"` | Python `format`，可用占位符 `{source} {date} {cycle:02d} {step}`。默认文件名只含 step（源、日期、cycle 已在路径里），便于按目录批量处理 |
| `workers` | int | `4` | 单个 step 内的 byte-range 并发数 |
| `gap_tolerance` | int | `0` | 合并相邻 range 时容忍的字节空隙；调大可减少 HTTP 次数，代价是浪费带宽 |
| `timeout_seconds` | float | `120` | 每个 HTTP 请求的超时 |
| `max_attempts` | int | `4` | 单个 range 的总尝试次数（含首次）|
| `init_concurrency` | int ≥1 | `2` | 跨 `(date, cycle)` init 的并发数；多日/多 cycle 回填时同时跑 N 个 init，每个 init 内部 step 仍串行 |
| `progress_bar` | bool | `false` | 单 step 内是否显示 tqdm byte-range 进度条（CLI `--progress` 等价）|

并发结构：`init_concurrency × workers` = 全局最大并发 byte-range 请求数。默认 `2 × 4 = 8`，回填大批量数据时可调大；但请注意 GCS 端的速率限制以及本地带宽。

#### 容错与断点续传

- **`run_job` 返回 `JobOutcome(succeeded, failed, report_path)`**：单个 step / init 出错只会写入 `failed: list[JobFailure]`（带 `phase = list / probe / download / validate / manifest`），不会让整批回填崩。
- **每次执行写 `<output_dir>/_runs/run_<ts>.json`**：含成功/失败计数 + 失败详情，可作为重试入口数据。
- **统一 HTTP 重试底线**：所有 metadata 请求（probe HEAD / sidecar GET / bucket LIST GET）走 `sources._http.request_with_retry`，byte-range 下载走 `PartialDownloader` 的 tenacity 循环，两边都用同一组触发条件（`httpx.TransportError` 含 SSL EOF / `TimeoutException` / 408 / 425 / 429 / 5xx）和同一档退避（`max_attempts=4`，指数退避 0.5–10s）。任一阶段出现瞬时网络抖动会自动重试，达到阈值后才计入 `JobFailure`。日志里以 `event=http_retry` 记录每次重试，便于事后核查。
- **断点续传**：每个 step 下载前会先看本地是否已经存在合法 GRIB（开头 `GRIB` + 末尾 `7777`），存在则记 `event=step_skipped` 直接跳过；不合法则删除重下。重跑同一条命令即可恢复中断。
- **进度事件**：`run_job` 每完成一个 init 都会发 `event=progress, done=N, total=M`，配合 `--log-file` 可用 `tail -f run.log | grep progress` 追踪长时任务。
- **CLI 退出码**：全部成功 `0`，部分失败 `1`，全部失败 `2`。


---

## 5. 编程接口（Python API）

如果你想跳过示例脚本、把下载嵌入到自己的服务里，下面是从高到低的三层 API。

### 5.1 高层：加载 YAML + 跑作业

````python path=src/climate_download/jobs.py mode=EXCERPT
def run_job(config: JobConfig, *, write_manifest: bool = True) -> list[JobResult]:
    """Execute every (cycle, step) combination declared by ``config``."""
````

```python
from climate_download.config import load_job
from climate_download.jobs import run_job
from climate_download.logging_setup import configure_logging

configure_logging()                                        # 推荐第一行调用
cfg = load_job("config/jobs/aifs_wind_pv.yaml")            # YAML → JobConfig

# 运行时直接改字段（CLI 覆盖就是这么做的）；三个字段都接受 单值 / 列表 / 范围
cfg.time.date = "20260501-20260507"   # 也可写 ["20260501", "20260502"] 或 {"start":..., "end":...}
cfg.time.cycle = [0, 12]
cfg.time.steps = "0-120:6"            # 也可写 [0, 6, 12, ...] 或 {"start":0,"end":120,"step":6}

# 想知道实际会展开成多少 init / step，可以先调 expanded_* 预览
print(cfg.time.expanded_dates())      # ['20260501', '20260502', ..., '20260507']
print(cfg.time.expanded_cycles())     # [0, 12]
print(cfg.time.expanded_steps())      # [0, 6, 12, ..., 120]

results = run_job(cfg)                                     # 返回 list[JobResult]
for r in results:
    print(r.date, r.cycle, r.step, r.output_path, r.savings_pct)
```

`JobResult` 字段（`dataclass`）：

| 字段 | 类型 | 含义 |
|---|---|---|
| `date / cycle / step` | str / int / int | 起报与时效（每个 (date, cycle, step) 一条记录）|
| `output_path` | Path | 落盘的 GRIB 子集 |
| `bytes_total` | int | 该 step 全量 GRIB 字节数（来自 .index 求和）|
| `bytes_downloaded` | int | 实际通过 byte-range 拉到的字节数 |
| `records_total / records_selected` | int / int | .index 中的 message 总数 / 入选数 |
| `http_requests` | int | 合并后实际发出的 HTTP 请求数 |
| `selected_breakdown` | dict[str, int] | 按 variable group 名称统计入选 message 数 |
| `savings_pct` | float | `100 * (1 - bytes_downloaded / bytes_total)` |

`run_job` 默认写 manifest（每个 `(date, cycle)` 一份）；要禁用（比如做 dry-run）传 `write_manifest=False`。多 init 之间按 `download.init_concurrency` 并发执行，单 init 内部 step 串行、step 内部 byte-range 用 `download.workers` 并发。

### 5.2 中层：手动驱动管道

如果想自定义"过滤后再做点什么"，可以直接拼装中层函数。源对象自己负责 URL 渲染与 sidecar 解析（不同源的 URL 形态、解析格式各异，集中到适配器里），调用方只关心过滤/合并/下载这三步：

```python
import httpx
from climate_download.config import load_source
from climate_download.grib import IndexFilter, filter_records, merge_ranges, PartialDownloader

src = load_source("config/sources/aifs.yaml")          # 返回 Source 实例（AifsSource / GfsSource / ...）

with httpx.Client(timeout=30) as client:
    records = src.fetch_records(client, date="20260507", cycle=0, step=6)

# 1) 选 100 m 风
wind = filter_records(records, IndexFilter(params=["100u", "100v"], levtypes=["sfc"]))
# 2) 合并 byte-range（gap_tolerance=64KB 时换更少请求）
ranges = merge_ranges(wind, gap_tolerance=64 * 1024)
# 3) 下载到本地
grib_url = src.build_data_url(date="20260507", cycle=0, step=6)
with PartialDownloader(max_workers=4) as dl:
    dl.download(grib_url, ranges, "wind_only.grib2")
```

这里完全没有 `if source_type == "aifs": ... elif "gfs": ...` 的分支 —— GFS / HRRR 走完全相同的代码路径，只把第一行换成对应的 YAML 即可。


### 5.3 底层：模块速查

#### `climate_download.grib.index`

| 符号 | 用途 |
|---|---|
| `IndexRecord` | 单条 .index 行的 pydantic 模型，包含 `param / levtype / levelist / step / offset / length / end` |
| `ByteRange` | `start / end / length / http_header()`，`http_header()` 返回 `bytes=start-end` |
| `IndexFilter(params, levtypes, levels, steps)` | 选择器，所有字段都是 AND；`None` = 通配 |
| `parse_index(path)` / `parse_index_text(text)` | 解析 ECMWF JSONL `.index` 文件 / 字符串 → `list[IndexRecord]` |
| `parse_wgrib2_idx_text(text, *, total_size)` | 解析 NOAA `.idx` 冒号分隔文本；`total_size` 用于推断末条 length；levtype 自动归类为 `pl/sfc/hag/hbg/atm/other` |
| `filter_records(records, selector)` | 用 `IndexFilter` 过滤，保持原顺序 |
| `merge_ranges(records, gap_tolerance=0)` | 排序后合并相邻 message 的字节段，返回 `list[ByteRange]` |

#### `climate_download.grib.partial`

| 符号 | 用途 |
|---|---|
| `PartialDownloader(client=None, *, timeout=60, max_workers=4, max_attempts=4)` | byte-range 并发下载器；可作为上下文管理器 |
| `.download(url, ranges, output_path) -> int` | 按 offset 升序拼接所有 range 写入本地，返回总字节数 |
| `PartialDownloadError` | 持久失败（4xx 配置错误、最终重试用尽、短读）抛出此异常 |

重试策略：408 / 425 / 429 / 5xx 与 `httpx.TransportError / TimeoutException` 走指数退避；4xx 配置类错误立即失败。

#### `climate_download.config`

| 符号 | 用途 |
|---|---|
| `load_job(path, *, sources_dir=None)` | YAML → `JobConfig`；`source: <name>` 字符串引用会自动到 `sources_dir`（默认 `<job 同级目录>/../sources/`）解析 |
| `load_source(path)` | source YAML → `Source` 实例（按 `type:` 字段分派到对应适配器类）|
| `load_source_dict(raw)` | 同上，但接受已解析的 dict；缺 `type` 抛 `ValueError`，未知 `type` 抛 `KeyError` |
| `TimeConfig.expanded_dates() / expanded_cycles() / expanded_steps()` | 把 `today / yesterday / YYYYMMDD / 列表 / Range / 字符串短语法` 展开成具体的 `list[str]` / `list[int]` |
| `resolve_time(time_cfg)` | **兼容用**：返回首个 `(date, cycle, steps)`，给旧代码用；新代码请直接用 `expanded_*` |
| `DateRange(start, end)` / `StepRange(start, end, step=6)` | mapping 写法对应的模型；构造时校验 `end >= start`、`step > 0` |
| `JobConfig` / `VariableGroup` / `TimeConfig` / `DownloadConfig` | pydantic v2 模型，全部 `extra="forbid"`，字段拼错会立刻报错 |

#### `climate_download.sources`

| 符号 | 用途 |
|---|---|
| `Source`（Protocol）| 适配器契约：`name / description / supports_byte_range` 三个属性 + `build_index_url / build_data_url / probe_step / fetch_records / download_step` 五个方法 |
| `BaseSource` | 默认实现混入：`probe_step`（HEAD index URL，404→False）+ `download_step`（`merge_ranges` → `PartialDownloader.download`），适合所有走 byte-range + sidecar 的源 |
| `StepDownloadResult(output_path, bytes_downloaded, http_requests)` | `download_step` 的返回值；自定义源覆盖 `download_step` 时也要返回它 |
| `register(name)` | 装饰器：把适配器类登记到 `SOURCE_REGISTRY[name]`；重名抛 `ValueError` |
| `get_source(name)` / `list_sources()` / `SOURCE_REGISTRY` | 注册表查询入口；`list_sources()` 返回当前可用 `type:` 名列表 |
| `AifsSource` / `GfsSource` / `HrrrSource` | 内置三个适配器，可作为新源的样板 |
| `sources._http.request_with_retry(client, method, url, *, max_attempts=4, **kw)` | **所有 metadata HTTP 调用必须走这里**：HEAD probe / GET sidecar / GET listing 都通过它。对 `httpx.TransportError`（含 ConnectError/ReadError/RemoteProtocolError/WriteError）、`TimeoutException` 与 408/425/429/5xx 状态指数退避重试，最多 `max_attempts` 次；其他状态（200/206/404/403…）按原样返回，调用方自行决定语义。`PartialDownloader` 的 byte-range 走自己的 tenacity 循环，但触发集与默认次数相同 —— probe / sidecar / listing / 下载共享同一档容错底线 |

#### `climate_download.manifest`

| 符号 | 用途 |
|---|---|
| `build_manifest(config, results, *, completed_at=None) -> dict` | 纯函数，把作业结果序列化为 manifest dict（不写盘）|
| `write_manifest(config, results, *, completed_at=None) -> Path` | 原子写入 `{output_dir}/{subdir_template}/{date}_{cycle:02d}z_{source}.manifest.json`（默认 subdir 即 `{source}/{date}/{cycle:02d}z`，与该 init 的 GRIB 同目录）|
| `manifest_path(config, results) -> Path` | 仅计算 manifest 路径，不写文件 |

#### `climate_download.logging_setup`

| 符号 | 用途 |
|---|---|
| `configure_logging(*, level=INFO, stderr_level=None, quiet_loggers=("httpx","httpcore",...), silence_cfgrib_future_warnings=True, log_file=None)` | 一次调用：structlog JSON（经 stdlib logging）渲染到 stderr + 可选 `log_file` 副本；`stderr_level=WARNING` 用于 tqdm bar 模式下静默 stderr 的 INFO 而仍把全量 INFO 写入 `log_file`；把 httpx/httpcore 等噪声 logger 降到 WARNING；屏蔽 cfgrib 的 `FutureWarning` |

#### `climate_download.cli`

| 符号 | 用途 |
|---|---|
| `main(argv=None) -> int` | `climate-download` 命令的程序入口（`pyproject.toml [project.scripts]` 也指向它）；接受 `argv` 便于在 Python 里直接调用 |
| `build_parser()` | 返回顶层 `argparse.ArgumentParser`，含 `run` / `list-sources` 两个 subcommand；想加新子命令在它上面挂即可 |
| `cmd_run(args) / cmd_list_sources(args)` | 两个 subcommand 的实现，可以单独 import 复用（如包成 Airflow operator）|

### 5.4 自定义源：写一个 Source 适配器

新源都从两层选一层覆盖：

| 场景 | 怎么写 |
|---|---|
| **走 byte-range + sidecar**（GRIB / NetCDF-via-DAP-byte-range / 任何能 HTTP `Range` 的源）| 继承 `BaseSource + BaseModel`，只实现 `build_index_url` / `build_data_url` / `fetch_records`；下载与 HEAD 探测复用基类 |
| **整文件下载**（NetCDF / BUFR snapshot / S3 直传 / OPeNDAP subset query）| 在上面基础上**额外**覆盖 `download_step`，自己决定怎么把数据写到 `output_path`，最后返回 `StepDownloadResult` |

整文件示例（一个 NetCDF 镜像，每个 step 一个 `.nc`）：

```python
# src/climate_download/sources/mync.py
from pathlib import Path
import httpx
from pydantic import BaseModel, ConfigDict

from climate_download.grib.index import IndexRecord
from climate_download.grib.partial import PartialDownloader
from climate_download.sources import BaseSource, StepDownloadResult, register

@register("mync")
class MyNcSource(BaseSource, BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    description: str | None = None
    base_url: str                                # e.g. https://nc.example.com
    supports_byte_range: bool = False            # 整文件下载，没有 Range 语义

    def build_index_url(self, *, date, cycle, step):
        # NetCDF 没有 sidecar：让"探测"也复用数据 URL，HEAD 200 就算可用
        return self.build_data_url(date=date, cycle=cycle, step=step)

    def build_data_url(self, *, date, cycle, step):
        return f"{self.base_url}/{date}/{cycle:02d}/f{step:03d}.nc"

    def fetch_records(self, client, *, date, cycle, step):
        # 整文件源没有"逐 message 选择"的概念，返回一条占位记录即可
        return [IndexRecord.model_validate(
            {"param": "all", "levtype": "sfc", "_offset": 0, "_length": 0}
        )]

    def download_step(self, downloader: PartialDownloader, *, records,
                      output_path: Path, gap_tolerance, date, cycle, step):
        # 完全绕开 downloader（byte-range 用不上），自己 stream 写文件
        url = self.build_data_url(date=date, cycle=cycle, step=step)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        bytes_written = 0
        with httpx.stream("GET", url, timeout=120, follow_redirects=True) as resp:
            resp.raise_for_status()
            with output_path.open("wb") as fh:
                for chunk in resp.iter_bytes():
                    fh.write(chunk)
                    bytes_written += len(chunk)
        return StepDownloadResult(
            output_path=output_path,
            bytes_downloaded=bytes_written,
            http_requests=1,
        )
```

接下来：

1. 在 `src/climate_download/sources/__init__.py` 里加一行 `from climate_download.sources import mync as _mync  # noqa: F401`，让 `@register` 在 import 时触发。
2. 写 `config/sources/mync.yaml`：
   ```yaml
   type: mync
   name: example-nc
   base_url: https://nc.example.com
   ```
3. job YAML 里 `source: mync` 直接引用。`run_job` / 测试 / manifest 全部不需要改 —— 它们只通过 `Source` 协议方法和这个适配器交互。

`tests/test_sources.py` 里的 `_FakeNcSource` 就是这个模式的最小验证用例，可以直接拿来当模板。

---

## 6. 输出产物

### 6.1 GRIB 子集

默认目录布局由 `download.subdir_template` + `download.filename_template` 决定，开箱即用为 `<output_dir>/<源>/<日期>/<起报时>z/f<step>.subset.grib2`，例如：

```
output/aifs-single/20260507/00z/f006.subset.grib2
output/gfs/20260507/12z/f024.subset.grib2
output/ifs-hres/20260507/12z/f048.subset.grib2
```

是合法 GRIB2，可直接被 `cfgrib` / `wgrib2` / `eccodes` 读取，也可以用 `xarray.open_dataset(..., engine="cfgrib")` 打开。需要自定义路径（如下游期望平铺）时按 §4.5 覆写 `subdir_template` / `filename_template` 即可。

### 6.2 manifest.json

每个 `(date, cycle)` 一份，字段示意：

```json
{
  "schema_version": 1,
  "source": { "name": "aifs-single", "description": "..." },
  "init_time": "2026-05-07T00:00:00+00:00",
  "date": "20260507",
  "cycle": 0,
  "completed_at": "2026-05-08T08:58:11.249676+00:00",
  "variables": [ { "name": "...", "levtype": "sfc", "params": ["..."], "levels": null } ],
  "download": { "output_dir": "...", "workers": 6, "gap_tolerance": 0, "timeout_seconds": 120 },
  "files": [
    {
      "step_hours": 0,
      "path": "examples_output/...subset.grib2",
      "size_bytes": 23738311,
      "sha256": "0bb16468...",
      "records_selected": 30,
      "records_total": 106,
      "http_requests": 22,
      "savings_pct": 71.17,
      "selected_breakdown": { "surface_...": 15, "pressure_...": 15 }
    }
  ]
}
```

下游 `cliamte_data` sensor 应当：
1. 监听 `*.manifest.json` 出现；
2. 读 `files[].path` 与 `sha256` 校验；
3. 落库或继续推后续加工。

manifest 是 **原子写入**（写到 `.tmp` 再 `os.replace`），sensor 看到文件存在即可视为完整。


---

## 7. 命令行入口

`uv sync` 之后 `pyproject.toml` 的 `[project.scripts]` 会注册一个 `climate-download` 命令，覆盖四个 subcommand：

| 命令 | 作用 |
|---|---|
| `climate-download run --config <job>.yaml ...` | 配置驱动的下载，可选 `--date / --cycle / --steps / --output-dir / --init-concurrency / --log-file / --progress` 覆盖 YAML 字段 |
| `climate-download list-sources` | 打印当前已注册的源 `type:` 名（registry 自检 / 排查 YAML 拼错）|
| `climate-download list-steps --source <name|path> --date YYYYMMDD --cycle N [--json]` | 调 `Source.list_available_steps`,枚举一个 init 在 bucket 里实际发布了哪些 step;`--source` 接受裸名(自动解析 `config/sources/<name>.yaml`)或 YAML 路径 |
| `climate-download list-variables --source <name|path> --date YYYYMMDD --cycle N --step H [--json | --yaml]` | 拉一个 step 的 sidecar,打印所有 `(param, levtype, levelist)` 唯一组合并附 `level_desc`(若源 sidecar 提供,如 wgrib2 idx 的 `"850 mb"` / `"2 m above ground"`);`--yaml` 直接吐 `variables:` 脚手架,按 levtype 聚合 + 排序 levels + 把 `level_desc` 作为注释列,粘进 job YAML 就能用 |

`--date / --cycle / --steps` 接受与 YAML 完全一致的字符串短语法（包括 `--steps all`）：

```bash
# 列源
uv run climate-download list-sources
# aifs    AifsSource    climate_download.sources.aifs
# gfs     GfsSource     climate_download.sources.gfs
# hrrr    HrrrSource    climate_download.sources.hrrr

# 未来 120h 预报，每 6h 一帧
uv run climate-download run --config config/jobs/aifs_wind_pv.yaml \
    --date 20260507 --cycle 0 --steps 0-120:6

# 一周历史回填，跨两个 cycle,0h 分析场,日志同时落文件
uv run climate-download run --config config/jobs/aifs_wind_pv.yaml \
    --date 20260501-20260507 --cycle 0,12 --steps 0 \
    --log-file examples_output/run.log

# 新能源业务样板:全量发布步长(S3 LIST 自动发现 209 个 step)
uv run climate-download run --config config/jobs/gfs_renewables.yaml \
    --cycle 0,12 --steps all --progress

# 列出 AIFS 当天发布了哪些 step(走 GCS XML LIST)
uv run climate-download list-steps --source aifs --date 20260510 --cycle 0
# 0
# 6
# 12
# ...

# 列出 GFS 某 step 里所有变量(默认文本表带 level_desc 列,JSON 喂下游)
uv run climate-download list-variables --source gfs \
    --date 20260510 --cycle 0 --step 6 --json | jq '.[0:3]'

# 一键生成可粘贴到 job YAML 的 variables 脚手架(按 levtype 聚合,level_desc 作注释)
uv run climate-download list-variables --source gfs \
    --date 20260510 --cycle 0 --step 6 --yaml > /tmp/gfs_scaffold.yaml
# variables:
#   - name: gfs-0p25_pl
#     levtype: pl
#     params: [HGT, TMP, RH, UGRD, VGRD, ...]
#     levels: ["50", "100", "150", "200", ..., "925", "1000"]
#     # level_desc: 50 mb, 100 mb, ...
#   - name: gfs-0p25_hag
#     levtype: hag
#     params: [TMP, RH, UGRD, VGRD, ...]
#     levels: ["2", "10", "80", "100", "1000", "4000"]
```

退出码:全部成功 `0`,部分失败 `1`(产物已写),全部失败 `2`(无可用产物)。
每次运行都会写一份 `<output_dir>/_runs/run_<ts>.json` 汇总,字段含 `succeeded / failed / results[] / failures[]`,可作为重试入口数据。

历史脚本路径仍然可用（thin shim，自动注入 `run` subcommand）：

```bash
uv run python examples/aifs_partial_download.py --config config/jobs/aifs_wind_pv.yaml --steps 0
```

### 7.1 绘图脚本

| 脚本 | 作用 | 关键参数 |
|---|---|---|
| `examples/plot_wind_speed.py` | 用 cfgrib 读 `u100/v100`，画 100 m 风速 + 风羽 | `--grib`（必填）、`--bbox lonW,lonE,latS,latN`、`--out` |
| `examples/plot_pv_radiation.py` | 画累计辐射或换算后的瞬时通量 W/m² | `--grib`（必填）、`--variable ssrd|strd`、`--prev-grib`（用于跨 step 求差→瞬时通量） |

绘图脚本目前按 ECMWF cfgrib shortName 写死（`u100/v100/ssrd/strd`），适用于 AIFS 产物。GFS 子集里的等价变量是 `u10/v10`（无 100m 风）和 `sdswrf/sdlwrf`（W/m²，已是平均通量），需要对脚本做小改才能直接套用 —— 见 §9 Q7。

---

## 8. 测试

```bash
uv run pytest                      # 全量
uv run pytest tests/test_index.py  # 仅索引解析 / 过滤 / 合并
uv run pytest tests/test_partial.py# byte-range 下载（用 respx mock httpx）
```

测试默认 **不发真实网络请求**，CI 可直接跑。示例脚本（`examples/`）走真实 HTTP，不在 pytest 内。

---

## 9. 常见问题

**Q1：`cfgrib` 报 `RuntimeError: Cannot find the eccodes library`。**
A：装 viz extra 即可：`uv sync --extra viz`，它会带上 `eccodeslib` 二进制 wheel，无需系统 `brew install eccodes`。

**Q2：累积变量（`ssrd / strd / tp`）在 step=0 全是 0。**
A：这些是从 cycle 起始时刻累积的量，step=0 物理上必然为零。要看瞬时通量，下载相邻两个 step（如 0 和 6），用 `plot_pv_radiation.py --prev-grib` 自动求差并除以时间窗。

**Q3：节省百分比为什么不更高？**
A：取决于业务变量在 GRIB 中的物理布局。AIFS 单 cycle 文件里业务变量交错分布，22 段 range 已接近下限。把 `gap_tolerance` 设为 64 KB 左右可继续合并相邻段，但代价是会顺带拉一些用不上的字节。

**Q4：能否一次跑多个 date / cycle / step？**
A：可以。`time.date / cycle / steps` 三个字段都支持单值、列表、范围三种写法（见 §4.2）。`run_job` 会按 `(date, cycle)` 笛卡尔积展开为多个 init，按 `download.init_concurrency` 并发执行；每个 init 单独写一份 manifest。

**Q5：写了 `steps: 0-360:6` 但远端只发布到 240h，会失败吗？**
A：不会。下载前会对每个 step 的 `.index` 发一个 HTTP HEAD 探测，404 的 step 自动跳过并打 warning（`event=init_steps_missing`），manifest 只记录实际拉到的文件。这让"宽范围 + 自动收敛"成为安全的写法。

**Q6：日志里 `httpx` INFO 又跳出来了。**
A：确认入口脚本第一行调用了 `configure_logging()`；如果在 Jupyter 里跑，可能 root logger 已被其它 handler 接管，传 `quiet_loggers=("httpx", "httpcore", "urllib3")` 显式覆盖。

**Q7：GFS 与 AIFS 的变量名 / 层类型怎么对照？**
A：两者用不同的 GRIB shortName 字典（ECMWF 自家命名 vs NCEP 命名）。常用对照表：

| 物理量 | AIFS（`config/jobs/aifs_wind_pv.yaml`）| GFS（`config/jobs/gfs_wind_pv.yaml`）|
|---|---|---|
| 10 m 风 u/v | `10u / 10v` @ `levtype: sfc` | `UGRD / VGRD` @ `levtype: hag, levels: ["10"]` |
| 100 m 风 u/v | `100u / 100v` @ `sfc` | **不可用**（GFS 不发布 100m）|
| 2 m 温/露点 | `2t / 2d` @ `sfc` | `TMP / DPT` @ `hag, levels: ["2"]` |
| 海平面气压 | `msl` @ `sfc` | `PRMSL` @ `levtype: atm` |
| 表面气压 | `sp` @ `sfc` | `PRES` @ `sfc` |
| 总云量 | `tcc` @ `sfc` | `TCDC` @ `atm` |
| 短/长波下行 | `ssrd / strd` @ `sfc`（J/m² 累计）| `DSWRF / DLWRF` @ `sfc`（W/m² 时段平均）|
| 总降水 | `tp` @ `sfc` | `APCP` @ `sfc`（时段累计）|
| 压力面 u/v/t/q/z | `u/v/t/q/z` @ `pl` | `UGRD/VGRD/TMP/SPFH/HGT` @ `pl` |

GFS 引入两个 AIFS 没有的 `levtype`：`hag`（height above ground）放置 10m 风 / 2m 温这类带高度的 surface 量，`atm`（column / mean-sea-level / tropopause / boundary layer 这类整层汇总）。这是 wgrib2 idx 解析器对原始 level 描述的归类结果，写 `VariableGroup` 时按这两个新 levtype 选即可。

---

## 10. S2S 次季节预报（ECDS）

5 家 S2S 中心（ECMWF / CMA / IAP-CAS / NCEP / UKMO）的 0–65 天集合预报，通过 ECMWF Data Store（ECDS）获取。和 §2–§9 描述的 byte-range + sidecar 链路是**两个独立的子项目**：S2S 走 `cdsapi` 的 submit-poll-download，每个 group 是一次完整的 `cdsapi.retrieve` 请求，落盘是一个 multi-message GRIB（不再切 byte-range）。

### 10.1 准备

```bash
# 1) 安装 cdsapi 依赖
uv sync --extra s2s

# 2) 在 ECDS 注册账号、拿 token：https://ecds.ecmwf.int/profile
#    然后写 ~/.ecdsapirc（YAML 两行）：
#    url: https://ecds.ecmwf.int/api
#    key: <your-token>
#    （凭证只从 home 读，永不进仓库；CLI 的日志会 redact）

# 3) 一次性手动接受 s2s-forecasts 数据集 license
#    https://ecds.ecmwf.int/datasets/s2s-forecasts
#    注：ECMWF / CMA / IAP-CAS / NCEP 共享同一份 license,接受一次即可。
#
# 4) UKMO 单独的 MARS 访问限制(可选)
#    UKMO S2S 在 MARS 后端有额外限制,标准 ECDS license 不覆盖；
#    实测请求会以 `AccessError: Restricted access to S2S data` 失败。
#    如需 UKMO,按 https://confluence.ecmwf.int/display/UDOC/MARS+access+restrictions#MARSaccessrestrictions-s2s
#    向 ECMWF 提交单独申请,审批通过后 cdsapi 即可访问。
```

### 10.2 中心能力索引（catalogue）

`config/s2s_catalogue.yaml` 是 5 家中心 × 变量的人读索引（含 CN 解释、单位、`max_leadtime_h` / init 节奏 / ensemble 大小）；`config/s2s/_capabilities.json` 是从 ECDS constraints endpoint 抓回的机读快照（27 KB，5570 records → 5 origins × 4 level_types），由 `tests/test_s2s_catalogue.py` 校验所有 job YAML 都是它的子集。

| 中心 | 模型 | 最大时效 | inst 步长 | 压力步长 | init 节奏 | 集合 | 关键缺失 |
|---|---|---|---|---|---|---|---|
| **ECMWF** | IFS-ENS extended | 46 d | 6h | 24h | Mon+Thu 00z | 51 | — |
| **CMA** | BCC-CPSv3 | 60 d | 6h | 24h | daily 00z | 4 | — |
| **IAP-CAS** | FGOALS-f2-S2S | **65 d** ★ | 6h | 24h | weekly Mon 00z | 4 | MSLP / 通量 / 土壤 / CAPE / 对流降水 |
| **NCEP** | CFSv2 | 44 d ★ | 6h | 24h | daily 00z | 16 | oceanic level_type |
| **UKMO** † | GloSea6 | 60 d | 6h | 24h | daily 00z (lagged) | 7 | MSLP / 通量 / 土壤 / 对流降水 |

† UKMO 的 MARS 后端有额外访问限制,标准 ECDS license 不够（见 §10.1 第 4 步）。

> 重要：ECDS 对**所有 5 家中心**的压力层数据**都只发 24h 步长**（不是 6h）。所以 `pressure_low` 这类组在 job 内本地 override 到 `step: 24`；catalogue test 会 fail 任何不符合的写法。

刷新快照（ECDS 改了 constraints hash 后才需要）：

```bash
uv run --with httpx python scripts/build_s2s_catalogue.py
uv run pytest tests/test_s2s_catalogue.py -v
```

### 10.3 source 与 job 配置

S2S 的 source YAML 极简（type / name / collection / origin / forecast_type 五字段）：

````yaml path=config/sources/s2s_ecmwf.yaml mode=EXCERPT
type: s2s
name: s2s-ecmwf
collection: s2s-forecasts
origin: ecmwf
forecast_type: control_forecast
````

job YAML 把 surface_inst / surface_daily / pressure 三类聚合成 3 个 group，每 group → 一次 `cdsapi.retrieve` → 一个 multi-message GRIB。已经为 5 家中心各自 ship 一份，按各自能力裁剪好（IAP-CAS / UKMO 用 `surface_pressure` 替代 `mean_sea_level_pressure`）：

````yaml path=config/jobs/s2s_renewables_ecmwf.yaml mode=EXCERPT
source: s2s_ecmwf
time:
  date: yesterday
  cycle: 0
  leadtime: { start: 0, end: 1104, step: 6 }   # 46d * 6h
groups:
  - name: single_inst
    level_type: single_level
    leadtime_kind: instant
    variables: [10_m_u_component_of_wind, 10_m_v_component_of_wind, ...]
  - name: pressure_low
    level_type: pressure
    leadtime_kind: instant
    leadtime: { start: 0, end: 1104, step: 24 }   # ECDS 压力层强制 24h
    levels: ["925", "1000"]
    variables: [u_component_of_wind, v_component_of_wind, temperature, geopotential_height]
````

### 10.4 跑

```bash
# 任一中心都用同一条命令；--date 必须是该中心实际 init 的日 + ≥48h 老
uv run climate-download s2s --config config/jobs/s2s_renewables_ecmwf.yaml   --date 20260511
uv run climate-download s2s --config config/jobs/s2s_renewables_cma.yaml     --date 20260511
uv run climate-download s2s --config config/jobs/s2s_renewables_iap_cas.yaml --date 20260511   # weekly Mon
uv run climate-download s2s --config config/jobs/s2s_renewables_ncep.yaml    --date 20260511
uv run climate-download s2s --config config/jobs/s2s_renewables_ukmo.yaml    --date 20260511

# 多 init 并发（共享 ECDS 队列,init_concurrency 默认 1 性能最佳）
uv run climate-download s2s --config config/jobs/s2s_renewables_ecmwf.yaml \
    --date 20260427,20260430,20260504,20260507 --init-concurrency 2

# 进度条：默认在 TTY 自动开启,两层 bar(inits + groups);
# stderr 不是 TTY(nohup / 重定向)时自动关闭,只写一行
# progress_disabled_non_tty 提示。要强制关掉就 --no-progress
uv run climate-download s2s --config config/jobs/s2s_renewables_ecmwf.yaml \
    --date 20260511                       # 交互跑：自动两层 bar
uv run climate-download s2s --config config/jobs/s2s_renewables_ecmwf.yaml \
    --date 20260511 --no-progress         # 即使在 TTY 也安静跑

# 进度条开启时,stderr 的 INFO JSON 自动降到 WARNING 以上(避免 partial_download_start
# 等高频事件冲掉 bar);要看完整 JSON 流就加 --log-file,日志文件始终保留 INFO
uv run climate-download s2s --config config/jobs/s2s_renewables_ecmwf.yaml \
    --date 20260511 --log-file output/_logs/s2s_ecmwf.log
nohup uv run climate-download s2s --config config/jobs/s2s_renewables_ecmwf.yaml \
    --date 20260511 --log-file output/_logs/s2s_ecmwf.log >/dev/null 2>&1 &
# nohup 后台自动无 bar,stderr 保持完整 INFO 流；前台 + --log-file 则 stderr 干净 + 文件全量
```

`climate-download run` 走同一套规则（双层 bar = inits + steps,bar 开启时 stderr 静默到 WARNING）。

### 10.5 输出布局

每 init 产出 3 个 multi-message GRIB（一组一个文件）+ 一份 manifest：

```
output/s2s-ecmwf/20260511/00z/
  single_inst.grib2          # ~38 MB  10u 10v msl tp ssrd strd × 185 step
  single_daily.grib2         # ~12 MB  2t 2d tcc × 46 daily window
  pressure_low.grib2         # ~10 MB  u v t z @ 925/1000 × 47 step (24h)
  20260511_00z_s2s-ecmwf.manifest.json
output/_runs/run_s2s_<ts>.json
```

manifest 的字段与 byte-range 链路一致（schema_version / files[] / sha256），但 `files[]` 是按 group 而不是按 step 聚合的。

### 10.6 容错与断点续传

- 每个 group 下载前会先校验本地是否已存在合法 GRIB（`GRIB` 头 + `7777` 尾），存在则跳过 → 重跑同一条命令即可恢复
- 单个 group 失败只会写入 `JobOutcome.failed`（带 `phase`），不会让批量失败
- `~/.ecdsapirc` 凭证在日志里 redact（`key=***`）
- 与 CLI 退出码一致：全成功 0，部分失败 1，全失败 2

### 10.7 编程 API

```python
from climate_download.s2s.config import load_s2s_job
from climate_download.s2s.jobs import run_s2s_job
from climate_download.logging_setup import configure_logging

configure_logging()
cfg = load_s2s_job("config/jobs/s2s_renewables_ecmwf.yaml")
cfg.time.date = "20260511"            # 单值 / 列表 / 范围都支持(同 §4.2)
outcome = run_s2s_job(cfg)
print(outcome.succeeded, outcome.failed, outcome.report_path)
```

---

## 11. 路线图（节选）

详见 [`PROJECT.md`](./PROJECT.md)。已完成：

- [x] GRIB `.index` 解析、过滤、range 合并
- [x] byte-range 并发下载 + 重试
- [x] YAML 驱动的 source / job 配置
- [x] manifest.json 原子写入
- [x] 结构化日志与第三方噪声静默
- [x] AIFS 端到端示例 + 风速 / 辐射可视化
- [x] 多 date / 多 cycle / 多 step（含字符串短语法）+ init 级并发
- [x] step 可用性 HEAD 探测（容错宽范围 / 未发布 step）
- [x] NOAA GFS 0.25°（wgrib2 .idx 解析 + 拆分 URL 模板 + S3 端到端）
- [x] 多源插件化（`Source` Protocol + `@register` 注册表 + 一源一文件，新源不动 `config.py / jobs.py`）
- [x] NOAA HRRR 3 km CONUS 适配器（验证插件机制对差异化 URL 的可扩展性）
- [x] `climate-download` CLI 正式入口（`pyproject.toml [project.scripts]`，`run` / `list-sources` 两个 subcommand）
- [x] S2S 次季节预报子链路（ECDS / cdsapi，覆盖 ECMWF / CMA / IAP-CAS / NCEP / UKMO 5 家，含 catalogue + constraints 快照 + 校验测试）

下一步候选：

- [ ] 其他源接入（IFS-HRES、ERA5/MARS、GEFS 集合）
- [ ] `manifest.py` / `config.py` 单元测试覆盖
- [ ] 绘图脚本参数化变量名（自动适配 AIFS / GFS shortName）
- [ ] 历史回填重型版（SQLite 任务表 + 断点续传 + 失败幂等重试）
