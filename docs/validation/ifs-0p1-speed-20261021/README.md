# IFS 0.1° 独立下载源与测速记录

测试日期：2026-10-21（北京时间）。样本为 2026-10-20 12 UTC 起报、+3 h 预报。
新增源名为 `ifs-hres-0p1`，配置引用为 `source: ifs_0p1`。

## 实测结果

使用与现有 0.25° 生产作业相同的变量组，共 24 个地面字段和 6 个 850 hPa 字段。
单时效下载量为 **57.35 MiB**（60,133,161 字节），四路并发，
相邻消息合并后发出 21 次数据请求。下面的耗时包含探测、索引、下载、文件合并、校验和及 manifest 写入，
不包含后续逐字段解码时间。

| 位置 / 入口 | 完整单时效耗时 | 平均速度 |
| --- | ---: | ---: |
| 241 / zhangmy-dev / Google 直连 | 53.19 秒 | 1.08 MiB/s |
| 本机 / Google / 现有代理环境 | 17.89 秒 | 3.20 MiB/s |

241 的镜像比较使用同一个 2 m 温度 GRIB 消息（1.31 MiB），顺序下载，速度包含连接开销：

| 镜像 | 耗时 | 速度 |
| --- | ---: | ---: |
| google | 2.03 秒 | 0.647 MiB/s |
| aws | 3.23 秒 | 0.406 MiB/s |
| ecmwf | 7.99 秒 | 0.164 MiB/s |

本轮 Google 单字段样本最快。另一次独立的 241 四路、逐消息下载取到相同 30 字段，
耗时 49.41 秒，平均 1.16 MiB/s。
这次独立探测为每个消息单独请求，共 30 次，不与上表的 21 次合并请求混作同一种测量。

以上均为一个起报、一个时效的短样本，不能据此认定整次起报耗时、持续带宽或定时任务的运行情况。

## 数据与隔离检查

- 新增 `config/sources/ifs_0p1.yaml`、`config/jobs/ifs_0p1_renewables.yaml` 与 `ifs-split` 适配器。
- 两份物理文件的字节偏移分别处理，选中的消息合并后原子发布；缺少一份文件会记录失败。
- 原有 0.25° 源、两份作业配置及 `AifsSource` 无差异；变量组逐项与现有生产作业相同。
- 本机完整解码 30 个字段，每字段 6,483,600 个有限值，无原生缺测；网格为 3600 × 1801，间距 0.1°。
- 真实消息采用 CCSDS 压缩，现有本机 ecCodes 已成功解码；各字段的单位、统计窗口及压缩元数据见 `source-acceptance.json`。
- 241 原始消息、本机合并文件及 241 新源合并文件的消息校验和一致。
- 合并文件 SHA-256：`283e74ad6151d79b705a4a5a2b6a053137ffeefab7a3cc774788795a9234243c`。
- 相关离线测试 40 项通过，见 `download-tests.xml`。
- 241 测试在内存注册新适配器，样本只写临时目录并自动清理；生产代码、配置、服务和调度未改。
- 本轮未接入 restorage、查询、监控或生产自动下载。

本机保留的完整 GRIB 样本与 manifest 位于：
`output/ifs-0p1-speed-20261021/pipeline/ifs-hres-0p1/20261020/12z/`。
该目录按项目规则被 Git 忽略；结构化验收结果保留在本目录。

## 使用与复现

从 `download/` 项目目录执行，默认只取单起报的 +3 h，输出到独立的 `output/ifs-hres-0p1/`：

```sh
uv run climate_download run --config config/jobs/ifs_0p1_renewables.yaml --date 20261020 --cycle 12 --steps 3
```

上游为滚动保留数据，后续重跑需改为仍在线且已发布 0.1° 的起报日期。
需要全时效时必须显式指定 `--steps all`；本轮没有执行全起报下载。

复现测速：

```sh
uv run python docs/validation/ifs-0p1-speed-20261021/benchmark.py --remote --output docs/validation/ifs-0p1-speed-20261021/241-results.json
uv run python docs/validation/ifs-0p1-speed-20261021/run_source.py
uv run python docs/validation/ifs-0p1-speed-20261021/run_remote_source.py
```

`run_source.py` 对已存在的样本会拒绝计为测速，以防把断点续传跳过时间当作下载时间。
再次测速请先将本轮样本目录另存，再运行；不要覆盖既有证据。

## 文件与依据

- `241-results.json`：241 三镜像、独立原始消息探测。
- `local-results.json`、`sample-decode.json`：本机单字段下载与解码。
- `source-acceptance.json`：新增源完整单时效下载及逐字段解码。
- `241-source-acceptance.json`、`241-source-run.txt`：241 临时运行新增源的真实验收记录。
- `unchanged-025.json`：0.25° 原文件校验和及变量组比对。
- `SHA256SUMS.json`：本轮脚本、配置、实现及验收工件的校验和。

官方说明：[ECMWF Open Data](https://www.ecmwf.int/en/forecasts/datasets/open-data)、
[0.1° 技术变化](https://confluence.ecmwf.int/spaces/DAC/pages/738041087)、
[已核对的实际数据目录](https://data.ecmwf.int/forecasts/20261020/12z/ifs/0p1/oper/)。

建议提交说明：新增 IFS 0.1° 独立下载源并记录实际测速结果。
