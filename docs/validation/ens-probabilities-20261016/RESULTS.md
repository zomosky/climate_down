# ENS 概率源接入验收（2026-10-16）

已接入下载、manifest、区域重整、统一查询、导出、监控与网页。服务器为
`192.168.1.241`，对外服务端口 8090。验收起报：2026-10-15 12 UTC。

| 数据源 | 字段 | 原始窗口样本 | 原始包 |
|---|---:|---:|---|
| ifs-ens-probabilities | 5 | 145 | 240h/360h |
| aifs-ens-probabilities | 25（14 个原始参数展开） | 313 | 240h/360h |

IFS 字段为 `tpg20/tpg50/tpg100/10fgg15/10fgg25`。AIFS 包括瞬时、24 小时及
48/72/120 小时窗口。重整区域 70–140°E、15–55°N，0.25°，网格 161×281。

## 数据与查询

- 本机和服务器均实际从公开数据端下载两个完整包；4 个原始文件的 SHA-256 相同。
- 每条原始消息在 31°N、101°E 网格点的概率值、窗口起止，与裁剪后 Zarr、默认
  查询和正式 API 逐条对账，共 458 条，差异容差 1e-6；完整区域的有限值均在 0–100。
- 概率保留 `%`，默认去累积不改变数值；原始阈值、GRIB probabilityType、比较方式、
  统计方式及逐样本窗口保留。AIFS 同参数不同窗口拆开，不交叉覆盖。
- 站点、多源、区域平均、训练查询通过。窗口起点即使早于查询范围仍原样保留。
  多源坐标分别命名，导出时窗口和有效时间统一转换到查询时区。
- JSON、CSV、Parquet、Arrow、NetCDF、Zarr 均实际读回；正式 API 的 CSV、Parquet、
  Arrow、NetCDF 也读回。浏览器 CSV 下载含百分比原值及完整窗口列。
- 15 分钟概率重采样被明确拒绝（HTTP 422）。不存在原始样本的时刻保留缺失。
- 自动实时查询允许首个窗口结束前读取后续原始样本，普通预报源的覆盖判定不变。
  多日概率地图只取真实窗口，前后切换使用原始时次列表。

依据：`raw-comparison.json`、`query-comparison.json`、
`production/production-raw-comparison.json`、`production/api-acceptance.json`、
`production/browser-query.csv`、`production/ui-*.png`、`browser-trace.zip`。

## 调度与服务

宿主机实际时区已核对为 `Asia/Shanghai`。crontab 已新增两条每日任务：

| 北京时间 | 下载起报 |
|---|---|
| 05:15 | 前一 UTC 日的 12z |
| 17:15 | 当日 00z |

入口 `/home/zhangmingyu/operation/probabilities_run.py`；两个源串行，下载 workers=4、
init_concurrency=1，重整 workers=1，并限制数值库线程。默认检查前一天同 cycle，
失败最多尝试 3 轮、间隔 15 分钟，未完整就绪不当作成功。全局锁防止重复运行。

服务器首轮已人工执行成功；再次运行返回 0，原始 GRIB、manifest 和 Zarr 标记的
大小、mtime 均不变。首次自然定时触发在验收结束时尚未发生，不能把人工运行
当作定时触发证据。实际调度快照见 `production/crontab.after`，幂等记录见
`production/resume-acceptance.json`。

正式服务健康检查通过，沿用现有 **12 CPU / 24 GiB** 限额；没有新增常驻生产容器。
临时验收容器为 2 CPU / 4 GiB，验收后移除。发布在原线上镜像基础上叠加本次文件，
保留已有修复；最终镜像与健康状态记录见 `production/promotion-final.json`。
原镜像 `climate-watcher:pre-ens-20261016` 及代码、crontab 备份保留于
`/home/zhangmingyu/operation/backups/ens-probabilities-20261016`。

没有提交或推送任何 Git 仓库。原有用户修改保持原样。

## 回归与复现

| 范围 | 结果 |
|---|---|
| download 全部测试 | 126 通过、1 个既有实数样本缺失而跳过 |
| restorage 全部测试 | 32 通过、3 个既有实数样本缺失而跳过 |
| zrread 标准 tests 目录 | 282 通过；后续窗口/自动起报相关专项 25 通过 |
| watcher 全部测试 | 130 通过；后续稀疏窗口地图及 query API 专项 41 通过 |
| 前端类型检查与构建 | 通过 |
| 真实端到端 | 下载→manifest→Zarr→查询/API→导出读回→网页验证通过 |

开发初次从 zrread 仓库根收集测试时，额外扫入用户既有
`artifacts/solar-boundaries-20261013/test-snapshot`，产生 2 个旧快照 fixture 错误；
限定正常 `tests/` 后通过。没有修改这些旧工件。日志含已有依赖弃用与构建体积提示，
不影响以上结果。各次测试有重叠，不能把表中数字相加当作独立用例总数。

复现脚本：`scripts/validate_ens_probabilities.py`。在 restorage 环境运行 raw 阶段，
在 watcher 环境运行 query/API 阶段；API 阶段使用 Python 标准库网络访问，可在
不安装开发依赖的服务环境中运行。示例（从 watcher 目录）：

```sh
uv run python ../download/scripts/validate_ens_probabilities.py --phase query --raw-root ../climate_data --zarr-root ../climate_data_storage/zarr --out ../download/docs/validation/ens-probabilities-20261016
uv run python ../download/scripts/validate_ens_probabilities.py --phase api --raw-root ../climate_data --zarr-root ../climate_data_storage/zarr --out ../download/docs/validation/ens-probabilities-20261016/production --base-url http://192.168.1.241:8090
```

原始数据与可再生成的大体积工件未纳入 Git；报告和查询规范可单独审阅。
运维与回滚见 `deploy/ENS_PROBABILITIES.md`，工件校验和见 `SHA256SUMS.json`。
