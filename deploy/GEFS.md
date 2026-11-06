# GEFS 自动生产

`gefs_run.py` 在宿主机运行，配置文件通过 `--config` 指定。每天北京时间 17:30
（09:30 UTC）当天 00z 起报开始符合调度条件；每半小时重试。最近三天及状态文件中
未完成的旧日期都保留，最新优先；完整并已发布的日期只检查完成回执和两个产品标记。

配置项：container、release_root（容器内固定版本目录，其下 download 与
climate_restorage 均有独立 .venv）、data_root、zarr_root、host_zarr_root、
start_date（YYYYMMDD，首次部署之前不自动回填）、member_count（1–31，默认 5）、
state_file、lock_file（后两项为宿主机路径）。成员数包含 c00。已有日期的成员选择
不可静默更改；需要扩容时应明确迁移旧日期，或从新起报开始配置。

```cron
0,30 * * * * /usr/bin/python3 /home/zhangmingyu/operation/gefs_run.py --config /home/zhangmingyu/operation/gefs.json >> /home/zhangmingyu/operation/logs/gefs.log 2>&1
```

宿主机时区不参与日期边界计算。开始时独占 host lock；容器内每阶段用 GNU timeout
限定两小时并终止整个进程组，避免宿主超时留下孤儿任务。两核 affinity、BLAS 单线程、
低优先级、每进程 12 GiB 地址空间上限；重整最多两个解码 worker。原容器总额度不变。

下载使用 NOAA HTTP Range，4 线程，仅拿 15 个气象变量，所有成员×181 时次均通过
GRIB 头校验才写 manifest。失败不发布，已下载内容留待下轮续传。此任务不通过 ECDS。

切片调用 restorage 的 `deploy/gefs_publish.py`：核对原始 SHA256、完整时次和成员，
私有暂存目录完成中国区域切片、全变量解码、窗口/缺失检查后原子发布 `gefs-ens`；
接着单独导出并验证 `gefs-ctrl`，更新两个覆盖索引。两个产品都成功才记录整轮完成。
重试不覆盖具有冲突来源的已发布数据。原始文件转冷存策略不随此任务自动变更。

回滚：删除仅本任务的 cron 标记块，等待当前任务结束，恢复之前的固定版本配置。
保留已有数据及 state 文件；不终止其他下载任务。索引巡检应增加本任务 lock_file 到
`--extra-lock`，避免与发布同时维护同一覆盖索引。

`download_base_url` 可指定 NOAA NOMADS 的近期数据根地址
`https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod`；默认仍为 S3 存储。
2026-11-06 服务器实测同一 1 MiB Range：NOMADS 约 5 秒，S3 约 48–65 秒，
因此该服务器实时任务使用 NOMADS。较旧历史日期应显式使用 S3。
切换同一 NOAA 数据的端点不改变请求身份，保留已验证成员/时次的断点；下载后仍检查全部 GRIB 身份。
