# CAMS 全球大气成分预报

独立生产源为 `ec_cams`，ADS 产品为 `cams-global-atmospheric-composition-forecasts`。
这是 ADS 提交、排队、提取、传输子流程，不使用开放数据的逐时效 byte-range 下载入口。
下游仍使用标准的每起报 manifest → `climate_restore` → `climate_zrread`。

## 固定数据契约

沿用已完成14次起报验收的52个光伏/风电相关变量。完整变量表在
`src/climate_download/cams/catalogue.py`，包含 ADS 名称、参数ID、单位、层次及原生步长。
中国区域为55–15°N、70–140°E，ADS原生0.4°网格为176×100。

每次00/12 UTC起报保留完整0–120小时：45个变量各121个逐小时时效；
过去3小时10米阵风、850hPa的位势/温度/U风/V风/比湿/相对湿度各41个3小时时效。
合计5732条GRIB消息。三个请求分别为5445、41、246字段，均低于本次核实的10000字段限制。
3小时变量在其余80个逐小时时刻的原始Zarr缺测会明确保留，不用插值冒充原始数据。

累积能量和降水保持原值，仅在切片时规范统计属性；原始stepType和累计起点另有属性留存。
读取层按实际时间间隔差分，h0无左区间的通量缺测保留。AOD保持瞬时值，阵风按区间极值处理。
`ssrd_accum`和`ssrdc_accum`沿用已验收的15分钟太阳辐射处理；长波不走太阳辐射曲线。

## 下载与恢复

```bash
uv sync --extra cams --extra s2s --inexact
uv run --extra cams climate_download cams --config config/jobs/cams_operational.yaml \
  --date 20261021 --cycle 0 --preflight-only
uv run --extra cams climate_download cams --config config/jobs/cams_operational.yaml \
  --date 20261021 --cycle 0
```

凭据只从配置中的文件读取，不写入配置、日志或命令参数。账号应自行在ADS站点接受条款。
程序只读核实站点访问、CC-BY条款、原生时效和成本，不代替用户接受条款。
00 UTC归属zmy，12 UTC归属zhangzomo；第三账号zomo不参与。一个账号只有一个下载worker。

状态在 `<root>/ec_cams/<date>/<cycle>z/.cams/<group>/state.json`。
请求ID、账号、请求摘要及原始分片固定关联；重复调用恢复同一个请求，不重复提交或自动取消。
提交前先持久化意图；提交响应不确定或提交中崩溃会停止自动重提，需只读核对ADS原请求ID后恢复。
`remote_failed`和`validation_failed`保留失败证据，必须定位后处理，不能删除状态以绕过失败。

下载的完整性检查覆盖全部参数、原生时效、起报、UTC有效时刻、层次、单位、网格和统计窗口。
先验证原始分组，再原样拆成121份逐时效GRIB，保存逐消息及逐文件SHA256。
三个分组齐全且总计5732条消息后，最后原子发布标准 `*.manifest.json`。
未完成的下载不会发布ready信号。完整重复运行不改manifest时间戳。

## 241定时运行

`deploy/cams_run.py`部署为 `/home/zhangmingyu/operation/cams_run.py`。
每5分钟分别驱动00/12 UTC，宿主机和容器内都有锁；每10分钟单worker扫描CAMS完整manifest。
原有其他源的下载、切片和资源配置保持独立。

当前配置 `publish_lag_hours=10`：00 UTC起报在北京时间当天18:00起进入下载候选，
12 UTC起报在北京时间次日06:00起进入候选。定时任务在随后第一个5分钟检查点尝试下载；
完整原生字段尚未发布时顺延，已有请求继续使用原ID。这个时刻是开始尝试时间，完成还需ADS排队、传输和切片。

`first_init="2026102100"`为本次部署下限，`lookback_days=1`仅恢复近期已发布起报。
已经保存请求ID的未完成起报会优先恢复，日期窗口移动也不会另提替代请求。
生产下限之前的历史请求由其独立回补worker续传，正常cron扫描不会接管或阻塞这些请求。
这不是全年回补入口；CLI单次`--date`也不接受日期区间。
官方00/12 UTC发布约为10/22 UTC，ADS排队、传输及切片延迟另计，不保证+10h已经落盘。
官方时刻依据：[ECMWF dissemination schedule](https://confluence.ecmwf.int/pages/viewpage.action?navigatingVersions=true&pageId=738034858)。

宿主机运行记录为 `operation/cams/run-00.json`、`run-12.json`及`runs.jsonl`，包含真实触发方式和时间。
账号组状态保存ADS排队/提取时间戳、传输尝试字节数和时长；未完成请求只报告已等待时间。

## 验收与回滚

本次部署工件位于 `docs/validation/cams-production-20261022/`。
241备份位于 `/home/zhangmingyu/operation/cams_integration_20261022/backup`，原crontab及文件摘要一并保留。
读取服务旧镜像保留为 `climate-watcher:before-cams-20261022`。
验收必须包括实际cron触发、新起报完整下载/切片、全变量原值核对、线上查询和NetCDF回读。
此前实验样本可用于回归，但不能替代本次生产下载及定时运行证据。

2026-10-22按用户要求将生产源名由 `cams-global` 迁为 `ec_cams`，ADS产品ID保持不变。
已有原始数据、manifest和Zarr同步改名，原请求ID、账号与数据字节保持不变；
之前的验收报告保留当时的名称，改名验收另存于 `docs/validation/ec-cams-rename-20261022/`。

2026-10-22另按用户授权冻结最近一个完整月：2026-09-22至2026-10-21，每天00/12 UTC共60起报。
42起报需要新下载，4起报只读复用此前已验收原值，其余14起报沿用原正式流程及请求。
新请求在独立暂存目录排队，每账号最多一个未提取完成请求和一个已就绪传输任务；
实时任务优先，旧近一周同账号worker完成前不启动月份传输。全部52变量原生身份与SHA通过后原子发布。
所有60起报就绪后重新进行全量读取验收，最终要求343920条原始消息和420份NetCDF回读及SHA256通过。
清单、续传规则和实时状态见 [月份回补目录](validation/ec-cams-backfill-month-20261022/README.md)。
该有限回补不改变生产日期下限、cron或读取服务；不启动全年回补。
