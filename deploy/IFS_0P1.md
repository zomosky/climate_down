# IFS 0.1°：Google 独立接入

源名 `ifs-hres-0p1`，下载配置键 `ifs_0p1`。原 `ifs-hres` 0.25° 的作业、
目录、时次和参数不变。本作业的 00/12 UTC 范围内，0.1° 从 2026-10-20 12 UTC
开始提供，补录策略不请求此前起报。

## 数据与处理

- Google `ecmwf-open-data` 的 `ifs/0p1/oper`，每个时效分别读取 sfc/pl 索引，
  各自请求字节范围后原子合并；不同物理文件的偏移不会混用。
- 与现有 0.25° 新能源作业同一组字段：24 个地面字段、6 个 850hPa 字段。
  原生时效为 0–144h 每 3h、150–360h 每 6h，共 85 个。
- 下载作业 `config/jobs/ifs_0p1_renewables_ser.yaml`：8 路请求、单起报，
  原始数据 `/climate_data/ifs-hres-0p1/<date>/<cycle>z/`，manifest 仍是唯一就绪信号。
- 重整作业 `climate_restorage/config/jobs/ifs_0p1_zarr_china_ser.yaml`：2 个解码进程，
  70–140°E、15–55°N，原生 401×701 网格，输出
  `/climate_data/climate_data_storage/zarr/ifs-hres-0p1/`。
- `climate_zrread` 注册同名 profile，查询/导出沿用 IFS 规则；Watcher 自动发现
  独立源和产品，业务可用延迟暂按 12h，默认 12z，可显式选择 00z 或 mix。

## 调度配置

依赖既有宿主机 `download_run.sh`、`restore_scan.sh`。通过新源配置和独立锁调用，
不修改原 0.25° 调度。下面是当前使用的北京时间调度块：

```cron
# BEGIN IFS-0P1 Google
5-55/10 4-7 * * * /usr/bin/python3 /home/zhangmingyu/operation/download_ifs_0p1.py --cycle 12 >> /home/zhangmingyu/operation/logs/download_ifs_0p1_12z.log 2>&1
5-55/10 16-19 * * * /usr/bin/python3 /home/zhangmingyu/operation/download_ifs_0p1.py --cycle 0 >> /home/zhangmingyu/operation/logs/download_ifs_0p1_00z.log 2>&1
*/10 * * * * /usr/bin/python3 /home/zhangmingyu/operation/restore_ifs_0p1.py >> /home/zhangmingyu/operation/logs/restore_ifs_0p1.log 2>&1
# END IFS-0P1 Google
```

2026-10-22 起，12z 从次日 04:05、00z 从当日 16:05 开始检查，每 10 分钟重试，
分别到 07:55 和 19:55。公开 0.1° 数据比 0.25° 约晚 2 小时；Google 最近三个起报
实查为 05:34 / 17:34 左右开始发布，约 6 分钟发完。本调整使下载尽快衔接实际发布，
不能提前取得尚未发布的数据。

`download_ifs_0p1.py` 先复用既有 `manifest_health` 检查完整 85 个时效及文件大小，
健康时直接退出，不重复请求索引或下载。其余情况委托原有 `download_run.sh`，
用 `PUBLISH_LAG_HOURS=8` 计算日期并显式传入 `INIT_DATE`，整个重试窗口保持相同，
防止提前检查误选前一天起报。尚无时效时，既有下载器只记录检查并跳过，不生成 manifest；
后续继续枚举已发布时效并断点续传。相同作业和 cycle 沿用同一个锁，已有下载运行时
新检查不并发下载。健康产品重复检查不改写 manifest，未齐 85 个时效不视为全量完成。

`reconcile_sources.py` 的独立 00/12z 策略在起报后 12h 检查完整 85 个时效，
回看 1 天且截断到首发时间；健康产品不重复下载或改写 manifest。
`recover_all_on_boot.sh` 已调用该补录入口，因此沿用开机恢复。

`restore_ifs_0p1.py` 先读容器的内存限额、当前用量与可回收文件缓存，
至少有 12GiB 有效余量才调用既有扫描入口。只扣除 `inactive_file` 缓存，
不清缓存、不改变容器资源限额。配置独立源、2 个进程与独立锁；余量不足留待下轮。

```sh
python3 /home/zhangmingyu/operation/restore_ifs_0p1.py --dry-run
python3 /home/zhangmingyu/operation/reconcile_sources.py --only-source ifs-hres-0p1 --dry-run
```

在完整真实数据验收前不启用以上新增调度。生产启用状态、版本哈希、回滚方式、
逐阶段耗时与验收工件在 [本次验收记录](../docs/validation/ifs-0p1-integration-20261021/README.md)。
提前检查的变更、实际发布时差与回滚记录见
[2026-10-22 调度核验](../docs/validation/ifs-0p1-schedule-20261022/README.md)。
旧的冷存储脚本尚未纳入新源；这里不启用冷迁移或删除原始数据。

更早的高分辨率历史、MARS 档案权限申请与费用见
[IFS 0.1° 历史档案说明](../docs/IFS_0P1_HISTORY.md)。
