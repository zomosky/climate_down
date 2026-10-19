# 历史起报清点方法

核查日期为 2026-10-16，历史计数截至完整 UTC 日 2026-10-15。
1 批指一个数据源的一次 UTC 起报，f240、f360 两包合在一起算 1 批。
两个包内部的概率记录数量不计为起报次数。

## 范围与证据等级

对象入口为匿名 Google Cloud Storage 桶 `ecmwf-open-data`，产品限定为当前
0.25°、`enfo/ep` 的 240h/360h 包。扫描 2024-01-01～2026-10-15 共 1,019 天，
逐页读取目录直到完整，不能把空分页当作无数据。记录每个对象的名称、大小和创建时间。

IFS 按当前 pipeline 的 00/12 UTC 统计；AIFS 的 00/12 UTC 进行同等核查，
另外清点 06/18 UTC 文件库存。每批同时具备非空的两份 GRIB 和两份 index 才称为文件齐全。

对文件齐全的全部 00/12 UTC 起报读取索引，检查当前 IFS 五字段的 145 条记录、
AIFS 十四参数对应的 313 条记录，要求参数、全部原始窗口和数量精确匹配且无重复。
再按索引从每个包抽取一条 tpg50 的 16 字节 GRIB2 头，核对魔数、版本和消息长度。
已检查全部选定索引记录是否超出 GRIB 对象大小；HTTP 不支持 Range 时不会读取整包。

因此，“原样索引及头部检查通过”是下载候选资格，不能等同于整批 GRIB 解码、
全部空间数值检查、Zarr 落盘或 API 查询验收。未批量下载这些历史预报网格。
真实回填仍应经过现有 manifest、概率适配器及读取链路的完整验收。

## 旧 AIFS 偏移恢复分析

部分旧索引在概率段重新从 0 计算 `_offset`，但 GRIB 对象实际包含多个连续产品段。
分析工具先要求所有索引记录 `_length` 之和等于整个 GRIB 对象大小，再按索引行序
累计消息长度生成全文件偏移。每个包再次核对全部所需字段、窗口，以及两条 tpg50
消息的修正后头部。该工具只作核查，没有修改生产下载适配器。

另在 `2025-07-02 12 UTC` 和 `2026-05-12 12 UTC` 的两个包中分别读取完整
tpg50 消息，实际用 ecCodes 解码全部 1,038,240 个网格概率值，检查阈值、窗口、
单位、有限值和数值范围。结果保存于 `legacy-decoded-samples.json`。
这四条消息的解码证据不能替代全部历史变量和消息的验收。

## 工件与重现

- `inventory-days.jsonl`：完整目录快照，包括 AIFS 额外时次。
- `init-checks.jsonl`、`summary.json`：逐起报索引/头部核查及汇总。
- `legacy-offset-checks.jsonl`：旧 AIFS 偏移恢复分析。
- `historical-inits.csv`：全部扫描时次，包含尚未出现该产品的早期日期。
- `download-candidates.csv`：文件齐全的起报及其当前下载条件分类。
- `monthly-counts.csv`：按源、月份汇总的批数。
- `research-summary.json`、`audit-provenance.json`：研究汇总和脚本校验和。
- `audit-tests.xml`：9 项针对性测试通过；覆盖分页、缺包、重复记录、索引长度、
  忽略 Range 的保护、连续区间、统计口径隔离及旧偏移重建的前置条件。

在 download 项目内运行：

```sh
uv run --no-sync python scripts/audit_probability_history.py \
  --start 20240101 --end 20261015 --workers 8 \
  --out docs/validation/probability-history-20261016
uv run --no-sync python scripts/audit_probability_legacy_offsets.py \
  --out docs/validation/probability-history-20261016
uv run --no-sync python docs/validation/probability-history-20261016/build_report.py
```

主清点脚本支持复用已成功的本地缓存；要重新核查远端，应使用新的输出目录。
目录状态及云端历史保有量是核查时的观测结果，不是上游永久保存承诺。

官方参考：[Open Data](https://www.ecmwf.int/en/forecasts/datasets/open-data)、
[AIFS 历史数据与 MARS](https://www.ecmwf.int/en/forecasts/datasets/aifs-machine-learning-data)、
[GCS 对象列表与分页](https://docs.cloud.google.com/storage/docs/json_api/v1/objects/list)。
