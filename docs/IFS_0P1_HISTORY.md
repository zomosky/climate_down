# IFS 0.1° 历史档案：申请、费用与覆盖

核实日期：2026-10-22。公开镜像与 MARS 原始档案是两种获取途径。

## 申请与费用

ECMWF 的 [Service Agreement Pricing](https://www.ecmwf.int/en/forecasts/accessing-forecasts/payment-rules-and-options/tariffs)
目前列明：商业用户通过 Web API 访问档案，服务费为 **3,000 欧元/年**。
这是档案访问年费，不是按历史年份分别购买。每个组织或企业集团最多三个访问账号；
部分实时服务包已包含档案访问。

1. 使用现有 ECMWF 账号登录
   [Archive order](https://products.ecmwf.int/shopping-cart/archive-orders/new)，申请 Archive / MARS Web API 服务。
2. 说明机构、实际用途和数据需求，由 ECMWF Data Support 办理服务协议和开通。
   对申请入口或产品选择有疑问时，使用 [ECMWF Support Portal](https://support.ecmwf.int/)。
3. 权限开通后，先查询目标字段和日期的目录、体量，再用 ECMWF Web API 取一个小样验收，随后批量回补。

申请需求可明确为：IFS 业务 HRES/control 预报，`class=od, stream=oper, type=fc, expver=1`，
00/12 UTC，中国范围 `area=55/70/15/140`，输出 `grid=0.1/0.1`；希望取得原始高分辨率归档场，
并核实 2m 温度、100m 风、累计辐射等本项目 30 个字段在目标年份的可用日期和时效。

纯非商业科研项目可以申请部分或全部费用减免。需说明项目名称、所属机构、内容、期限、成果及
公开数据无法满足需求的原因；必须满足非商业和开放发表要求，不能用于提供服务、服务原型或未来商业化。
官方说明费用减免申请可能需要最多两周处理，须经批准。商业预测业务应按实际商业用途申请。
见 [Fee Waiver 官方申请规则](https://confluence.ecmwf.int/spaces/DAC/pages/400510594/What+are+the+criteria+to+qualify+for+a+Fee+Waiver+for+a+Research+Project+with+ECMWF)。

本项目现有账号身份认证成功，但 MARS 服务信息和一次历史目录查询均返回 **HTTP 403，无服务权限**。
注册普通账号、持有 API key 或 CDS 权限，并不会自动取得 MARS 业务档案权限。
本次未申请付费服务；[脱敏核验记录](validation/ifs-0p1-schedule-20261022/mars-access.json)
没有提交气象场下载。

## 能取得多久以前的 0.1° 历史

| 数据含义 | 历史范围依据 |
| --- | --- |
| 新的免费公开 0p1 文件 | 从 2026-10-20 开始公开；本项目 00/12 UTC 范围内首期是 20 日 12z |
| MARS 原始约 9 km HRES 场输出到 0.1° 经纬网格 | HRES 自 2016-03-08 升级到约 9 km，因此可按这个日期起查询同级分辨率的业务历史，至今约十年七个月 |
| 2016-03-08 以前的模型档案 | 可能有更早但分辨率较粗的原始场；输出更密的 0.1° 网格不会增加原始空间信息 |

官方依据：
[IFS Cycle 41r2 分辨率升级](https://www.ecmwf.int/en/newsletter/147/meteorology/new-model-cycle-brings-higher-resolution)、
[MARS 输出网格](https://confluence.ecmwf.int/spaces/UDOC/pages/123799065/grid+-+keyword+in+MARS+Dissemination+request)、
[公开数据说明](https://www.ecmwf.int/en/forecasts/datasets/open-data)。

2016-03-08 是原始业务模式进入约 9 km 的界线，不是已逐日验证本项目所有字段完整的结论。
当前没有档案权限，尚不能确认这 30 个字段及现有 85 个时效在十年期间逐项齐全。
历史模式版本、变量、最大预报长度和采样频率都可能不同；不应把当前 0—360h 契约直接套到全部早期年份。
授权后必须先查目录，再验证原始场、统计窗口、网格和数值。

普通档案权限仅适用于已过有效期的数据。
[MARS 访问规则](https://confluence.ecmwf.int/spaces/UDOC/pages/47290687/MARS+access+restrictions)
以“起报 + 时效 + 24 小时”判断受限数据；因此新起报的完整 +360h 套件约需起报 16 天后才全部过期。
例如 2026-10-20 12z 的 +360h 场约到 11-05 12 UTC 后越过该门槛，不能把档案年费理解成实时数据授权。

## 接入与当前部署

当前免费 Google 源保持独立名称 `ifs-hres-0p1`；原 0.25° 不变。提前检查时间及部署验证见
[IFS 0p1 运行说明](../deploy/IFS_0P1.md) 和
[2026-10-22 调度核验](validation/ifs-0p1-schedule-20261022/README.md)。
取得档案权限后，MARS 返回的数据需单独记录原始来源、模型版本、网格转换、字段及统计窗，
经过小样对照后才能纳入历史回补，不能声称与新公开文件逐比特一致。
