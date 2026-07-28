# WeatherNext 2 Mean — 探索笔记（草稿，待补充）

> 记录 2026-07 对 Google DeepMind **WeatherNext 2 Mean** 作为潜在数据源的调研。
> 结论：数据已验证可读、可切成和其它源同格式的中国区 Zarr；**定位为 GraphCast 的升级替代（风/温/形势），不含光伏辐照**。尚未正式并入项目。
> 同目录 `weathernext_reader.py` 是验证过的读取器原型。

---

## 1. 它是什么

- Google DeepMind 的新一代 AI 天气模型（GraphCast 的后继），**64 成员集合，本产品是集合均值**。
- 全球 **0.25°**，原生 **Zarr**（不是 GRIB）。
- 历史 **2022-01-01 至今**，每天 4 场起报（00/06/12/18z）。
- 官方文档：
  - 接入指南 https://developers.google.com/weathernext/guides/gcs
  - 数据目录 https://developers.google.com/earth-engine/datasets/catalog/projects_gcp-public-data-weathernext_assets_weathernext_2_0_0_mean
  - Starter notebook（含更多示例）：`gs://weathernext-public/colabs/WeatherNext_2_Starter_Guide_Zarr_on_Google_Cloud_Storage.ipynb`

## 2. 访问方式（四个坑，缺一不可）

这是**非公开桶**，需要 Google 授权 + 认证，且从国内还要过代理。踩全了才通：

1. **代理**：本机（macOS）到 GCS 必须走 Clash 代理 `127.0.0.1:7890`（`proxy_on`）。gcsfs 需要 `session_kwargs={"trust_env": True}` 才会用代理。裸命令会 `HTTP 000 / SSL_ERROR_SYSCALL`。
2. **授权**：先在 https://developers.google.com/weathernext/guides/gcs 填 Data Request 表单拿访问权（绑到 Google 账号，本例 `zomoskynuist@gmail.com`）。
3. **认证**：`gcloud auth application-default login`（**不是** `gcloud auth login`；后者只认证 CLI，不生成 ADC，client 库用不了）。gcloud 用 brew cask 装在 `/opt/homebrew/share/google-cloud-sdk/`。
4. **删掉 ADC 的 quota project**：登录会把 `quota_project_id` 设成一个没开账单的项目，GCS 报错
   `The billing account for the owning project is disabled in state absent`。
   把该键从 `~/.config/gcloud/application_default_credentials.json` 删掉（留了 `.bak`），再用 `token="google_default"` 就不会带 `x-goog-user-project`。
   - **桶不是 requester-pays** → egress 由 Google 出 → **读取免费**（那个 billing 报错纯粹是 quota project attribution，删掉即可）。

打开示例：
```python
import xarray as xr
so = {"token": "google_default", "session_kwargs": {"trust_env": True}}
ds = xr.open_zarr("gs://.../<init>/predictions.zarr", storage_options=so, consolidated=True, chunks=None)
```
> 依赖：`uv run --with gcsfs --with zarr --with xarray --with fsspec --with google-auth --with numcodecs`。
> 注意 grpcio(11.6MiB) 在代理上下得很慢，第一次装要耐心；装好后缓存。

## 3. 数据布局与结构

**布局**（每个起报一个独立 Zarr）：
```
gs://weathernext/weathernext_2_0_0_mean/zarr/
  ├── 2022_to_2023/  2023_to_2024/  2024_to_2025/  2025_to_present/     # 按年份分段
  │     └── {YYYYMMDD}_{HH}hr_01_preds/
  │           ├── predictions.zarr        # ← 真正的 zarr store
  │           └── success                 # 完成标记
  └── weathernext_2_0_0_mean_file_structure.pdf
```

**`predictions.zarr` 结构**：
- dims：`time=60, lat=721, lon=1440, level=13`
- 坐标：
  - `init_time`（scalar，起报时刻）
  - `time`（timedelta，**预报步 6h→360h = 15 天，60 步**）
  - `datetime`（沿 time，有效时刻）
  - `lat`：-90..90 **升序** 0.25°
  - `lon`：**0..359.75（0-360 约定）** 0.25°
  - `level`：50..1000 hPa（13 层）
- **分块**：地面 `(1,721,1440)`、气压 `(1,1,721,1440)` —— **每时次/每层一个全球块（~4.15MB）**。
  → 切中国区仍要下整块（96% 是中国外数据被丢），但选**层**是真省的（按层分块）。

**16 个变量**：
| 类 | 变量 |
|---|---|
| 地面风 | `10m_u/v_component_of_wind`、`10m_wind_speed`、`100m_u/v_component_of_wind`、`100m_wind_speed` |
| 温/压 | `2m_temperature`、`mean_sea_level_pressure`、`sea_surface_temperature` |
| 降水 | `total_precipitation_6hr` |
| 气压层(13层) | `geopotential`、`specific_humidity`、`temperature`、`u_component_of_wind`、`v_component_of_wind`、`vertical_velocity` |

**没有**：辐射、云量、阵风、2m 露点/湿度、皮肤温度、CAPE、可降水。

## 4. 预报时长 / 分辨率 与各源对比

| 源 | 时长 | 分辨率 | 100m风 | 辐射 | 云量 |
|---|---|---|:-:|:-:|:-:|
| **WeatherNext** | **15 天** | **纯 6h** | ✓ | ✗ | ✗ |
| GFS | 16 天 | 1h→3h | ✓ | ✓ | ✓ |
| IFS | 15 天 | 3h→6h | ✓ | ✓ | ✓ |
| AIFS | 15 天 | 纯 6h | ✓ | ✓ | ✓(tcc) |
| ICON | 7.5 天 | 1h→3h | ✗ | ✓(直+散最全) | ✓ |
| GraphCast | 16 天 | 纯 6h | ✗ | ✗ | ✗ |

**定位结论**：
- 强项 = **风电 + 天气形势**：原生 100m 风 + 风速、13 层气压廓线（最全）、15 天、海温、免费认证 GCS 稳。
- 短板 = **光伏**：无辐射、无云量（同 GraphCast）→ PV 必须配 ICON/IFS/GFS。
- 本质是 **GraphCast 的升级替代**（精度更高、历史 2022 起更深、原生 Zarr、认证 GCS 比中国↔AWS 稳）。

## 5. 读取器（原型：`weathernext_reader.py`）

它把 download+restore **合成一步**（源已是 Zarr，省掉 GRIB 解码）：
```
认证开 predictions.zarr → sel(lat 15..55, lon 70..140) → 选变量/层 → 改名对齐 → 落中国区 Zarr
```
**架构对应**：GCS 原生 Zarr = 其它源的 GRIB/nc（原始，不复制到本地）；reader = restore；
产物 `climate_data_storage/zarr/weathernext/{date}_{cc}z_weathernext.zarr` = 处理后 Zarr（同其它源布局：坐标 `step/latitude/longitude/time/valid_time`）。

**已定稿的下载变量集**（风电向 + 完整替代 GraphCast）：
- 地面（10）：`u10 v10 ws10 · u100 v100 ws100 · t2m · prmsl · sst · tp`
  - ⚠️ 原生 `ws10/ws100` 必取：集合均值下 `风速均值 ≠ 均值风的风速`（前者≥后者），用 u/v 反算会**系统性低估风速**。
- 气压 @ **925、1000**（低层近轮毂）：`u v t w`

**变量改名**（WeatherNext→我们）：`10m_u→u10, 100m_u→u100, 2m_temperature→t2m, mean_sea_level_pressure→prmsl, total_precipitation_6hr→tp, sea_surface_temperature→sst, geopotential→z, specific_humidity→q, temperature→t, u/v_component_of_wind→u/v, vertical_velocity→w`。

**维度映射**：`time(lead)→step, lat→latitude, lon→longitude, level→pressure_level, init_time→time(scalar), datetime→valid_time`。

**写盘注意**：源 zarr 用 Blosc，zarr v3 重用会报 `Expected a BytesBytesCodec`。要 `ds.load()` 后清 `encoding`、以 **zarr_format=2 + zstd** 写（和其它源一致）。

## 6. 验证结果（20250101 12z，本机走代理）

- 输出 **159MB** 中国区 Zarr，14 变量、60 步 × 2 层，结构和其它源逐格对齐（lat 15–55 161点 / lon 70–140 281点）。
- 数值合理：t2m 234/272/302 K；100m 风 max 16.9 m/s；u/v 反算风速 16.86 ≈ 原生 ws100 16.93（自洽）。
- **tp 有极小负值**（-5.5e-6 m，ML 模型噪声）→ 下游 clip 到 0；单位是米。
- **egress ~4.5GB/起报**（地面 2.5 + 气压 2 层 2.0），免费。
- **本机耗时**：这次 ~1.9h（代理慢时），顺畅时 ~27min。**批量回补建议放服务器**（GCS 直连快）。

## 7. 待办 / 回头补充

- [ ] 决定**定位**：新增独立源 `weathernext` vs 取代 graphcast。
- [ ] 把 reader 正式收进项目（认证/代理配置、批量循环、幂等 resume）。
- [ ] 加 **zrread profile**（像 graphcast 那样）——注意 `tp` 是逐 6h 桶(总量,负值)、`ws10/ws100` 是原生风速。
- [ ] 服务器落地：服务器也要一次 ADC 登录 + 删 quota project；批量回补在服务器跑。
- [ ] 回补范围：12z-only vs 4 场；起止日期（可到 2022）。
- [ ] 待确认：各变量单位（探测时 attrs 为空，实测 t2m 是 K；tp 是米）；`geopotential` 是位势(z, m²/s²)非高度。
- [ ] 光伏侧仍用 ICON/IFS/GFS 补辐射+云量。
