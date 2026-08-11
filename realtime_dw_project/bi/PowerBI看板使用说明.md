# Power BI 实时经营看板使用说明

## 文件

- `电商实时数仓运营看板.pbip`：Power BI 项目入口。
- `电商实时数仓运营看板.Report`：PBIR 报表定义、主题与视觉配置。
- `电商实时数仓运营看板.SemanticModel`：TMDL 语义模型、关系与 DAX 指标。
- 原有 `电商实时数仓运营看板.pbix` 未被修改。

## 打开与刷新

1. 在 `realtime_dw_project` 目录启动数据环境：`docker compose up -d`。
2. 确认 ClickHouse 可通过 `localhost:8123` 访问，并已安装 ClickHouse ODBC Unicode 驱动。
3. 双击 `电商实时数仓运营看板.pbip`。
4. 首次在新电脑刷新时，选择 ODBC“数据库”认证；用户名和密码使用 `docker-compose.yml` 中的 ClickHouse 配置，凭据连接字符串属性留空。
5. 在 Power BI 中点击“刷新”。

模型连接字符串只保存驱动、地址、端口和数据库等非凭据属性，密码由 Power BI 凭据存储单独管理。

## 页面内容

- `实时经营总览`：最新窗口时间、PV、UV、支付用户、支付金额及环比，分钟级支付趋势、严格渠道漏斗、商品 Top 10 和品类排名。
- `订单生命周期`：日期与渠道切片器，订单/支付/退款关键指标，每日支付与退款金额趋势、渠道状态分布、净支付金额和渠道明细矩阵。
- `SCD2 版本审计`：商品 ID 与当前版本切片器，商品/版本/调价指标、商品历史版本数量和完整生效区间明细。

订单 CDC 与 SCD2 页面字段、指标和验收口径见 [订单 CDC 页面实现说明](订单CDC页面设计.md) 与 `order_cdc_queries.sql`。

## 验收证据

- PBIR/PBIP 静态校验为 0 错误；若本机无法访问微软远程 JSON Schema，校验器会保留一条 `PBIR_SCHEMA_UNREACHABLE` 环境警告。
- 三页刷新后截图位于 `../artifacts/powerbi_after/实时经营总览.png`、`../artifacts/powerbi_after/订单生命周期.png` 和 `../artifacts/powerbi_after/SCD2 版本审计.png`。
- 刷新后应在项目根目录运行 `scripts/verify_result.ps1` 或 `scripts/verify_result.sh`，以 ClickHouse `FINAL` 口径复核订单状态、退款约束和 SCD2 当前版本唯一性。
