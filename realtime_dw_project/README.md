# 电商用户行为实时数仓项目运行指南

## 1. 项目说明

本项目是一个电商用户行为实时数仓案例，模拟用户在电商平台中的浏览、加购、下单、支付行为，并通过实时计算链路统计访问、转化、交易、排行等指标。

项目采用 Docker Compose 启动基础环境，使用 Python 生成实时行为日志，Kafka 接收日志流，Flink SQL 完成实时清洗、维表关联和窗口聚合，Python 装载脚本把 Flink 输出写入 ClickHouse，MySQL 存储商品、店铺、地区等维表。

整体流程：

```text
Python 模拟日志
    -> Kafka 接收实时事件
    -> Flink SQL 读取 Kafka
    -> Flink SQL 关联 MySQL 维表
    -> Flink SQL 输出结果 Topic
    -> Python 装载脚本写入 ClickHouse
    -> SQL 查询分析
```

## 2. 项目目录

```text
realtime_dw_project/
  docker-compose.yml
  requirements.txt
  scripts/
    create_clickhouse_tables.sql
    create_mysql_dim.sql
    generate_mock_events.py
    load_kafka_to_clickhouse.py
    run_demo.sh
  flink-sql/
    01_create_source_tables.sql
    02_create_dwd_tables.sql
    03_create_dws_ads_tables.sql
    04_queries.sql
  docs/
    architecture.md
```

## 3. 文件作用

| 文件 | 作用 |
| --- | --- |
| `docker-compose.yml` | 定义 Kafka、MySQL、ClickHouse、Flink 等本地服务 |
| `requirements.txt` | Python 数据生成脚本依赖 |
| `scripts/create_mysql_dim.sql` | 创建 MySQL 维表并插入样例数据 |
| `scripts/create_clickhouse_tables.sql` | 创建 ClickHouse 明细表和指标表 |
| `scripts/generate_mock_events.py` | 模拟用户行为日志，并持续写入 Kafka |
| `scripts/load_kafka_to_clickhouse.py` | 消费 Flink 输出 Topic，并写入 ClickHouse |
| `scripts/run_demo.sh` | 一键运行本地演示并输出验收结果 |
| `flink-sql/01_create_source_tables.sql` | 创建 Flink Kafka 源表和 MySQL 维表 |
| `flink-sql/02_create_dwd_tables.sql` | 清洗 ODS 数据，形成 DWD 明细层 |
| `flink-sql/03_create_dws_ads_tables.sql` | 进行窗口聚合，生成 ADS 指标结果 |
| `flink-sql/04_queries.sql` | 查询 ClickHouse 中的实时指标结果 |
| `docs/architecture.md` | 项目架构、分层、数据流说明 |

## 4. 技术栈说明

| 技术 | 版本参考 | 本项目作用 |
| --- | --- | --- |
| Docker Compose | 本机安装版本 | 编排本地服务 |
| Kafka | Confluent 7.6.1 | 接收实时用户行为日志 |
| Flink | 1.18 | 执行实时 SQL 任务 |
| MySQL | 8.0 | 存储商品、店铺、地区维表 |
| ClickHouse | 24.3 | 存储明细和统计结果 |
| Python | 3.x | 生成模拟日志 |

## 5. 新手需要先理解的概念

### 5.1 什么是实时数仓

实时数仓就是数据一产生就尽快进入处理链路，然后持续更新统计结果。例如用户刚刚完成支付，系统就能在很短时间内更新支付金额、成交人数、热销商品等指标。

### 5.2 为什么要用 Kafka

用户行为日志是持续产生的，不适合先保存成一个固定文件再处理。Kafka 可以把源源不断的日志先接住，再让 Flink 持续消费。

### 5.3 为什么要用 Flink SQL

Flink 适合处理实时流数据。Flink SQL 可以用接近普通 SQL 的方式编写实时任务，降低代码复杂度。

### 5.4 为什么要用 MySQL 维表

用户行为日志中一般只有 `product_id`、`shop_id` 这类 ID。要得到商品名称、品类、店铺名称，就需要关联维表。本项目使用 MySQL 模拟业务系统中的维表。

### 5.5 为什么要用 ClickHouse

ClickHouse 适合分析查询。本项目中 Flink 先把实时结果写入 Kafka 结果 Topic，再由 Python 装载脚本写入 ClickHouse，最后用 SQL 查看 PV、UV、支付金额、热销商品等指标。

## 6. 数据分层

| 层级 | 作用 | 本项目内容 |
| --- | --- | --- |
| ODS | 原始数据层，保留实时日志原貌 | Kafka Topic `ods_user_behavior` |
| DWD | 明细数据层，清洗字段、补充维度 | Kafka Topic `dwd_user_behavior`、ClickHouse 表 `dwd_user_behavior` |
| DWS | 汇总数据层，按主题和窗口聚合 | Flink SQL 窗口统计逻辑 |
| ADS | 应用数据层，面向查询和展示 | `ads_realtime_overview`、`ads_product_rank` |

## 7. 运行前准备

本地需要提前安装：

- Docker Desktop
- Python 3
- 可以执行命令的终端

建议配置：

- 内存：8GB 以上
- 磁盘：预留 10GB 以上空间
- 网络：第一次拉取 Docker 镜像需要联网

注意：Flink 连接 Kafka、MySQL 需要对应 Connector jar。项目已经提供 `scripts/download_connectors.sh`，用于下载 Kafka、JDBC、MySQL 连接器。下载后 jar 会挂载到 Flink 容器的 `lib` 目录。

## 8. 快速运行

如果只是想先把项目跑起来，建议使用一键演示脚本。

### 第 1 步：进入项目目录

```bash
cd realtime_dw_project
```

### 第 2 步：执行演示脚本

```bash
bash scripts/run_demo.sh
```

脚本会自动完成下面事情：

1. 检查 Python 依赖。
2. 检查 Flink 连接器 jar。
3. 启动 Kafka、MySQL、ClickHouse、Flink 容器。
4. 初始化 MySQL 维表。
5. 创建 Kafka Topic。
6. 初始化 ClickHouse 表。
7. 提交 Flink SQL 实时任务。
8. 生成测试行为数据。
9. 把 Flink 输出结果写入 ClickHouse。
10. 查询并校验 DWD 明细、实时汇总、商品排行结果。

正常结束时会看到类似输出：

```text
verify passed: dwd_rows=220, overview_rows=1, rank_rows=5
```

注意：`run_demo.sh` 会执行 `docker compose down -v`，用于清理旧的演示数据并重建环境。如果本机同名容器中有需要保留的数据，请不要直接运行该脚本。

Flink Web 页面：

```text
http://localhost:8081
```

## 9. 分步运行说明

下面的步骤适合学习每个组件分别做了什么。如果只想快速验证项目，可以直接使用第 8 节的一键脚本。

### 第 1 步：进入项目目录

```bash
cd realtime_dw_project
```

### 第 2 步：启动基础环境

```bash
docker compose up -d
```

启动后可以查看容器：

```bash
docker ps
```

正常情况下应该能看到：

| 容器 | 作用 |
| --- | --- |
| `rtdw_zookeeper` | Kafka 依赖服务 |
| `rtdw_kafka` | Kafka 消息队列 |
| `rtdw_mysql` | MySQL 维表库 |
| `rtdw_clickhouse` | ClickHouse 分析库 |
| `rtdw_flink_jobmanager` | Flink 管理节点 |
| `rtdw_flink_taskmanager` | Flink 计算节点 |

Flink Web 页面：

```text
http://localhost:8081
```

### 第 3 步：初始化 MySQL 维表

```bash
docker exec -i rtdw_mysql mysql -uroot -proot ecommerce < scripts/create_mysql_dim.sql
```

检查维表：

```bash
docker exec -it rtdw_mysql mysql -uroot -proot ecommerce
```

进入 MySQL 后执行：

```sql
SHOW TABLES;
SELECT * FROM dim_product LIMIT 5;
SELECT * FROM dim_shop LIMIT 5;
SELECT * FROM dim_region LIMIT 5;
```

### 第 4 步：创建 Kafka Topic

```bash
bash scripts/create_kafka_topics.sh
```

这一步会创建：

```text
ods_user_behavior
dwd_user_behavior
ads_realtime_overview
ads_product_rank
```

Flink 任务启动前必须先创建源 Topic，否则 Flink 查询 Kafka 元数据时可能直接失败。

### 第 5 步：初始化 ClickHouse 表

```bash
docker exec -i rtdw_clickhouse clickhouse-client --password clickhouse --multiquery < scripts/create_clickhouse_tables.sql
```

检查结果表：

```bash
docker exec -it rtdw_clickhouse clickhouse-client --password clickhouse
```

进入 ClickHouse 后执行：

```sql
SHOW TABLES;
DESCRIBE TABLE dwd_user_behavior;
DESCRIBE TABLE ads_realtime_overview;
DESCRIBE TABLE ads_product_rank;
```

### 第 6 步：安装 Python 依赖

```bash
python3 -m pip install -r requirements.txt
```

### 第 7 步：启动 Flink SQL 任务

```bash
bash scripts/run_flink_sql.sh
```

正常情况下，Flink 会提交 3 个实时任务：

- DWD 明细任务
- 实时概览指标任务
- 商品排行指标任务

可以查看任务状态：

```bash
docker exec rtdw_flink_jobmanager /opt/flink/bin/flink list
```

### 第 8 步：启动模拟数据

```bash
python3 scripts/generate_mock_events.py
```

正常现象：终端会持续打印 JSON 数据，例如：

```text
{'event_id': '...', 'user_id': 123, 'product_id': 1001, 'shop_id': 1, 'event_type': 'view', ...}
```

这表示模拟日志已经开始写入 Kafka。

### 第 9 步：把 Flink 输出写入 ClickHouse

Flink 会把处理结果先写到 Kafka 结果 Topic，然后由 Python 脚本写入 ClickHouse：

```bash
python3 scripts/load_kafka_to_clickhouse.py
```

验证时可以限制消费条数：

```bash
python3 scripts/load_kafka_to_clickhouse.py --max-messages 200 --idle-timeout 20
```

### 第 10 步：查询结果

在 ClickHouse 中执行：

```sql
SELECT *
FROM ads_realtime_overview
ORDER BY window_start DESC
LIMIT 20;
```

查询商品排行：

```sql
SELECT
  window_start,
  window_end,
  product_name,
  category_name,
  pay_count,
  pay_amount
FROM ads_product_rank
ORDER BY window_start DESC, pay_amount DESC
LIMIT 20;
```

## 10. 核心 SQL 说明

### 9.1 Kafka 源表

`01_create_source_tables.sql` 中的 `ods_user_behavior` 对应 Kafka Topic。Flink 会从这个 Topic 持续读取 JSON 数据。

关键字段：

| 字段 | 含义 |
| --- | --- |
| `event_id` | 事件唯一 ID |
| `user_id` | 用户 ID |
| `product_id` | 商品 ID |
| `shop_id` | 店铺 ID |
| `event_type` | 行为类型 |
| `channel` | 访问渠道 |
| `amount` | 金额 |
| `event_time` | 事件时间 |

### 9.2 DWD 明细层

`02_create_dwd_tables.sql` 会做三件事：

1. 过滤异常数据。
2. 把 `event_time` 转成 Flink 可识别的时间字段。
3. 关联 MySQL 维表，补充商品名称、品类名称、店铺名称。

清洗后的明细会先写入 Kafka Topic `dwd_user_behavior`，再由 `scripts/load_kafka_to_clickhouse.py` 写入 ClickHouse 表 `dwd_user_behavior`。

### 9.3 ADS 指标层

`03_create_dws_ads_tables.sql` 包含两个主要指标：

| 指标表 | 说明 |
| --- | --- |
| `ads_realtime_overview` | 统计 1 分钟窗口内的 PV、UV、加购人数、下单人数、支付人数、支付金额 |
| `ads_product_rank` | 统计 5 分钟窗口内的商品支付次数和支付金额 |

## 11. 验收标准

完成运行后，可以按下面标准检查项目是否正常：

| 检查项 | 正常结果 |
| --- | --- |
| Docker 容器 | Kafka、MySQL、ClickHouse、Flink 容器都在运行 |
| MySQL 维表 | `dim_product`、`dim_shop`、`dim_region` 能查询到数据 |
| Python 脚本 | 持续打印模拟事件 |
| Flink 任务 | Flink Web 页面能看到运行中的 SQL Job |
| DWD 明细 | ClickHouse `dwd_user_behavior` 有数据 |
| ADS 指标 | ClickHouse `ads_realtime_overview`、`ads_product_rank` 有统计结果 |

## 12. 常见问题

### 11.1 Docker 镜像下载慢

第一次运行需要拉取 Kafka、MySQL、ClickHouse、Flink 镜像。如果下载慢，可以更换网络，或者配置 Docker 镜像加速。

### 11.2 端口被占用

本项目默认使用：

| 端口 | 服务 |
| --- | --- |
| `9092` | Kafka |
| `3306` | MySQL |
| `8123` | ClickHouse HTTP |
| `9000` | ClickHouse Native |
| `8081` | Flink Web |

如果本机已有相同服务，需要先关闭原服务，或者修改 `docker-compose.yml` 中的端口映射。

### 11.3 Python 连接不上 Kafka

先确认 Kafka 容器是否启动：

```bash
docker ps
```

再确认脚本中的地址是：

```text
localhost:9092
```

如果是在容器内部运行 Python，需要改成：

```text
kafka:29092
```

### 11.4 Flink SQL 提示找不到 connector

这是 Flink 运行环境问题。需要补充 Kafka、JDBC、MySQL 对应 Connector jar。可以先执行 `scripts/download_connectors.sh`，再重启 Flink。

### 11.5 ClickHouse 查询不到结果

按顺序检查：

1. Python 脚本是否正在输出数据。
2. Kafka Topic 是否收到数据。
3. Flink SQL 任务是否在运行。
4. ClickHouse 表是否已经创建。
5. `scripts/load_kafka_to_clickhouse.py` 是否正在运行。
6. ClickHouse 连接账号和密码是否正确。

## 13. 当前项目数据规模（优化后）

| 指标 | 数值 |
|------|------|
| 商品维表 (dim_product) | 100 条（10 品类 × 10 商品） |
| 店铺维表 (dim_shop) | 20 条 |
| 地区维表 (dim_region) | 20 条 |
| ODS 日志量 | 250,000+ |
| DWD 清洗后明细 | 580,000+ |
| Flink Job 数量 | 5 个（DWD + 4 个 ADS） |
| ClickHouse 表数量 | 6 张 |

## 14. 新增 ADS 指标说明

| 指标表 | 窗口 | 说明 |
|--------|------|------|
| `ads_realtime_overview` | 1 分钟 | PV、UV、加购/下单/支付人数、支付金额 |
| `ads_product_rank` | 5 分钟 | 商品支付次数和金额排行 |
| `ads_category_rank` | 5 分钟 | 品类支付次数、用户数、金额排行 |
| `ads_channel_funnel` | 1 分钟 | 5 个渠道的四级行为漏斗（view→cart→order→pay） |
| `ads_realtime_alert` | 实时 | 支付归零、支付骤降、转化率异常等告警 |

## 15. 验收结果（最新）

```
verify summary: 6 PASS, 0 FAIL
dwd_rows=255862, overview_rows=93, rank_rows=254
category_rows=60, funnel_rows=398, alert_rows=3
All core checks PASSED!
```

## 16. Streamlit Dashboard

```bash
cd realtime_dw_project
python3 -m streamlit run dashboard/app.py
```

访问 http://localhost:8501，包含：
- KPI 卡片（PV/UV/加购/下单/支付/金额）
- 支付金额趋势折线图
- Top 10 商品排行柱状图
- 品类销售饼图
- 渠道转化漏斗图
- 实时异常告警表格

## 17. 停止项目

停止所有容器：

```bash
docker compose down
```

如果需要清理容器数据卷，可以根据实际情况手动删除 Docker volume。学习阶段一般不建议直接清理，避免误删已生成的数据。

## 14. 学习路线

新手建议按下面路线学习：

1. 先运行 `generate_mock_events.py`，看懂模拟日志格式。
2. 再看 `create_mysql_dim.sql`，理解维表里保存了什么。
3. 看 `01_create_source_tables.sql`，理解 Flink 如何读取 Kafka 和 MySQL。
4. 看 `02_create_dwd_tables.sql`，理解实时明细层如何清洗和补维。
5. 看 `03_create_dws_ads_tables.sql`，理解窗口聚合如何生成指标。
6. 看 `04_queries.sql`，理解如何查询最终结果。

## 15. 可扩展方向

- 增加用户维表，统计用户画像指标。
- 增加地区维度，统计不同省市的访问和成交。
- 接入 DataEase、Superset 或 Grafana 展示实时看板。
- 使用 Debezium 或 Canal 实现 MySQL CDC 实时同步。
- 使用 Redis 做维表缓存，提高维表关联性能。
- 使用 Doris、Paimon、Hudi 等组件扩展实时湖仓能力。
