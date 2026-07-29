# 基于 Kafka + Flink + ClickHouse 的电商用户行为实时数仓

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/docker-compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![Flink](https://img.shields.io/badge/flink-1.18-E6526F?logo=apache-flink&logoColor=white)](https://flink.apache.org/)
[![ClickHouse](https://img.shields.io/badge/clickhouse-24.3-FFCC01?logo=clickhouse&logoColor=black)](https://clickhouse.com/)

## 项目定位

这是一个个人学习型实时数仓项目，用于完整复现电商用户行为从产生、传输、实时加工、指标落库到看板展示的链路：

`Python -> Kafka -> Flink SQL -> Kafka -> Python Batch Loader -> ClickHouse -> Streamlit`

项目以 Docker Compose 编排 6 个本地服务，按 ODS、DWD、ADS 组织数据；DWS 仅体现为 Flink SQL 中的窗口聚合逻辑，没有单独持久化。

## 架构

```text
Python mock events
        |
        v
Kafka: ods_user_behavior                         MySQL dimensions
        |                                      product / shop / region
        v                                                |
Flink SQL source + event time + 5-second Watermark       |
        |                                                |
        +------ JDBC processing-time Temporal Join <-----+
        |
        +------ Kafka: dwd_user_behavior
        |
        +------ 1-minute / 5-minute TUMBLE aggregates
                         |
                         +-- ads_realtime_overview
                         +-- ads_product_rank
                         +-- ads_category_rank
                         +-- ads_channel_funnel
                                      |
                                      v
                         Python batch loader
                                      |
                                      v
                         ClickHouse + Streamlit
```

Docker 服务：

- ZooKeeper
- Kafka
- MySQL
- ClickHouse
- Flink JobManager
- Flink TaskManager

## 数据分层

| 层级 | 实现 | 粒度与职责 |
| --- | --- | --- |
| ODS | Kafka `ods_user_behavior` | 一条消息代表一次用户行为，保留原始 JSON |
| DWD | Kafka + ClickHouse `dwd_user_behavior` | 一行代表一条清洗并补充维度后的用户行为 |
| DWS | Flink SQL 窗口聚合逻辑 | 1 分钟或 5 分钟的主题聚合，不单独持久化 |
| ADS | ClickHouse `ads_*` | 面向看板的窗口指标 |

详细粒度、维表关系和指标口径见 [数据模型说明](realtime_dw_project/docs/data_model.md)。

## 核心实现

### DWD 清洗与维表关联

- 过滤空 `event_id`、空 `user_id` 和非法 `event_type`
- 使用事件时间和 5 秒 Watermark 处理有限乱序
- 有限批次生成器按事件时间递增发送，并注入默认 3 秒抖动，用于验证 Watermark
- 使用 JDBC processing-time Temporal Join 关联商品、店铺和地区维表
- 保留维表未命中的明细，并在质量报告中统计命中率

### 实时指标

| 输出 | 窗口 | 口径 |
| --- | --- | --- |
| 实时概览 | 1 分钟 | PV、UV、加购/下单/支付用户数、支付金额 |
| 商品支付聚合 | 5 分钟 | 每商品支付次数与金额，看板取 Top 10 |
| 品类支付聚合 | 5 分钟 | 每品类支付次数、用户数与金额 |
| 渠道阶段人数 | 1 分钟 | 各渠道 view/cart/order/pay 去重用户数 |

`pv` 只统计 `event_type='view'`；`uv` 是发生浏览行为的去重用户数。

渠道表用于比较各阶段人数。由于模拟事件没有 `order_id`、`session_id`，也不保证同一用户严格按 view -> cart -> order -> pay 发生，因此它不是严格的用户路径漏斗。

### ClickHouse 装载与看板

- Python 消费 5 个 Flink 输出 Topic，按表批量写入 ClickHouse
- 批次成功后同步提交 Kafka offset，避免“已提交但未落库”
- Streamlit 只读取最新窗口的商品、品类和渠道结果
- ReplacingMergeTree 查询使用 `FINAL` 展示合并后的最新窗口值

### 数据质量

`quality_report.py` 验证：

- DWD 核心表非空
- 空事件 ID、非法事件类型
- 重复事件 ID
- 商品、店铺、地区维表命中率
- 4 类 ADS 结果非空
- `UV <= PV`

ODS 位于 Kafka，因此报告不会再用 DWD 行数估算 ODS 行数或虚构“清洗率”。

## 快速启动

### 前提

- Docker Desktop
- Python 3
- Bash
- 建议至少 8 GB 内存

### 一键演示

```bash
bash start_demo.sh
```

> `run_demo.sh` 会执行 `docker compose down -v`，重建本项目容器和数据卷，仅适合本地演示环境。

### 分步运行

```bash
cd realtime_dw_project
docker compose up -d
bash scripts/run_demo.sh
python3 -m streamlit run dashboard/app.py
```

- Flink Web UI: http://localhost:8081
- Streamlit: http://localhost:8501

停止环境：

```bash
bash stop_demo.sh
```

## 项目规模

默认演示脚本生成 100,000 条模拟事件。当前静态结构为：

| 项目 | 数量 |
| --- | ---: |
| Docker 服务 | 6 |
| Kafka Topic | 6 |
| Flink Job | 5（1 个 DWD + 4 个 ADS） |
| MySQL 维表 | 3 |
| ClickHouse 表 | 6（1 个 DWD + 4 个 ADS + 1 个告警） |

运行后的实际行数受脏数据、窗口、消费组和执行次数影响，以最新质量报告为准，不在文档中固定宣传累计值。

## 已知边界

- 单机 Docker Compose、单 Kafka 分区和单副本，不具备生产高可用
- 未配置 Flink Checkpoint、状态后端和端到端 Exactly Once
- MySQL 维表为静态初始化数据，未接入 CDC
- JDBC Lookup 未显式配置缓存
- 商品表保存的是窗口聚合结果，Top 10 在看板侧选择
- 渠道表是阶段人数对比，不是严格路径漏斗
- ReplacingMergeTree 后台合并是异步的，查询端使用 `FINAL` 获得稳定展示

## 项目结构

```text
.
├── README.md
├── REPORT.md
├── start_demo.sh
├── stop_demo.sh
└── realtime_dw_project
    ├── docker-compose.yml
    ├── dashboard/app.py
    ├── flink-sql
    │   ├── 01_create_source_tables.sql
    │   ├── 02_create_dwd_tables.sql
    │   ├── 03_create_dws_ads_tables.sql
    │   └── 04_queries.sql
    ├── scripts
    │   ├── generate_mock_events.py
    │   ├── load_kafka_to_clickhouse.py
    │   ├── generate_alerts.py
    │   ├── quality_report.py
    │   └── verify_result.sh
    └── docs
        ├── architecture.md
        ├── data_model.md
        ├── interview_qa.md
        ├── quality_report.md
        └── resume.md
```

## 安全说明

仓库内密码只用于本地 Docker Demo，不可直接用于生产环境。

## License

MIT
