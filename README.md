# 基于 Kafka + Flink + ClickHouse 的电商用户行为实时数仓

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/docker-compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![Flink](https://img.shields.io/badge/flink-1.18-E6526F?logo=apache-flink&logoColor=white)](https://flink.apache.org/)
[![ClickHouse](https://img.shields.io/badge/clickhouse-24.3-FFCC01?logo=clickhouse&logoColor=black)](https://clickhouse.com/)

## 项目简介

基于 Docker Compose 搭建的电商用户行为实时数仓项目。模拟用户浏览、加购、下单、支付行为，构建 **Kafka → Flink SQL → ClickHouse** 的完整实时分析链路，按 ODS → DWD → DWS → ADS 四层数仓架构组织，并通过 Streamlit 构建运营监控看板。

## 技术栈

| 技术 | 版本 | 作用 |
|------|------|------|
| **Kafka** | Confluent 7.6.1 | 实时消息队列，承接 ODS 日志和 Flink 输出 |
| **Flink SQL** | 1.18 | 流式计算引擎，实时清洗 + 维表关联 + 窗口聚合 |
| **MySQL** | 8.0 | 商品/店铺/地区维表存储 |
| **ClickHouse** | 24.3 | DWD 明细和 ADS 指标存储与查询 |
| **Docker Compose** | - | 一键编排全部服务 |
| **Python** | 3.x | 模拟数据生成、Kafka→CH 装载、告警、看板 |
| **Streamlit** | - | 运营监控 Dashboard |

## 项目架构

```
Python Mock Events (generate_mock_events.py)
        │
        ▼
Kafka ODS Topic (ods_user_behavior)
        │
        ▼
Flink SQL ── Temporal Table Join ── MySQL 维表 (dim_product / dim_shop / dim_region)
        │
        ├──► Kafka DWD Topic (dwd_user_behavior)
        │
        └──► Kafka ADS Topics (ads_realtime_overview / ads_product_rank / ads_category_rank / ads_channel_funnel)
        │
        ▼
Python Consumer (load_kafka_to_clickhouse.py)
        │
        ▼
ClickHouse (dwd_user_behavior + ads_* 表)
        │
        ▼
Streamlit Dashboard (dashboard/app.py)
```

## 数仓分层

| 层级 | 存储 | 说明 |
|------|------|------|
| **ODS** | Kafka `ods_user_behavior` | 原始行为日志，保留数据原貌 |
| **DWD** | Kafka `dwd_user_behavior` + ClickHouse `dwd_user_behavior` | 清洗脏数据 + Temporal Table Join 关联商品/店铺/地区维表 |
| **DWS** | Flink SQL 窗口聚合逻辑 | 1min / 5min TUMBLE 窗口轻度汇总 |
| **ADS** | ClickHouse `ads_*` 表 | 面向查询的实时指标表 |

## 核心指标

### 实时概览（1 分钟窗口）
- 实时 PV / UV
- 加购人数 / 下单人数 / 支付人数
- 支付金额

### 商品 & 品类排行（5 分钟窗口）
- 商品支付次数 & 金额排行
- 品类支付次数 & 用户数 & 金额排行

### 渠道转化漏斗（1 分钟窗口）
- 5 个渠道（app / h5 / wechat / search / ad）的四级行为漏斗
- view → cart → order → pay 四级转化率

### 实时异常告警
- 支付金额归零 / 骤降（< 均值 50%）
- 转化率异常（view → pay < 1%）

## 项目成果

| 指标 | 数值 |
|------|------|
| ODS 行为日志 | 250,000+ |
| DWD 明细数据 | 250,000+ |
| 商品维表 | 100 条（10 品类 × 10 商品） |
| 店铺维表 | 20 条 |
| 地区维表 | 20 条 |
| Kafka Topic | 7 个 |
| Flink Job | 5 个（DWD + 4 ADS） |
| ClickHouse 表 | 6 张 |
| 质量校验 | 6 PASS 0 FAIL |
| Streamlit Dashboard | 可访问 |

## 快速启动

### 前提条件
- Docker Desktop
- Python 3
- 8GB+ 内存，10GB+ 磁盘

### 方式一：一键演示

```bash
# 从项目根目录执行
bash start_demo.sh
```

正常结束时输出：
```
verify passed: dwd_rows=220, overview_rows=1, rank_rows=5
```

### 方式二：分步启动

```bash
cd realtime_dw_project

# 1. 启动基础环境
docker compose up -d

# 2. 一键运行演示（含维表初始化、Flink 任务、数据生成、结果校验）
bash scripts/run_demo.sh

# 3. 启动 Dashboard
python3 -m streamlit run dashboard/app.py
```

### 停止环境

```bash
bash stop_demo.sh
# 或
cd realtime_dw_project && docker compose down
```

## Dashboard

访问地址：**http://localhost:8501**

Dashboard 包含：
- KPI 卡片（PV / UV / 加购 / 下单 / 支付 / 金额）
- 支付金额趋势折线图
- Top 10 商品排行柱状图
- 品类销售饼图
- 渠道转化漏斗图
- 实时异常告警表格

> Dashboard 截图可在运行后自行补充。

Flink Web UI：**http://localhost:8081**

## 项目亮点

1. **Flink SQL 实现实时清洗和窗口聚合** — 所有实时计算逻辑用纯 SQL 表达，降低理解门槛
2. **Temporal Table Join 关联 MySQL 维表** — 实时补充商品名称、品类、店铺、地区等业务字段
3. **ClickHouse 存储实时指标** — 利用 ReplacingMergeTree 引擎支持指标更新，查询性能优异
4. **渠道转化漏斗和异常告警** — 四级转化率 + 4 种告警规则，贴近真实业务场景
5. **Docker Compose 一键部署** — 6 个服务统一编排，降低环境搭建成本

## 已知限制

- 数据为模拟数据，非真实业务日志
- 单机 Docker Compose 部署，未做高可用
- Kafka 分区和副本较少（单分区）
- 未配置 Flink Checkpoint 和状态后端
- MySQL 维表为静态初始化，未接入 CDC 实时同步

## 适用场景

- 大数据开发学习项目
- 实时数仓课程实训
- Kafka / Flink / ClickHouse 面试项目展示

## 项目结构

```
├── README.md                         # 项目说明（本文件）
├── START_HERE.md                     # 新手入门指南
├── REPORT.md                         # 完整项目报告
├── start_demo.sh                     # 一键启动脚本
├── stop_demo.sh                      # 停止脚本
└── realtime_dw_project/
    ├── docker-compose.yml            # Docker 服务编排
    ├── requirements.txt              # Python 依赖
    ├── dashboard/
    │   └── app.py                    # Streamlit 运营监控看板
    ├── scripts/
    │   ├── run_demo.sh               # 一键演示脚本
    │   ├── run_flink_sql.sh          # Flink SQL 任务提交
    │   ├── verify_result.sh          # 验收校验脚本
    │   ├── validate_pipeline.sh      # Pipeline 验证脚本
    │   ├── create_kafka_topics.sh    # Kafka Topic 初始化
    │   ├── create_mysql_dim.sql      # MySQL 维表初始化（基础版）
    │   ├── create_mysql_dim_expanded.sql  # MySQL 维表初始化（扩展版）
    │   ├── create_clickhouse_tables.sql   # ClickHouse 建表
    │   ├── generate_mock_events.py   # 模拟用户行为数据生成
    │   ├── load_kafka_to_clickhouse.py    # Kafka → ClickHouse 装载
    │   ├── generate_alerts.py        # 实时异常告警生成
    │   ├── quality_report.py         # 数据质量报告
    │   ├── download_connectors.sh    # Flink Connector 下载
    │   └── verify_clickhouse.sql     # ClickHouse 验收查询
    ├── flink-sql/
    │   ├── 01_create_source_tables.sql   # ODS 源表 + MySQL 维表定义
    │   ├── 02_create_dwd_tables.sql      # DWD 明细清洗 + 维表关联
    │   ├── 03_create_dws_ads_tables.sql  # DWS/ADS 窗口聚合指标
    │   └── 04_queries.sql                # ClickHouse 查询示例
    ├── flink-lib/                    # Flink Connector jar（Kafka / JDBC / MySQL）
    └── docs/
        ├── architecture.md           # 架构设计文档
        ├── interview_qa.md           # 面试问答准备
        ├── resume.md                 # 简历项目描述
        └── quality_report.md         # 数据质量报告
```

## 安全声明

> 本项目密码（`root` / `clickhouse`）仅用于本地 Docker Demo 环境，非生产密码。

## License

MIT
