# 新手先看

这是一个“电商实时数仓”学习项目。你可以把它理解成：模拟用户在电商平台上的浏览、加购、下单、支付行为，然后用大数据组件实时统计访问量、成交金额、热销商品等指标。

## 先搞清楚这个项目在做什么

项目流程可以简单理解为：

```text
Python 模拟用户行为
        |
        v
Kafka 接收实时日志
        |
        v
Flink SQL 实时清洗和统计
        |
        v
ClickHouse 保存统计结果
        |
        v
SQL 查询实时指标
```

每个组件的作用：

| 组件 | 新手理解 |
| --- | --- |
| Python | 不断造出“用户行为数据”，比如用户浏览商品、下单、支付 |
| Kafka | 像一个实时消息队列，先把日志接住 |
| Flink SQL | 实时处理日志，清洗字段、关联维表、计算指标 |
| MySQL | 放商品、店铺、地区这些基础信息 |
| ClickHouse | 放最终查询结果，比如实时 UV、支付金额、热销商品 |
| Docker Compose | 一次性启动上面这些环境 |

## 你先看哪些文件

建议顺序：

1. 先看 `realtime_dw_project/README.md`
2. 再看 `realtime_dw_project/docs/architecture.md`
3. 想写报告时看 `REPORT.md`
4. 想写课程报告或项目说明时看 `REPORT.md`

## 运行前需要准备什么

需要电脑已经安装：

- Docker Desktop
- Python 3
- 终端工具

如果只是学习项目结构、写报告、整理简历，不一定要完整跑起来。完整运行需要下载 Docker 镜像，也需要 Flink 连接器版本匹配，第一次配置可能会花时间。

## 想先跑通怎么办

进入 `realtime_dw_project` 目录后执行：

```bash
bash scripts/run_demo.sh
```

这个脚本会自动启动环境、建表、提交 Flink 任务、生成测试数据，并查询 ClickHouse 结果。正常结束时会看到：

```text
verify passed: dwd_rows=220, overview_rows=1, rank_rows=5
```

注意：这个脚本会重建本项目的 Docker 容器和数据卷，用来保证演示环境干净。

## 最简单的学习方式

如果你是新手，建议先不要急着跑全流程，按下面顺序理解：

1. 看 `scripts/generate_mock_events.py`，理解模拟数据长什么样。
2. 看 `flink-sql/01_create_source_tables.sql`，理解 Flink 怎么读 Kafka。
3. 看 `flink-sql/02_create_dwd_tables.sql`，理解明细层怎么清洗和关联维表。
4. 看 `flink-sql/03_create_dws_ads_tables.sql`，理解实时指标怎么统计。
5. 看 `flink-sql/04_queries.sql`，理解最后怎么查询结果。

## 这个项目可以怎么介绍

可以这样说：

> 这个项目是一个电商用户行为实时数仓，使用 Kafka 接收实时行为日志，Flink SQL 做实时清洗、维表关联和窗口聚合，MySQL 存储商品与店铺维表，ClickHouse 存储实时指标结果。项目按照 ODS、DWD、DWS、ADS 分层设计，指标包括实时 PV、UV、支付金额、转化漏斗和热销商品排行。

## 新手常见疑问

### 1. ODS、DWD、DWS、ADS 是什么？

- ODS：原始数据层，尽量保留原始日志。
- DWD：明细数据层，把脏数据过滤掉，把字段整理清楚。
- DWS：汇总数据层，按商品、渠道、窗口等维度做统计。
- ADS：应用数据层，给报表、看板、查询直接使用。

### 2. 为什么要用 Kafka？

因为用户行为是实时产生的。Kafka 用来接收源源不断的日志，Flink 再从 Kafka 中持续消费数据。

### 3. 为什么要用 Flink？

Flink 适合实时计算。比如最近 1 分钟有多少访问、最近 5 分钟哪些商品卖得好，这类指标需要边来数据边计算。

### 4. 为什么还要 MySQL？

用户行为日志里通常只有 `product_id`、`shop_id`。要知道商品名称、品类、店铺名称，就需要去维表里补充信息。这里用 MySQL 模拟业务维表。

### 5. 为什么用 ClickHouse？

ClickHouse 查询聚合数据比较快，适合保存实时指标结果，然后用 SQL 快速查询。

## 学到什么算基本掌握

如果你能回答下面几个问题，说明已经基本理解这个项目：

1. Kafka 在项目里负责什么？
2. Flink SQL 从哪里读取数据，又把结果写到哪里？
3. MySQL 维表为什么要和用户行为日志关联？
4. ODS、DWD、DWS、ADS 每一层分别解决什么问题？
5. ClickHouse 中的结果表可以查询哪些指标？
