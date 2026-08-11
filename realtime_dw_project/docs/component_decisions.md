# 组件选型与企业化设计说明

> 本项目是“企业化设计的单机可复现实验环境”，不是生产集群。选型关注可解释的职责边界、故障恢复和数据可信度，而不是简单堆叠组件。

## 1. Python 订单旅程生成器

**为什么用：** 真实业务数据不能公开，Python 便于构造可重复、可控制的数据输入，并能注入乱序和业务脏数据。

**承担职责：**

- 生成同一 `session_id` 下按时间推进的 view → cart → order → pay 事件
- 下单与支付使用同一 `order_id`
- 通过固定随机种子保证测试可重复
- Producer 开启幂等、`acks=all` 和重试
- 注入空主键、空用户和非法事件类型，验证 DLQ

**为什么不用纯随机独立事件：** 独立事件只能统计各阶段人数，不能证明用户真正走过转化路径。

## 2. Kafka

**为什么用：** Kafka 在生产者、实时计算和存储之间提供解耦、削峰、持久化与重放。Flink 失败时，源数据仍可从已确认的 offset 恢复。

**项目配置：**

- 业务 Topic 默认 3 分区，展示分区并行与 key 路由
- DWD 按 `event_id` 写 key，订单旅程由 Producer 按 `session_id` 分区
- DWD/ADS Topic 使用 `compact,delete`，兼顾最新键值和有限保留
- Flink Kafka Sink 依赖 Checkpoint 使用 Exactly-Once 事务输出

**边界：** 本地只有 1 个 Broker 和 1 个副本，不能声称高可用；生产需要至少 3 Broker、合理副本、ISR 和容量规划。

### 2.1 ZooKeeper

**为什么用：** 当前 Confluent Kafka 镜像按 ZooKeeper 模式配置，ZooKeeper 保存 Broker 元数据并参与控制器协调，使这一套固定版本的本地环境启动稳定、资料也较容易理解。

**边界：** ZooKeeper 不是业务数据存储，应用代码也不直接访问它。新建生产集群应优先评估 Kafka KRaft 模式，避免为了“组件多”继续引入 ZooKeeper；这里保留它是版本兼容选择，不是简历卖点。

## 3. Flink SQL

**为什么用：** Flink 擅长事件时间、乱序处理、有状态计算、窗口聚合和流式维表关联；SQL 让指标逻辑更容易评审和复用。

**承担职责：**

- 事件时间与 5 秒 Watermark
- 业务字段校验与脏数据分流
- MySQL JDBC Temporal Lookup Join
- 1 分钟经营指标、5 分钟商品/品类指标
- 30 分钟严格会话漏斗
- Kafka Exactly-Once Sink

**为什么使用 RocksDB 状态后端：** 去重、窗口和会话阶段需要保存状态。RocksDB 将大状态放到本地磁盘并通过 Checkpoint 持久化，容量上限高于纯 JVM 堆状态。

**为什么配置 Checkpoint 和重启策略：** Checkpoint 同时记录算子状态和 Kafka offset；TaskManager 异常后从一致状态恢复，而不是从头计算或依赖人工重提任务。

### 3.1 JobManager 与 TaskManager 为什么拆开

- JobManager 负责作业调度、Checkpoint 协调和故障恢复决策。
- TaskManager 提供 Slot 并实际执行 Source、Join、窗口与 Sink 算子。
- 拆分后可以单独注入 TaskManager 故障，证明作业状态恢复，而不是只做“容器能启动”的演示。

本地各只有一个实例，因此只能验证恢复机制，不能声称 JobManager 高可用。

## 4. MySQL

**为什么用：** 订单、明细、支付、退款和商品主数据需要事务更新、主键约束和 binlog；MySQL 适合作为业务系统来源，不承担下游高并发 OLAP。

**项目配置：**

- 行为流使用 processing-time JDBC Temporal Join，并启用 Partial Lookup Cache
- Lookup Join 使用只读维表账号，不能读取交易表、binlog 或执行写操作
- 订单链路通过专用最小权限 `flink_cdc` 用户读取 ROW binlog
- 五张表由 Flink CDC 3.2.1 捕获快照和增量，分别写入 Upsert Kafka ODS
- `dim_product_scd2` 使用版本号、有效起止时间和当前标记保留调价历史

**为什么同时保留 JDBC Lookup 与 CDC：** 前者展示低频当前维度补全，后者处理会更新的交易事实和历史维度。两者对应不同业务语义，不是重复堆组件。

**边界：** CDC 作业使用显式 Schema；上游不兼容 DDL 不会自动传播，需通过 Savepoint 和契约版本计划升级。

## 5. 业务 DLQ（死信 Topic）

**为什么用：** 企业链路不能把非法数据静默丢弃，也不能因为单条坏数据阻塞全部任务。

**承担职责：** 将空 `event_id`、空 `session_id`、空用户、非法事件类型和非法时间记录写入 `dwd_dirty_behavior`，并附带 `error_reason`。DLQ 数据同步进入 ClickHouse，便于统计、修复和回放。

**边界：** 当前捕获的是可解析 JSON 中的业务字段错误；完全无法解析的二进制或 JSON 仍需要独立原始消息接入层才能完整隔离。

## 6. ClickHouse

**为什么用：** ClickHouse 是列式 OLAP 数据库，适合实时明细抽查、时间窗口指标和聚合看板查询，压缩和扫描效率高于用 MySQL 承担分析查询。

**幂等设计：**

- Loader 为每行附带 Kafka topic、partition、offset 和 `ingest_version`
- DWD/ADS 使用 `ReplacingMergeTree(ingest_version)` 和稳定业务排序键
- Loader 写成功后才同步提交 Kafka offset
- 发生“写成功、提交 offset 失败”时允许安全重放，验收查询用 `FINAL` 读取逻辑去重结果

**边界：** 这是“逻辑幂等 + 最终一致”，不是 Kafka 与 ClickHouse 之间的分布式事务。`FINAL` 在大表上有成本，生产应结合版本列、物化视图或原生 Connector 设计。

## 7. Python Loader（常驻服务 + 批处理入口）

**为什么使用：** 它把“计算”和“Serving 层装载”拆开，便于观察 Kafka 中间结果，也能展示批量、重试、手动 offset 和幂等键设计。Compose 等 Kafka/ClickHouse 健康后启动它，并以依赖探测 healthcheck 和 `restart: unless-stopped` 运行常驻容器；同一程序保留宿主机有限批处理入口，用于演示、基准和历史回放。

**承担职责：**

- 多 Topic 消费与分表缓冲
- JSONEachRow 批量写 ClickHouse
- 指数退避重试
- 写入成功后同步提交 offset
- 写入 Kafka 元数据，支持追踪和问题定位
- 使用 `version_epoch + Kafka partition/offset` 生成确定性版本，历史重放不会用“更晚的装载时间”覆盖新业务值
- 收到 SIGTERM 时先刷新缓冲并同步提交，再退出容器
- 用 Python 标准库暴露 Prometheus 指标：消费数、成功写入行数、失败尝试、最后成功时间和批次耗时

**约束：** Upsert 业务键必须保持在同一 Kafka 分区，才能以 offset 表达该键的版本顺序；若保留 ClickHouse 卷但重置 Kafka log/offset 或改变分区路由，需要递增 Loader epoch 并进行迁移评审。数据量更大时优先评估成熟 ClickHouse Flink Connector，减少自研 Loader 的运维成本。

## 8. Prometheus、Kafka Exporter 与 Grafana

**为什么用：** 数据正确只是底线，企业还要知道链路是否健康。Prometheus 负责拉取时序指标，Kafka Exporter 暴露消费组 Lag，Grafana 负责可视化，Alertmanager 负责告警分组与通知路由。

**重点观察：**

- Flink Job/TaskManager 是否存活
- Records In/Out 与反压
- Checkpoint 成功率、时长和失败次数
- Kafka Consumer Lag
- Loader 消费/写入计数、插入失败、最后成功写入时间和批次耗时
- TaskManager 重启后的恢复时间

告警将 Loader 自身 `up` 与 Kafka 消费组 Lag 联合判断：进程不可抓取触发 `LoaderDown`；存在积压且长时间无成功写入触发 `LoaderWriteStalled`，正常空闲不会仅因“很久没写”报警。

## 9. Power BI 与 Streamlit

**Power BI 为什么用：** 更贴近企业 BI 交付，支持语义模型、DAX、切片器和标准报表文件，适合作为业务验收层。

**Streamlit 为什么保留：** 启动快、便于开发者诊断原始 DWD、告警和最新 ADS，是工程调试页，不再作为简历核心卖点。

## 10. Docker Compose

**为什么用：** 固化组件版本、端口、网络、健康检查和数据卷，使面试官或同事可以在单机复现完整链路。

**边界：** Compose 解决可复现性，不解决跨机器高可用、滚动发布和弹性扩容；生产通常使用 Kubernetes 或云托管服务。

## 11. GitHub Actions

**为什么用：** 防止修改生成器、Loader、CDC 字段或 Compose 后破坏基本契约。CI 执行 Python 编译、单元测试和 Compose 静态解析；字段契约测试还会比对 `contracts/cdc_contracts.json`、MySQL 建表 DDL 与 Flink CDC Source/ODS Sink 的字段、类型、主键和 Topic。

**边界：** 当前 CI 不启动完整重型链路；端到端验证由本地 `validate_pipeline.sh` 完成。

## 12. pytest 与数据质量 SQL

**为什么用 pytest：** 生成器的事件顺序、主键稳定性、Loader 重放版本和脏数据注入属于代码契约；CDC 契约测试还能静态阻止 MySQL/Flink/Topic 字段漂移，适合在不启动整套 Docker 的情况下快速回归。

**为什么还要数据质量 SQL：** 单元测试只能证明代码局部行为，不能证明 Kafka 重放、Flink 窗口和 ClickHouse 落库后的业务结果。验收因此检查主键唯一、维表命中、`UV <= PV`、严格漏斗单调和 DLQ 可追踪。

## 13. 为什么没有继续堆 Hadoop、Hive、Spark、Airflow、Redis

- Hadoop/Hive：当前是单机实时链路，没有离线湖仓和大规模历史批处理需求，加入只会制造伪复杂度。
- Spark：Flink 已承担实时有状态计算；没有第二套计算引擎必须解决的独立问题。
- Airflow：现阶段没有复杂的离线 DAG，启动、建表和验收由脚本编排即可。
- Redis：当前没有毫秒级在线特征或高并发 KV 查询场景，MySQL Lookup Cache 已覆盖本项目维表访问。

企业级不是组件越多越好，而是每个组件都有明确职责、失败边界、监控方式和可替换方案。
