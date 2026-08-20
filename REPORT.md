# 电商交易实时数仓项目报告

## 1. 项目目标

本项目在单机 Docker Desktop 中复现电商行为日志与交易数据库两类实时数据源，完成 Kafka 接入、Flink CDC/SQL 计算、ClickHouse 查询服务、Power BI 展示、质量校验、可观测性和故障恢复。目标是形成能运行、能验证、能解释边界的个人项目，不把单机 Demo 描述成生产高可用集群。

## 2. 技术架构

| 组件 | 版本/实现 | 职责 |
| --- | --- | --- |
| Kafka | Confluent 7.6.1 | ODS、DWD、ADS、DLQ 与可重放缓冲 |
| Flink SQL | 1.18.1 | 事件时间、清洗、Join、窗口、Changelog 聚合 |
| Flink CDC | MySQL CDC 3.2.1 | MySQL 快照与 ROW binlog 增量捕获 |
| Spark SQL | 3.5.9（按需 local 模式） | 历史日期回补、Parquet 分区覆盖与批流对账 |
| MySQL | 8.0 | 交易表、当前维表、SCD2 和 Schema 契约 |
| ClickHouse | 24.3 | 行为/订单 DWD、ADS、历史维度与质量查询 |
| Python | 3.x | 数据生成、常驻/批量装载、验收、压测与报告 |
| Power BI | PBIP/PBIR | 可版本化的企业业务报表 |
| Streamlit | 开发诊断页 | 明细与告警调试，不作为核心交付 |
| Prometheus/Grafana/Alertmanager | 固定镜像 | 指标采集、监控看板与告警路由 |
| Docker Compose | 12 个长期容器 + 1 个初始化服务 + 1 个按需 Spark 任务 | 本地可复现编排 |

## 3. 数据链路

### 3.1 行为事件链路

`Python Journey → Kafka ODS → Flink 清洗/Lookup/窗口 → Kafka DWD/ADS/DLQ → Loader → ClickHouse → Power BI`

生成器产生带 `session_id`、`order_id` 和事件顺序的 view、cart、order、pay 旅程。Flink 使用 5 秒 Watermark、业务 DLQ、JDBC Temporal Lookup、1/5 分钟窗口和 30 分钟严格漏斗。

### 3.2 订单 CDC 链路

`MySQL 交易表/SCD2 → Flink CDC → Upsert Kafka ODS → Flink 订单 DWD → 订单 ADS → Loader → ClickHouse`

CDC 捕获订单、明细、支付、退款和商品历史版本。生成器先提交 CREATED，再分别提交支付/取消和退款；订单 DWD 的粒度是一条订单明细，状态更新通过 Changelog 覆盖同一业务键；ADS 先折叠到订单级，再按渠道和日期聚合，避免多明细订单重复计数。五个 CDC Source 不提供跨 Topic 原子可见性，下游依靠 Changelog 最终收敛。

## 4. 历史维度

商品调价存储旧/新两个版本，使用 `[effective_from,effective_to)` 左闭右开区间。订单以 `order_create_time` 命中历史版本，质量检查要求：

- 每个商品只有一个当前版本。
- 任意版本有效区间不重叠。
- 订单 `unit_price` 与事件时间命中的 `catalog_price` 一致。

连续压测曾发现生成器把订单时间回填到过去、却使用运行时当前价，导致重复调价后发生历史价错配。已改为真实插入时间，并隔开调价毫秒边界；干净重建和重复调价后错配均为 0。

## 5. 可靠性

- 10 秒 Checkpoint、Embedded RocksDB、持久化状态卷。
- 固定延迟重启和 Kafka Exactly-Once Sink。
- Compose 常驻 Loader 批量写入成功后才提交 offset，异常退出自动重启。
- `ingest_version` 由 Loader epoch + Kafka partition/offset 确定性生成，旧 offset 重放不会覆盖新业务值。
- Loader 对 JSON、ClickHouse 字段类型和必填项逐条预检；坏消息获得 DLQ broker ACK 后才推进源 offset，修复回放前再次按目标 Topic 校验。
- Loader DLQ 使用纯 compaction 保存每条异常最新的 `pending/replayed` 审计状态；端到端 Freshness Exporter 使用逻辑最新版本业务时间识别“Lag 为 0 但数据已停更”。
- Loader 暴露消费、写入、失败、最后成功时间和批次耗时指标，Prometheus/Alertmanager 检测进程下线及“有 Lag 但长时间无写入”。
- ClickHouse 使用稳定业务键、版本列、软删除和 `ReplacingMergeTree` 接受安全重放。
- Flink SQL Client 同时检查退出码和 `[ERROR]` 文本，避免 SQL 失败仍返回进程码 0。

一致性边界：Kafka → Flink → Kafka 使用 Exactly-Once；Kafka → Loader → ClickHouse 是最终一致和逻辑幂等，不是分布式事务。

## 6. Schema Evolution

`contracts/cdc_contracts.json` 固化五张 CDC 表的字段类型、可空性、主键、ODS Topic、tombstone 和跨表最终一致性语义，pytest 自动与 MySQL/Flink DDL 比对。上游新增 nullable `delivery_type` 时，旧显式 Schema 作业继续运行，变更登记到 `cdc_schema_contract`。演练脚本可重复执行，并验证 9 条作业保持健康。删除、重命名和类型收窄需要 Savepoint、先加后用或双写迁移，禁止未评审自动透传。

## 7. 数据质量

最终质量报告执行 24 项规则：

- 行为与订单键完整、唯一。
- 行为类型、订单状态合法。
- 商品、店铺、地区命中率。
- `UV <= PV`、严格漏斗阶段单调。
- 支付和退款记录与订单状态一致。
- SCD2 当前版本和区间合法，历史价格命中。
- 订单 ADS 订单数与金额关系合法。

最新可复核报告为 24/24 PASS；累计行数会随演练次数变化，以 `realtime_dw_project/artifacts/enterprise_quality_report.md` 为准。

## 8. 性能与恢复证据

### 压测

- 输入：10,000 个行为旅程、22,663 个事件、1,000 个可变订单。
- 增量输出：22,497 条行为 DWD、6,443 条订单 DWD Changelog。
- 生产开始到 DWD offset 连续稳定：55.01 秒。
- 单机综合 DWD 输出：526.09 条/秒。
- 压测后 21/21 快速验收通过，历史价格错误 0。

### 故障恢复

- 在生产期间 kill TaskManager。
- 9 条作业全部从 Checkpoint 恢复：26.36 秒。
- 新消费组重放 85,647 条结果。
- 行为重复键 0、订单明细重复键 0、历史价格错误 0。
- 恢复后 21/21 快速验收通过。

数字只适用于本机单节点 Docker 环境，完整记录见 `realtime_dw_project/artifacts/` 和 `docs/performance_report.md`。

## 9. 当前规模

- 15 个业务 Kafka Topic，另有 1 个纯 compaction Loader DLQ Topic；每个默认 3 分区。
- 9 条 Flink Job。
- 5 张 CDC 业务/历史表，外加静态维表与 Schema 契约表。
- 10 张 ClickHouse 查询表。
- 24 项质量规则、21 项快速验收。
- 12 个长期运行容器和 1 个一次性初始化服务。
- 1 个 `batch` profile 下的按需 Spark SQL 任务；实测 1,540 笔订单连续回补两次，批流差异为 0 且 Parquet 行数不膨胀。

## 10. 生产化差距

- 单 Broker、单副本、单 JobManager、单 TaskManager。
- Checkpoint 在本地卷，没有远端对象存储和多可用区容灾。
- 没有 Schema Registry、元数据、血缘、权限和敏感字段治理。
- Alertmanager 未接真实值班通知渠道。
- Python Loader 适合展示幂等边界，大规模场景应评估成熟 Connector。
- 仍需完成 5 万/10 万档压测、Checkpoint P95、Lag 峰值和扩并行度对比。

## 11. 面试表达重点

重点讲清：为什么事实选择这个粒度、为什么订单更新需要 Changelog、为什么 SCD2 用事件时间、为什么多明细订单先折叠、故障如何证明恢复、Exactly-Once 的边界在哪里。不要用“九节点集群”“完整四层物理数仓”或“端到端 Exactly-Once”等不准确表述。
