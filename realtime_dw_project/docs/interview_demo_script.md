# 面试项目讲解脚本

## 30 秒开场

我做的是一个单机可复现、但按企业故障边界设计的电商实时数仓。它同时处理追加型行为日志和会更新撤回的订单 CDC：Kafka 负责解耦与重放，Flink SQL 处理事件时间、Changelog Join 和 SCD2，ClickHouse 提供查询，Power BI 展示；我重点验证了幂等、坏消息隔离、删除传播、故障恢复和数据质量，而不是只把组件启动起来。

## 3 分钟标准讲法

1. **业务问题**：行为流要算 PV、UV、严格漏斗；订单会从 CREATED 变成 PAID、CANCELLED、REFUNDED，还要按下单时间还原商品历史价格。
2. **数据路径**：行为日志进入 Kafka；订单、明细、支付、退款和商品 SCD2 从 MySQL 经 Flink CDC 进入 5 个 Upsert Topic。Flink 输出行为 DWD/ADS 和订单明细 DWD/生命周期 ADS。
3. **关键设计**：订单 DWD 采用明细粒度，但 ADS 先按订单折叠，避免多商品订单重复计算金额；商品版本使用 `[effective_from,effective_to)` 与订单事件时间关联。
4. **可靠性**：Kafka→Flink→Kafka 依赖 Checkpoint 和事务 Sink；Kafka→ClickHouse 不宣称 Exactly-Once，而是先写后显式提交 offset、确定性版本加 ReplacingMergeTree 的逻辑幂等。
5. **异常闭环**：坏消息只有在 DLQ 获 broker ACK 后才推进源 offset；DELETE tombstone 可以把 DWD 软删除并恢复；Freshness Exporter 监控业务时间而不只看 Kafka Lag。
6. **证据**：9 条 Flink Job 实际运行，TaskManager 故障恢复约 26 秒；1 万旅程和 1000 订单实测约 526 条/秒；24 项质量规则通过，Power BI 有经营、订单生命周期和 SCD2 审计三页。
7. **边界**：这是单 Broker、单 JobManager/TaskManager 的实验环境，能证明机制和口径，不能声称生产高可用或端到端 Exactly-Once。

## 10 分钟深挖顺序

按下面顺序展开，不要从组件清单开始背：

1. 订单多明细为什么会重复计数，以及 ADS 怎样先折叠。
2. SCD2 边界时刻为什么必须左闭右开。
3. 五个 CDC Source 为什么只能最终收敛，不能说跨 Topic 原子一致。
4. ClickHouse 写成功、offset 提交失败时为什么允许重放。
5. 坏消息为什么必须先写 DLQ 再提交，而不是简单 `try/except continue`。
6. Kafka Lag 为 0 为什么仍可能业务断流，以及新鲜度告警为什么要有启用条件。
7. 用 TaskManager 故障、tombstone 删除恢复、Schema 兼容和集成 CI 展示证据。

## 卡住时的回答模板

遇到不会的细节，先说清边界再推导：

> 这个点我没有在当前单机版本里实现，我目前实现的是……；如果上生产，我会先确认……，再选择……，因为主要权衡是……。我不会把当前项目描述成已经具备这个能力。

项目题优先回答四件事：业务粒度、时间语义、更新/删除语义、失败后如何证明恢复。即使记不起某个配置名，也不要失去这条主线。

## 演示顺序

1. README 架构图与组件边界。
2. Flink Web UI：9 条 RUNNING Job、Checkpoint、Backpressure。
3. Grafana：Lag、Checkpoint、Loader DLQ、新鲜度。
4. Power BI：订单生命周期和 SCD2 审计。
5. `artifacts/`：性能、故障恢复、质量报告。
6. 代码只展示三处：订单 DWD 粒度/SCD2 Join、Loader 显式 offset + DLQ、tombstone drill。

演示控制在 3～5 分钟，面试官追问时再进入代码，不要主动逐文件讲解。
