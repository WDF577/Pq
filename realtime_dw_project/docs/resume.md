# 简历项目描述（与当前实现一致）

## 项目名称

基于 Kafka + Flink CDC + Spark SQL + ClickHouse 的电商交易批流数仓

## 技术栈

Kafka / Flink SQL / Flink CDC / Spark SQL / RocksDB / ClickHouse / MySQL / Parquet / Prometheus / Grafana / Docker Compose / Python / Power BI

## 推荐描述

- 基于 Docker Compose 编排 MySQL、Kafka、Flink、ClickHouse 与监控组件，构建业务库 CDC/行为日志 → ODS → DWD → ADS → Power BI 的可复现实时时数仓链路。
- 使用 Flink CDC 捕获订单、明细、支付、退款和商品历史维度的快照/binlog；生成器分阶段提交 CREATED、支付/取消与退款事务，以订单明细为 DWD 粒度处理 Changelog 更新并关联支付退款信息。
- 设计商品 SCD2 调价过程和 `[effective_from, effective_to)` 事件时间关联，质量 SQL 校验唯一当前版本、有效区间不重叠及订单历史成交价命中。
- 为 Flink 配置 10 秒 Checkpoint、RocksDB、固定延迟重启和 Kafka Exactly-Once Sink；TaskManager 故障演练验证 9 条作业恢复、结果重放与业务主键逻辑幂等。
- 构建 1 分钟经营概览、30 分钟严格漏斗和订单生命周期 ADS；常驻 Loader 以显式 Kafka offset 与确定性版本幂等写 ClickHouse，坏消息获 DLQ broker 确认后才推进 offset，并通过 DELETE tombstone 演练验证软删除恢复。
- 建设 24 项数据质量规则、端到端业务时间新鲜度监控、Prometheus/Grafana 告警和 Power BI 看板；设计可手动触发的集成 CI，覆盖 9 条 Flink 作业、9 个查询输出与 tombstone 恢复，并配置成功/失败证据归档。
- 使用 Spark SQL 从 MySQL 事务事实按日期范围重算订单日指标，JDBC 谓词下推后按 `order_date` 动态覆盖 Parquet 分区，并与 ClickHouse 实时 ADS 全外连接对账；实测 1,540 笔订单连续回补两次均得到 5 行渠道指标、差异 0 且结果不膨胀。

## 不建议使用的说法

- “十节点集群”：当前是单机容器化服务，不是十台机器或高可用集群
- “Flink TopN”：当前 Top 10 在 Streamlit 侧选择
- “跨窗口完整漏斗”：当前严格路径仍受 30 分钟固定窗口边界限制
- “完整四层物理数仓”：DWS 仅为逻辑窗口聚合，没有单独持久化
- 固定累计行数：实际结果受运行次数、脏数据和消费组影响
- “保证最新值”：ReplacingMergeTree 后台合并是异步的
- “端到端 Exactly Once”：Kafka 到 ClickHouse 是可重放的逻辑幂等，不是分布式事务
- “GitHub Actions 已云端验证”：首次 `workflow_dispatch` 成功并留下可访问的 artifact 前，只能说已实现集成工作流和本地等价验收
- “分布式 Spark/Hive 湖仓”：当前是按需 `local[2]` Spark SQL 与本地 Parquet，用于证明批处理和对账语义
- 未经实测的固定吞吐：只使用 `artifacts/benchmark_*.md` 的本机实测并注明单机环境
