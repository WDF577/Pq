# 简历项目描述（与当前实现一致）

## 项目名称

基于 Kafka + Flink + ClickHouse 的电商用户行为实时数仓

## 技术栈

Kafka / Flink SQL / ClickHouse / MySQL / Docker Compose / Python

## 推荐描述

- 基于 Docker Compose 搭建 ZooKeeper、Kafka、MySQL、ClickHouse 及 Flink JobManager/TaskManager 共 6 个容器化服务，构建 Python -> Kafka -> Flink -> ClickHouse -> Streamlit 实时链路，并按 ODS -> DWD -> ADS 组织数据加工。
- 设计 ODS、DWD 与 4 类 ADS 共 6 个 Kafka Topic，拆分日志生成、Flink 实时计算和 ClickHouse 落库模块；可按 Topic 消息、Flink Job 状态及 ClickHouse 表行数逐层定位链路异常。
- 使用事件时间、Watermark、MySQL JDBC Temporal Join 与 TUMBLE 窗口；DWD 层过滤空事件 ID、空用户 ID 和非法事件类型，并关联商品、店铺、地区 3 张维表。
- 构建 1 分钟运营概览、5 分钟商品/品类支付聚合和 1 分钟渠道阶段人数 4 类指标；看板侧完成 Top 10 排行与 view -> cart -> order -> pay 阶段对比。
- 在 ClickHouse 设计 DWD、4 类 ADS 及告警共 6 张表，通过 Streamlit 展示核心结果；补充空值、非法枚举、重复事件 ID、维表命中率及核心表非空校验。

## 不建议使用的说法

- “六节点集群”：当前是 6 个单机容器化服务，不是 6 台机器或高可用集群
- “Flink TopN”：当前 Top 10 在 Streamlit 侧选择
- “严格转化漏斗”：当前缺少订单/会话标识和事件序列
- “完整四层物理数仓”：DWS 仅为逻辑窗口聚合，没有单独持久化
- 固定累计行数：实际结果受运行次数、脏数据和消费组影响
- “保证最新值”：ReplacingMergeTree 后台合并是异步的
