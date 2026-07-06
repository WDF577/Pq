# 面试问答：电商用户行为实时数仓

## 1. 请介绍一下这个项目的整体架构

项目采用经典的 Lambda 架构简化版：Python 模拟用户行为 → Kafka 消息队列（ODS 层）→ Flink SQL 实时计算（DWD + DWS/ADS）→ ClickHouse 结果存储 → Streamlit 可视化。共 6 个 Docker 容器，遵循 ODS → DWD → DWS → ADS 四层数仓分层。ODS 层保留原始日志，DWD 层完成清洗和维表关联（Temporal Join 关联 MySQL），DWS/ADS 层通过 TUMBLE 窗口聚合生成 5 类业务指标。

## 2. 为什么选择 Kafka 而不是直接写数据库？

Kafka 在项目中承担**消息队列的削峰填谷和解耦**作用。用户行为是流式持续产生的，如果直接写 ClickHouse 或 MySQL，高峰期会压垮存储层。Kafka 先把数据缓冲起来，Flink 按自己的速度消费处理，生产者和消费者互不依赖。此外 Kafka 支持多消费者组，Flink、监控脚本、备份任务可以各自消费同一份数据。

## 3. 为什么用 Flink SQL 而不是 Spark Streaming？

Flink 是真正的流式计算引擎，每条数据来一条处理一条（事件驱动），延迟在毫秒-秒级。Spark Streaming 本质是微批处理，延迟在秒-分钟级。对于实时数仓场景（1 分钟窗口指标），Flink 更合适。Flink SQL 还可以用接近标准 SQL 的方式编写流式计算逻辑，降低开发门槛，维表 JOIN、窗口聚合等都可以用 SQL 表达。

## 4. ODS/DWD/DWS/ADS 分别解决什么问题？

- **ODS（操作数据层）**：保留原始日志原貌，不做加工，方便数据回溯和问题排查。本项目对应 Kafka Topic `ods_user_behavior`。
- **DWD（明细数据层）**：对 ODS 数据做清洗（过滤脏数据）+ 字段补全（关联维表），形成干净的业务明细。本项目对应 Kafka Topic `dwd_user_behavior` + ClickHouse `dwd_user_behavior` 表。
- **DWS（汇总数据层）**：按业务主题和时间窗口做轻度汇总，如按 1 分钟/5 分钟窗口聚合。本项目对应 Flink SQL 中 TUMBLE 窗口聚合逻辑。
- **ADS（应用数据层）**：面向最终查询、报表、看板的指标表，直接可被 BI 工具或看板消费。本项目对应 ClickHouse 中的 `ads_realtime_overview`、`ads_product_rank` 等表。

## 5. Flink 中 Watermark 的作用是什么？怎么设置的？

Watermark 用于处理**数据乱序和延迟到达**的问题。Flink 基于事件时间做窗口计算时，需要知道"什么时候窗口可以关闭并输出结果"。Watermark = max(event_time) - delay，表示"比 Watermark 更早的数据都到了"。

本项目设置：`WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND`，允许 5 秒内的乱序数据。当 Watermark 超过窗口结束时间，窗口触发计算并输出结果。

## 6. Temporal Table Join 是什么？为什么用它关联维表？

Temporal Table Join（时态表连接）是 Flink SQL 中关联维表的机制。本项目使用 `FOR SYSTEM_TIME AS OF o.proc_time` 语法，以处理时间为基准，关联 MySQL 维表的**当前快照**。

为什么不用普通 JOIN？因为维表数据可能变化（如商品改名、店铺换地区），Temporal Join 保证每次查询关联的是维表**最新**状态，而不是历史快照。Flink 会缓存维表数据（Lookup Join），避免每条数据都查 MySQL。

## 7. 商品的 province/city 是怎么拿到的？为什么不在日志里直接带？

用户行为日志里只有 `shop_id`，没有省份和城市。项目中通过三层 JOIN 拿到：
1. `ods_user_behavior.shop_id` → `dim_shop` 拿到 `region_id`
2. `dim_shop.region_id` → `dim_region` 拿到 `province` 和 `city`

这是数仓中典型的**星型模型**：事实表只存外键 ID，维度信息通过维表关联补充。好处是维表独立维护，不会因为店铺搬城市而需要改历史日志。

## 8. 窗口聚合为什么用 TUMBLE 而不是 HOP 或 SESSION？

- **TUMBLE（滚动窗口）**：固定大小、不重叠。适合计算"每 1 分钟的 PV"、"每 5 分钟的商品排行"。本项目全部用 TUMBLE。
- **HOP（滑动窗口）**：固定大小、有滑动步长、会重叠。适合计算"最近 30 分钟滚动统计，每 5 分钟更新"。
- **SESSION（会话窗口）**：按活动间隔切分、大小不固定。适合计算"用户一次会话内的行为路径"。

本项目指标需求是"每分钟/每 5 分钟统计一次"，用 TUMBLE 最合适。如果后续要做"最近 30 分钟滑动统计"，可以改用 HOP。

## 9. 转化漏斗的 4 级转化率怎么计算的？

渠道转化漏斗统计每个渠道（app/h5/wechat/search/ad）的用户行为转化：

- `view_to_cart_rate` = cart_users / view_users（浏览→加购）
- `cart_to_order_rate` = order_users / cart_users（加购→下单）
- `order_to_pay_rate` = pay_users / order_users（下单→支付）
- `view_to_pay_rate` = pay_users / view_users（浏览→支付，端到端转化率）

用 `NULLIF` 防止除零错误。每个指标用 `COUNT(DISTINCT user_id)` 去重。

## 10. 脏数据是怎么处理的？

模拟数据生成时注入约 3% 的脏数据：
- 1% event_id 为空
- 0.5% user_id 为 NULL
- 0.5% event_type 为非法值
- 1% product_id 不存在（9999/8888）
- 0.5% shop_id 不存在（99/88）

Flink DWD 层通过 WHERE 条件过滤：`event_id IS NOT NULL AND user_id IS NOT NULL AND event_type IN ('view','cart','order','pay')`。不存在的 product_id/shop_id 在 LEFT JOIN 维表时变成 NULL，不影响统计。

## 11. ClickHouse 为什么用 ReplacingMergeTree 引擎？

ClickHouse 的 ReplacingMergeTree 在后台 Merge 时会按 ORDER BY 键去重（保留最新版本）。本项目 ADS 指标表 ORDER BY (window_start, window_end, ...)，当同一个窗口的指标被重新计算并写入时，旧的会被替换。对于实时场景，同一窗口可能因为迟到数据需要更新，ReplacingMergeTree 可以保证查询时看到最新的指标值。

## 12. 异常告警的逻辑是什么？

告警脚本 `generate_alerts.py` 查询 ClickHouse 中最近 10 个窗口的概览数据，生成 4 种告警：
1. **支付金额为 0**（WARNING）
2. **支付人数为 0**（WARNING）
3. **支付金额骤降**：当前窗口 < 近 5 窗口均值的 50%（CRITICAL）
4. **转化率异常**：view → pay < 1%（WARNING）

告警写入 ClickHouse `ads_realtime_alert` 表，Streamlit 看板实时展示。

## 13. Kafka 的 Topic 怎么设计的？分区数为什么是 1？

4 个 Topic 分别对应数仓各层：
- `ods_user_behavior`：ODS 层，接收原始日志
- `dwd_user_behavior`：DWD 层，Flink 输出清洗后的明细
- `ads_realtime_overview` / `ads_product_rank` / `ads_category_rank` / `ads_channel_funnel`：ADS 层，Flink 输出聚合指标

分区数设为 1 是因为 Demo 环境单机部署，不需要并行消费。生产环境应按数据量设置更多分区（如 8-16），以支持并行消费和提高吞吐。

## 14. 为什么用 Docker Compose 而不是 Kubernetes？

Docker Compose 适合本地开发和 Demo 环境，一条命令启动全部 6 个服务，配置写在 YAML 文件中，学习和演示成本低。生产环境应迁移到 Kubernetes 或云厂商托管服务（如阿里云 Flink、Confluent Cloud）。

## 15. 这个项目如果上生产，还需要补充什么？

1. **Flink Checkpoint**：配置 Checkpoint + 状态后端（RocksDB），保证任务故障恢复。
2. **Kafka 多分区 + 副本**：提高吞吐和可用性。
3. **MySQL CDC**：通过 Debezium/Canal 实时同步维表变更。
4. **Redis 缓存**：维表查询加 Redis 缓存层，减少 MySQL 压力。
5. **监控告警**：接入 Prometheus + Grafana，监控 Flink 任务延迟、Kafka 消费 lag。
6. **可视化大屏**：接入 DataEase/Superset/Grafana，替代 Streamlit。
7. **数据治理**：元数据管理（Atlas/DataHub）、数据血缘、数据质量监控。

## 16. 你在这个项目中遇到的最大挑战是什么？

**Python 3.12 与 kafka-python 2.0.2 的兼容性问题**。kafka-python 2.0.2 内部 vendored 了一个 six 模块，在 Python 3.12 下 `six.moves` 导入失败。这是一个典型的依赖管理问题。我的解决方案是将整个项目迁移到 **confluent-kafka**（Confluent 官方 Python 客户端），它由 C 库 librdkafka 支持，兼容性更好，性能也更高。这个过程中需要重写 `generate_mock_events.py` 和 `load_kafka_to_clickhouse.py` 两个脚本的 Producer/Consumer API。

## 17. 为什么要做数据质量报告？

数据质量是数仓的生命线。如果 DWD 数据不干净，ADS 指标就不可信。质量报告从 4 个维度检查数据质量：
1. **完整性**：ODS 日志总量
2. **准确性**：DWD 清洗后脏数据残留量
3. **一致性**：维表关联命中率
4. **及时性**：各层表是否有数据产出

这对于面试很重要——面试官会想知道你是否有数据质量意识，是否在项目中考虑了异常情况的处理。
