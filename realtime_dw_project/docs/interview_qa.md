# 面试问答：电商用户行为实时数仓

## 1. 用一分钟介绍项目

这是一个个人学习型实时数仓项目。我用 Python 生成浏览、加购、下单和支付事件，写入 Kafka ODS Topic；Flink SQL 基于事件时间和 Watermark 做清洗，通过 JDBC Temporal Join 关联 MySQL 商品、店铺和地区维表，输出 DWD 明细和 1 分钟/5 分钟窗口指标；结果先写回 Kafka，再由 Python 批量落入 ClickHouse，最后用 Streamlit 展示概览、商品品类聚合、渠道阶段人数和告警。项目由 Docker Compose 编排 6 个本地服务。

## 2. 为什么使用 Kafka？

Kafka 把生产者、计算层和存储层解耦，并提供缓冲和可重放能力。生成脚本不需要知道 Flink 和 ClickHouse 的处理速度，Flink 结果也可以由不同消费组独立消费。当前 Demo 是单分区、单副本；生产环境需根据吞吐量设置分区和副本。

## 3. 数据分层如何落地？

- ODS：Kafka `ods_user_behavior`，保存原始 JSON
- DWD：Flink 清洗并关联维表，写入 Kafka 和 ClickHouse
- DWS：Flink 的 1 分钟/5 分钟窗口聚合逻辑，没有单独落表
- ADS：4 类窗口结果写入 Kafka 和 ClickHouse

准确说法是“ODS-DWD-ADS 是持久化层，DWS 是逻辑聚合层”。

## 4. DWD 的粒度是什么？

一行代表“一个用户在一个时间点，对一个商品发生的一次行为”，业务键是 `event_id`。先确定粒度，再决定可以放哪些维度和度量，避免把不同粒度的数据混在同一事实表中。

## 5. Watermark 有什么作用？

Watermark 用于事件时间计算中的乱序处理。本项目：

```sql
WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
```

表示允许 5 秒有限乱序。有限批次生成器按事件时间递增发送并默认注入 3 秒抖动，Watermark 超过窗口结束时间时触发结果。更晚到达的数据可能错过已关闭窗口，这是当前配置的取舍。

## 6. Temporal Join 关联的是什么状态？

项目使用 processing-time JDBC Temporal Join：

```sql
LEFT JOIN dim_product FOR SYSTEM_TIME AS OF o.proc_time AS p
```

它关联处理时刻可见的 MySQL 维表状态。当前没有配置 Lookup Cache、CDC 或 SCD2，因此不能声称还原事件发生时的历史维度版本。

## 7. 为什么用 LEFT JOIN？

维表未命中时仍保留行为明细，便于在质量报告中发现商品、店铺或地区维度缺失。如果使用 INNER JOIN，未命中数据会被直接丢弃，业务量可能悄悄减少。

## 8. PV、UV 的准确口径是什么？

- PV：窗口内 `event_type='view'` 的事件数
- UV：窗口内发生 `view` 行为的去重用户数

之前使用 `COUNT(*)` 会把加购、下单和支付也算进 PV，口径错误，现已改为条件聚合。

## 9. 商品排行是 Flink TopN 吗？

不是。Flink SQL 产出的是“5 分钟窗口 + 商品”的支付次数和支付金额聚合，表中没有排名字段。Streamlit 从最新窗口数据中取支付金额 Top 10。

如果要在 Flink 中实现真正 TopN，需要在窗口聚合之后使用 `ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY pay_amount DESC)`，再筛选 `rn <= 10`。

## 10. 渠道漏斗是严格漏斗吗？

不是严格路径漏斗。当前表分别统计同一窗口、同一渠道下 view/cart/order/pay 的去重用户数，可以做阶段人数与比率对比，但不能证明支付用户一定属于浏览用户集合。

严格漏斗需要增加 `session_id` 或 `order_id`、构造可追踪事件序列，并明确跨窗口归因规则。这是我识别到的模型边界。

## 11. 为什么结果先写 Kafka，再由 Python 写 ClickHouse？

这样便于拆分 Flink 计算和存储装载，出现问题时可以分别检查 Kafka 结果 Topic 和 ClickHouse。

当前 Loader 按表批量写入 ClickHouse，成功后同步提交 Kafka offset，较逐行 HTTP 和自动提交更稳。但它仍不是端到端 Exactly Once；写入成功而 offset 提交失败时可能重复写入。生产环境可使用成熟 Flink ClickHouse Connector、幂等键或事务性方案。

## 12. ReplacingMergeTree 能保证立即去重吗？

不能。ReplacingMergeTree 在后台 Merge 时按排序键替换旧版本，合并是异步的。本项目 Dashboard 对 ADS 查询使用 `FINAL` 获得稳定演示结果。生产环境应评估 `FINAL` 成本，并通过版本列或聚合查询设计稳定读取口径。

## 13. 数据质量怎么验证？

质量报告验证 ClickHouse 服务层：

- DWD 是否有数据
- 空事件 ID、非法事件类型
- 重复事件 ID
- 商品、店铺、地区维表命中率
- 4 类 ADS 是否有数据
- 是否存在 `UV > PV`

ODS 位于 Kafka，报告不再用 DWD 行数假装 ODS 行数，也不虚构清洗率。

## 14. 为什么维表命中率不是 100%？

模拟器会注入不存在的商品或店铺 ID，用于验证 LEFT JOIN 和质量检查。未命中记录保留在 DWD，维度字段为空。实际业务中需要根据场景选择补默认维度、进入隔离区、延迟重试或阻断下游。

## 15. 遇到过哪些工程问题？

可以重点讲一个，按“现象-定位-原因-解决-验证”回答：

1. 第 5 个 Flink Job 失败
2. 查看 Web UI 和日志，确认可用 Slot 不足
3. 原因是 5 个 Job 并行运行超过默认 Slot
4. 调整 TaskManager Slot 配置
5. 重新提交并用 `flink list`、Kafka Topic 和 ClickHouse 行数验证

其他实际问题包括 Python 3.12 客户端兼容、MySQL 端口冲突、SQL Client 注释解析和 ClickHouse 返回类型导致看板渲染失败。

另一个关键问题是：早期生成器把过去 2 小时事件完全随机发送，而 Watermark 只允许 5 秒，导致大量 ADS 数据被判定为迟到。修复方式是有限批次按事件时间递增发送，只注入 3 秒抖动，再通过 ADS 行数和窗口分布验证。

## 16. 如果把项目用于生产，还缺什么？

- Flink Checkpoint、状态后端、重启策略
- Kafka 多分区、多副本和容量规划
- 端到端一致性、幂等和补数机制
- MySQL CDC、SCD2 或维度版本管理
- 监控 Kafka Lag、Flink 延迟和失败率
- 元数据、血缘、口径中心和数据质量告警
- 权限、密钥、审计和敏感数据治理

## 17. 如何从业务需求设计数据模型？

先明确业务过程和问题，例如“分析渠道在不同阶段的用户人数”；再确定事实粒度；然后识别维度、度量和时间口径；最后设计 DWD 与 ADS，并列出数据质量规则。

本项目的例子：

- 业务过程：用户商品行为
- 事实粒度：一次行为事件
- 维度：用户、商品、店铺、地区、渠道、时间
- 度量：事件次数、用户数、支付金额
- 输出粒度：1 分钟概览、5 分钟商品/品类、1 分钟渠道

## 18. 你在项目中的最大收获是什么？

不是“把组件都跑起来”，而是理解了指标可信度依赖三件事：先定义粒度和口径，再保证链路可验证，最后明确系统边界。修正 PV、识别非严格漏斗、区分聚合和 TopN，都是从“能运行”走向“能解释”的过程。
