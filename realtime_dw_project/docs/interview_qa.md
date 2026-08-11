# 面试问答：电商交易实时数仓

## 1. 一分钟介绍项目

这是一个企业化设计、单机可复现的电商实时数仓。我做了两条链路：第一条将带 `session_id/order_id` 的用户行为写入 Kafka，Flink SQL 基于事件时间完成清洗、维表补全、DLQ、窗口指标和严格漏斗；第二条从 MySQL 的订单、明细、支付、退款和商品 SCD2 表读取快照与 binlog，经 Upsert Kafka 构建订单明细 DWD 和生命周期 ADS。Flink 配置 RocksDB、10 秒 Checkpoint、重启策略和 Kafka Exactly-Once Sink；结果由常驻 Loader 显式提交 offset 并幂等写入 ClickHouse，坏消息经 broker 确认后进入独立 DLQ。Power BI 展示业务结果，Prometheus/Grafana/Alertmanager 监控运行状态和端到端业务数据年龄，并提供可手动触发的集成 CI 来验收完整数据面。它验证了 TaskManager 故障恢复、Schema 兼容变更、DELETE tombstone 和 24 项数据质量规则，但我会明确它是单机实验环境，不是生产高可用集群。

## 2. 项目的核心亮点是什么？

不是组件数量，而是三个可验证能力：

1. 业务表更新能经 CDC 和 Changelog 正确传播，而不是只处理 Append 日志。
2. 商品调价后，订单能按下单时间命中 SCD2 历史价格。
3. 故障、重放和字段新增都有脚本与验收证据，边界也说得清楚。

## 3. 为什么既有行为流，又有订单 CDC 流？

行为日志主要是追加事件，适合 Watermark、窗口和漏斗；订单会从 CREATED 更新为 PAID、CANCELLED 或 REFUNDED，适合 CDC、Upsert 和撤回流。两者一起覆盖实时数仓常见的 Append 与 Changelog 语义。

## 4. 为什么用 Kafka？

Kafka 解耦上游、计算和存储，提供削峰、持久化和重放。Flink 故障后可从 Checkpoint 记录的 offset 恢复；多个下游也能使用不同消费组独立读取。当前 3 分区是为了展示并行与 key 路由，但只有单 Broker、单副本，不能称为高可用。

## 5. ODS、DWD、DWS、ADS 怎样落地？

- ODS：Kafka 原始行为和 5 个 MySQL Upsert Topic。
- DWD：清洗补维后的行为明细，以及订单/支付/退款/SCD2 关联后的订单明细宽表，落 Kafka 与 ClickHouse。
- DWS：Flink 内部的窗口或订单级聚合逻辑，没有单独物理表。
- ADS：行为经营指标、严格漏斗、订单生命周期和每日渠道指标，落 Kafka 与 ClickHouse。

所以准确说法是“ODS-DWD-ADS 持久化，DWS 为逻辑层”。

## 6. 订单 DWD 为什么选择明细粒度？

一张订单可以有多个商品，商品分析需要明细粒度，所以业务键是 `detail_id`。但是订单数和支付金额不能直接对明细求和，否则多明细订单会重复。ADS 先按 `order_id` 折叠为一行，再按渠道或日期聚合。

## 7. CDC 捕获哪些表，怎样表达更新？

捕获 `order_info`、`order_detail`、`payment_info`、`refund_info`、`dim_product_scd2`。Flink CDC 读取初始快照和 MySQL ROW binlog，ODS 使用 Upsert Kafka：同一主键的新 value 覆盖旧值，删除用 tombstone。下游 Flink 读取 Changelog 后维护最新关联结果。

五个 Source 放在同一作业便于统一运维和 Checkpoint，但它们各自维护快照/位点并写不同 Topic，不能说跨表原子可见。订单与支付短暂先后到达是预期现象，Changelog Join 会在两边更新到达后最终收敛。

## 8. SCD2 是怎样实现的？

商品调价时，在一个事务里关闭旧版本：`effective_to=调价时间`、`is_current=0`；再插入新版本：版本号加一、`effective_from=调价时间`、`is_current=1`。订单 DWD 用 `order_create_time >= effective_from AND order_create_time < effective_to` 关联，因此可以还原下单时目录价。质量规则检查每个商品恰好一个当前版本、区间不重叠、成交价与命中版本一致。

## 9. 为什么 SCD2 使用左闭右开区间？

在调价边界时刻，旧版本不再有效，新版本开始有效。`[from,to)` 可以保证任意时间最多命中一个版本，避免两个闭区间在边界同时命中。

## 10. Watermark 有什么作用？

行为源定义 `event_ts - 5 秒` 的 Watermark，用来在事件时间计算中容忍有限乱序并推动窗口关闭。它不是“迟到数据永不丢失”的保证；超过容忍范围的数据可能错过已关闭窗口。生成器按事件时间递增发送，只注入有限抖动来匹配这一口径。

## 11. 行为维表为什么用 Processing-time Temporal Lookup？

商品、店铺、地区是低频查询维度，JDBC Lookup 配合 Partial Cache 可以降低 MySQL 压力。它返回处理时刻的当前值，不保证历史版本；需要历史语义的商品价格在交易链路里通过 CDC + SCD2 单独实现。能解释这两个语义差异比统一使用一种 Join 更重要。

## 12. 为什么用 LEFT JOIN？

维表未命中时仍保留事实，质量报告才能暴露缺失；INNER JOIN 会静默丢事实并让业务量变小。订单 DWD 里的强业务关系则通过质量规则要求支付/退款状态一致。

## 13. 严格漏斗怎样计算？

同一 `session_id` 在 30 分钟窗口内先提取每个阶段最早时间，再要求 `view_ts <= cart_ts <= order_ts <= pay_ts`。因此支付会话一定属于上游集合。边界是跨固定窗口的长会话可能不完整，生产可评估 Session Window、CEP 或离线归因修正。

## 14. 商品榜是不是 Flink TopN？

不是。Flink 输出的是“5 分钟窗口 + 商品”的聚合结果，展示层选择最新窗口的 Top 10。真正 Flink TopN 需要窗口聚合后使用 `ROW_NUMBER() OVER (PARTITION BY window ORDER BY amount DESC)` 并筛选 `rn <= 10`。

## 15. Exactly-Once 做到了哪一段？

Kafka → Flink → Kafka 通过 Checkpoint 和事务性 Kafka Sink 使用 Exactly-Once。Kafka → Python Loader → ClickHouse 没有分布式事务：Loader 写成功后提交 offset，失败时允许重放；ClickHouse 用业务键和由 Loader epoch + Kafka partition/offset 确定性生成的版本列逻辑去重，使旧 offset 的历史重放不会覆盖新值。因此后半段是最终一致和逻辑幂等，不能称端到端 Exactly-Once。

## 16. ReplacingMergeTree 会立即去重吗？

不会。后台 Merge 是异步的，项目验收使用 `FINAL` 读取逻辑最新版本。大表生产查询不能无条件依赖 `FINAL`，应评估物化视图、版本聚合、分区设计或专用 Connector。

## 17. TaskManager 故障怎样验证？

脚本先确认 9 条 Job 和全部 Task 健康，再记录恢复日志计数，杀掉 TaskManager，由 Compose 显式重拉。脚本必须看到本次新增的 `Restoring job ... from Checkpoint`，再等待 9 条作业恢复，最后用新消费组重放并执行 21 项业务验收。这样同时证明状态恢复与结果幂等，而不是只看容器重新启动。

## 18. Schema Evolution 怎么做？

新增可空字段属于兼容变更：上游先加字段并登记 `cdc_schema_contract`，显式旧 Schema 的 Flink 作业继续运行；要使用新字段时，先扩展 Kafka/ClickHouse/语义模型，再通过 Savepoint 升级作业。删除、改名和类型收窄是不兼容变更，需要双写或迁移期，不能自动透传。

## 19. 数据质量检查什么？

质量报告共 24 项，覆盖主键、合法枚举、维表命中、漏斗单调、支付与退款一致性、SCD2 唯一当前版本与区间不重叠、历史价格命中、订单 ADS 金额和数量关系。Kafka 重放后还要验证 ClickHouse `FINAL` 口径业务键不膨胀。

## 20. 你实际解决过什么问题？

推荐讲 SCD2 重复压测问题：首次演练完全正确，但连续调价后出现历史价格不匹配。定位发现生成器每次都把订单时间回填到过去一小时，却使用运行时当前价格，导致事件时间落入旧版本区间。修复为每次运行使用真实插入时间，在调价边界隔开毫秒，并用干净重建 + 历史价格质量规则回归。这说明问题不在 Join 语法，而在事实时间与维度有效时间必须使用同一业务口径。

另一个可讲问题是 Flink SQL Client 遇到 SQL 错误仍可能返回进程码 0；一键脚本现在同时检查 native exit code 和输出中的 `[ERROR]`，并分阶段提交 SQL，避免等待不存在的 9 条作业直至超时。

删除语义也不是口头说明：演练会删除订单头，要求关联 DWD 明细收到 tombstone 并在 ClickHouse 变为 `is_deleted=1`，随后恢复源订单并验证重新生效。它同时暴露过“使用装载时间作版本会让旧消息重放覆盖新值”的风险，因此版本改成确定性的 Kafka 位置。

## 21. 为什么没有再加入 Hadoop、Hive、Spark、Airflow？

当前目标是单机实时 CDC 和 Serving 链路，没有大规模离线历史批处理或复杂离线 DAG。Flink 已承担实时计算，继续加入第二套引擎只会增加伪复杂度。企业级是每个组件解决明确问题，不是堆名词。

## 22. 如果上生产还缺什么？

- Kafka 多 Broker、多副本、ISR 与容量规划。
- JobManager HA、多个 TaskManager、Kubernetes 和远端对象存储 Checkpoint。
- Schema Registry、元数据、血缘、指标口径中心和补数平台。
- 成熟 ClickHouse Connector 或更强的端到端一致性设计。
- 权限、密钥、审计、脱敏与真实告警通知闭环。
- 压测、容量模型、SLA/SLO 和跨可用区容灾。

## 23. 这个项目最值得表达的收获是什么？

实时数仓的可信度不取决于 SQL 写了多少，而取决于粒度、时间语义、更新语义和失败边界是否一致。能解释为什么多明细订单先折叠、为什么 SCD2 用事件时间、为什么重放不等于端到端 Exactly-Once，比只说“熟悉 Kafka/Flink”更有说服力。

## 24. Loader 遇到一条坏消息为什么不会无限重启？

Loader 会捕获 JSON 解码、非对象、必填字段缺失，以及与 ClickHouse 表映射不符的整数、Decimal、日期和时间格式，把源 topic/partition/offset、原始 key/value Base64、错误信息和确定性 SHA-256 消息 ID 写入 `clickhouse_loader_dlq`。只有 DLQ 获得 broker ACK 后，该消息的 `offset + 1` 才进入显式提交集合；DLQ 发布失败不会推进源 offset。DLQ 使用纯 `compact`，消息 ID 是 key，因此保留每条异常最新的 `pending/replayed` 状态且不按时间删除；代价是需要容量监控和审计归档。修复工具默认只读且隐藏 payload，执行回放必须完整扫描到各非空分区当时 high watermark，并指定单条 ID、修复方式和 `--execute`；扫描不完整会拒绝写入。它还拒绝未知 envelope 版本，并用目标 Topic 的同一套 Loader 规则预检修复后的 payload，避免无效回放再次进 DLQ。

## 25. 怎样判断“链路没有积压但业务数据已经不更新”？

Kafka Lag 为 0 只能说明消费追上了，不能证明上游仍在产数。独立 Freshness Exporter 查询行为 DWD、订单 DWD 和实时 ADS 的最大业务时间，暴露 `pipeline_data_age_seconds`、采集成功和时间戳可用性。数据年龄变大既可能是断流，也可能是正常静默，所以陈旧告警默认关闭；只有在明确持续流量或 SLA 时间窗时才显式启用。这比无条件设置一个“5 分钟没数据就报警”更可信。

## 26. CI 为什么不在每次提交都启动完整 Flink 链路？

push/PR 运行 Python 单测、Shell/JSON/Compose 静态检查，反馈快且资源稳定。完整链路需要 Kafka、MySQL、Flink、ClickHouse 和 Connector 下载，放在 `workflow_dispatch` 手动集成任务：验证 9 条 Job、9 个查询输出、tombstone 删除恢复和 24 项质量规则，成功上传质量报告，失败保存容器日志与 Flink 作业快照。这样把快速门禁与重型验收分层，而不是让每次小改动都承担 30～40 分钟成本。
