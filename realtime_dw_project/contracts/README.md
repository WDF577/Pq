# CDC 数据契约

`cdc_contracts.json` 是 MySQL → Flink CDC → Kafka ODS 的机器可读字段契约，`cdc_contracts.schema.json` 约束契约文件自身结构。

## 为什么保留这一层

- MySQL DDL 是业务库实现，Flink DDL 是消费实现；两者都不是跨团队可评审的稳定接口。
- 契约单独声明字段、类型、可空性、主键、Topic 和删除语义，CI 能在运行重型容器前发现漂移。
- 五张表由独立 CDC Source 采集，契约明确 `cross_table_consistency=eventual`，避免把单 Topic Exactly-Once 误解为跨表事务一致。

## 变更规则

- 新增可空字段：先提升 `contract_version`，补契约与测试，再按“先加后用”升级 MySQL、Flink、ClickHouse 和 BI。
- 改名、删除、类型收窄或主键变化：视为不兼容变更，需要双写/迁移期和回滚方案。
- Kafka 分区路由或 offset 重置：不属于普通字段演进；必须同时评审 Loader `version_epoch`。

执行 `pytest -q` 会检查五张契约是否覆盖完整，并核对 MySQL/Flink Source/Sink 的字段、类型、主键和 Topic。
