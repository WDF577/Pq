# Schema Evolution 策略

## 当前策略

订单 CDC 使用显式表结构。上游 DDL 不会被未经评审地直接传播到 DWD、ClickHouse 和 Power BI：

- 新增 nullable 字段：兼容，运行任务继续投影旧字段；登记契约版本，再通过 Savepoint 计划升级。
- 字段重命名、类型收窄、删除字段：不兼容，先停止发布并评估下游影响。
- 删除表、清空表：生产默认禁止自动向下游传播。

`cdc_schema_contract` 保存部署到 MySQL 的表级版本记录；`contracts/cdc_contracts.json` 进一步固定五张 CDC 表的字段、Flink 类型、可空性、主键、ODS Topic 与 tombstone 语义。`tests/test_data_contracts.py` 会把机器可读契约与 MySQL/Flink DDL 做静态比对，避免只靠口头约定，也避免上游改了字段而下游 SQL 没同步。

## 演练

```powershell
.\scripts\schema_evolution_drill.ps1
```

或：

```bash
bash scripts/schema_evolution_drill.sh
```

演练向 `order_info` 增加 nullable `delivery_type`，更新已有数据，等待 CDC 捕获 DDL/DML，然后检查九条作业仍然运行。干净重建环境会删除该字段并恢复基线契约。

## 生产升级步骤

1. 在测试环境验证 DDL、历史数据和下游兼容性。
2. 触发 Savepoint，保留回滚点。
3. 扩展 Kafka/ClickHouse/语义模型字段，优先先加后用。
4. 升级 Flink Source/DWD SQL 并从 Savepoint 恢复。
5. 验证 Lag、Checkpoint、DLQ 和核心指标后再允许业务使用新字段。
6. 删除或改名必须经过双写与下游迁移期。
