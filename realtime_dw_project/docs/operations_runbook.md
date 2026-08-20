# 运行、验收与故障演练手册

## 1. 一键干净演示

```bash
bash start_demo.sh
```

Windows PowerShell（推荐，避免 WSL 未连接 Docker Desktop）：

```powershell
.\start_demo.ps1
```

该命令会执行 `docker compose down -v`，删除本项目的本地演示数据卷并重新生成数据，只适合 Demo 环境。

服务入口：

| 服务 | 地址 | 用途 |
| --- | --- | --- |
| Flink Web UI | http://localhost:8081 | Job、Checkpoint、反压与异常 |
| Prometheus | http://localhost:9090 | 原始运行指标查询 |
| Grafana | http://localhost:3000 | 运行监控看板，本地默认 admin/admin |
| Loader Metrics | http://localhost:9410/metrics | 消费、写入、失败与批次耗时 |
| Freshness Metrics | http://localhost:9420/metrics | DWD/ADS 最大业务时间与端到端数据年龄 |
| Streamlit | http://localhost:8501 | 开发诊断页 |
| Power BI | `bi/电商实时数仓运营看板.pbip` | 业务报表 |

`clickhouse-loader` 没有对外端口，它会随 Compose 常驻并在异常退出后自动重启。查看装载日志：

```bash
docker compose logs -f clickhouse-loader
```

Loader 告警排查：先确认 `up{job="clickhouse-loader"}`，再看 `clickhouse_loader_insert_failures_total`、`clickhouse_loader_last_success_unixtime_seconds` 和默认消费组 `clickhouse_loader` 的 Kafka Lag。若修改 `CLICKHOUSE_LOADER_GROUP_ID`，需要同步修改 `LoaderWriteStalled` 规则里的消费组标签。

Loader 遇到 JSON 解析、ClickHouse 映射类型/格式或必填字段异常时不会让整批反复失败。原始 key/value 会以 Base64 保存在 `clickhouse_loader_dlq`，并记录源 topic/partition/offset、确定性消息 ID 和错误原因；只有 DLQ 得到 broker ACK 后源 offset 才会进入显式提交集合。该 Topic 使用纯 `compact`：消息 ID 是 key，保留每条异常最新的 `pending/replayed` 状态，没有 delete retention 造成的时间性静默过期；这不等于无限容量，生产环境仍要监控磁盘并按审计要求归档。查看待处理消息默认不会显示 payload，也不会执行写入：

```bash
python3 scripts/replay_loader_dlq.py
```

只读扫描受 `--scan-limit` 限制时会明确输出 `TRUNCATED`，此时结果可能不是最新状态。真正执行回放会忽略该条数上限，并且必须扫描至所有非空分区启动时捕获的 high watermark（`low >= high` 的空分区直接完成）；任何超时或未完整扫描都会拒绝 `--execute`。回放工具还会拒绝未知 DLQ schema 版本，并复用 Loader 的目标 Topic 字段类型规则验证修复结果，避免“修复”后再次进入 DLQ。

修复一条消息应先 dry-run，再显式执行，例如：

```bash
python3 scripts/replay_loader_dlq.py --message-id <sha256-id> --patch-json '{"missing_field":1}'
python3 scripts/replay_loader_dlq.py --message-id <sha256-id> --patch-json '{"missing_field":1}' --execute
```

`LoaderPoisonMessageDetected` 代表消息已安全隔离；`LoaderDlqPublishFailed` 更严重，表示 DLQ 未确认且源 offset 被刻意保留，需先恢复 Kafka/DLQ 再处理。

端到端新鲜度探针默认持续采集但关闭陈旧告警。原因是仅凭“最新业务时间很旧”无法区分正常静默期和上游断流。只有在业务确实承诺持续产数的时间窗口，才在 `.env` 中设置：

```dotenv
PIPELINE_FRESHNESS_MONITOR_ENABLED=true
PIPELINE_FRESHNESS_STALE_AFTER_SECONDS=300
PIPELINE_BUSINESS_TIMEZONE=Asia/Shanghai
```

随后重建 `pipeline-freshness-exporter`。`PIPELINE_BUSINESS_TIMEZONE` 用来把 Flink/MySQL 产生的无时区业务时间显式解释为本地墙上时间，避免 ClickHouse 运行在 UTC 时把数据误判为“来自未来”。Grafana 会展示行为 DWD、订单 DWD 和实时 ADS 的数据年龄、时间戳可用性与采集状态。

探针边界与告警语义：

- 这是低成本“哨兵”探针：每 15 秒只对三张代表性 ClickHouse 表执行一次最大业务时间聚合，不等价于逐 Topic、逐分区或逐记录的完整链路追踪。
- 三张表均为 `ReplacingMergeTree`，查询使用 `FINAL` 按逻辑最新版本计算，避免旧物理版本的较新时间戳掩盖真实陈旧；代价是查询时合并，因此没有扩展到所有大表。
- 数据年龄衡量的是 ClickHouse 中最新业务时间距当前时间的差值，不是单条记录的端到端处理耗时；结果依赖业务时钟和 `PIPELINE_BUSINESS_TIMEZONE` 配置正确。
- `PipelineDataStale` 只有在陈旧告警开关开启且最新一轮三项查询全部成功时才触发；查询失败由 `PipelineFreshnessCollectionFailed` 单独报告，避免同一故障双报。
- `pipeline-freshness-exporter` 是 Prometheus 依赖的常驻基础设施，因此 `ExporterDown` 始终启用，不受业务静默期开关影响；业务数据陈旧、无时间戳和采集失败告警仍受开关控制。
- 探针不能仅凭时间戳区分“正常无流量”和“上游断流”。生产环境应按业务日历维护告警窗口，或结合源端心跳、Kafka 分区水位及调度元数据判断。

## 2. 验收

```bash
bash scripts/verify_result.sh
python3 scripts/quality_report.py --output artifacts/latest_quality_report.md
```

验收重点：

- 行为/订单 DWD 与六类 ADS 非空
- 业务主键逻辑去重后无重复
- 非法业务数据进入 DLQ
- `UV <= PV`
- 严格漏斗满足 `view >= cart >= order >= pay`
- 支付/退款状态一致，历史商品价格命中 SCD2 版本
- SCD2 每个商品只有一个当前版本且有效区间不重叠

## 3. DELETE / tombstone 演练

整条链路和常驻 Loader 健康后执行：

```bash
python3 scripts/delete_tombstone_drill.py
```

脚本选择一张已进入 ClickHouse DWD 的订单，先记录 `ods_order_info` 各分区 end offset，再独立提交源表 `DELETE`；它要求先看到目标主键的 ODS `value=null`，再验证关联明细在 ClickHouse `FINAL` 口径下全部变为 `is_deleted=1`。随后默认恢复订单，并验证 ODS upsert 和 DWD 明细重新生效。异常中断时也会尽力恢复源订单；只有显式传入 `--keep-deleted` 才保留删除状态。

## 4. 故障恢复实验

```bash
bash scripts/fault_recovery_drill.sh
```

Windows PowerShell：

```powershell
.\scripts\fault_recovery_drill.ps1
```

脚本会在订单旅程生成期间杀掉 TaskManager，由本地 Compose 编排立即重新拉起节点，随后要求出现本次新增的 Checkpoint 恢复日志，验证九条作业从 checkpoint 恢复，再重新装载结果并执行质量检查。手动 `docker kill` 属于操作员行为，Docker 不保证仅凭 `restart` 策略自动拉起，所以脚本显式执行 `docker compose up -d flink-taskmanager`；生产环境中这一职责通常由 Kubernetes 等编排平台承担。面试材料应保留：

1. 故障前成功 Checkpoint
2. TaskManager 重启日志
3. Job 恢复后的状态
4. 重放后重复主键为 0 的查询结果
5. 恢复耗时

当前固定延迟重启策略为每 5 秒重试、最多 100 次，避免短时资源离线后仅因重试次数耗尽而进入终态；真正生产环境还应配合告警、自动扩缩容和高可用 JobManager。

## 5. 压测

```bash
bash scripts/benchmark_pipeline.sh 50000
```

Windows 参考实现会记录增量 Kafka offset、并发 ClickHouse 装载和验收结果：

```powershell
.\scripts\benchmark_pipeline.ps1 -Journeys 10000 -Orders 1000
```

至少测试三组：1 万、5 万、10 万 journeys。记录机器配置、事件条数、Kafka 分区、Flink 并行度、端到端耗时、吞吐和 Lag 峰值。没有实测前不要在简历填写吞吐量。

## 6. 手动集成 CI

GitHub Actions 的 push/PR 只执行快速单测和静态检查。需要云端完整证据时，在 `project-ci` 工作流手动运行 `integration-smoke`；它会启动核心数据面、提交 9 条作业、生成小批数据、执行 tombstone 删除/恢复和 24 项质量规则，成功上传质量报告，失败上传 Compose/Flink 诊断，最后清理 Runner 的临时卷。

## 7. Spark SQL 历史回补与对账

Spark 是 `batch` profile 下的一次性任务，不随 `docker compose up -d` 常驻。日期范围为左闭右开：

```powershell
.\scripts\run_spark_backfill.ps1 -StartDate 2026-08-11 -EndDate 2026-08-12
```

正常结果应输出 `mismatches=0`，并生成 `artifacts/spark_reconciliation_report.md`。任务默认遇到差异返回非零；排查时可暂用 `-AllowMismatch` 保存差异，但验收和简历证据必须使用 PASS 报告。相同日期重复执行后，可用 Spark SQL 读取 `data/spark-warehouse/ads_order_daily`，行数不应膨胀。

## 8. 正常停止

```bash
docker compose stop
```

需要删除本地 Demo 数据时才执行：

```bash
docker compose down -v
```
