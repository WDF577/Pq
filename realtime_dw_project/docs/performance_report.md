# 性能与故障恢复实验报告

> 实测日期：2026-08-11。以下数字只代表这台电脑上的单机 Docker 实验，不是生产容量承诺。

## 测试环境

| 项目 | 配置 |
| --- | --- |
| CPU | 13th Gen Intel Core i9-13900HX |
| 主机内存 | 15.7 GB |
| Kafka | 单 Broker、每业务 Topic 3 分区、单副本 |
| Flink | 1 JobManager、1 TaskManager、16 Slot、9 条 Job |
| 状态与恢复 | Embedded RocksDB、10 秒 Checkpoint、本地持久化卷 |
| 数据服务 | MySQL 8.0、ClickHouse 24.3 |

## 一万旅程 + 一千订单实测

运行命令：

```powershell
.\scripts\benchmark_pipeline.ps1 -Journeys 10000 -Orders 1000 -IdleTimeout 20
```

| 指标 | 结果 |
| --- | ---: |
| 行为旅程 | 10,000 |
| 生成行为事件 | 22,663 |
| 可变订单 | 1,000 |
| DWD 行为增量输出 | 22,497 |
| DWD 订单 Changelog 增量输出 | 6,443 |
| 生产开始至 DWD offset 连续稳定 | 55.01 秒 |
| 两类 DWD 综合输出速率 | 526.09 条/秒 |
| 压测后快速验收 | 21/21 PASS |
| 历史价格不匹配 | 0 |

压测时 Loader 以 `latest` offset 在生产前启动，并发装载 ClickHouse；脚本以 DWD Topic 的增量 end offset 连续三个轮询不再变化作为 Flink 本轮处理完成条件。该速率包含行为清洗、窗口链路共享资源、订单 CDC、多表 Changelog Join 和本机 Docker 开销，不等同于单算子极限吞吐。

原始报告：`artifacts/benchmark_20260811_153602.md`。

## TaskManager 故障恢复实测

运行 1,000 条旅程期间执行 `docker kill rtdw_flink_taskmanager`，随后由 Compose 重新拉起 TaskManager。脚本要求出现本次新增的 Checkpoint 恢复日志，再等待 9 条 Job 的全部 Task 进入 RUNNING。

| 指标 | 结果 |
| --- | ---: |
| 故障 | Kill TaskManager |
| 恢复到 9 条 Job 全部健康 | 26.36 秒 |
| 本次新增恢复日志 | 13 行 |
| 重放 Kafka 输出 | 85,647 条 |
| 行为重复业务键 | 0 |
| 订单明细重复业务键 | 0 |
| 历史价格不匹配 | 0 |
| 恢复后快速验收 | 21/21 PASS |

原始报告：`artifacts/fault_recovery_20260811_153947.md`。

## 最终数据质量结果

最终报告为 `artifacts/final_quality_report.md`：

- 35,998 条行为 DWD。
- 3,005 条订单明细 DWD。
- 106 条 SCD2 商品版本。
- 24/24 质量规则通过。
- 行为、订单业务键重复均为 0。
- 支付/退款不一致、SCD2 区间重叠、历史价格不匹配均为 0。

## 结论与瓶颈

- 当前 9 条独立 Job 共享一个 TaskManager；订单多表 Changelog Join 和 30 分钟漏斗是状态较重的部分。
- 单 Broker、单 TaskManager 和 Docker Desktop 资源竞争限制吞吐，结果不能外推为集群容量。
- Loader 重放全部历史 Topic 时物理写入量较大，但业务键最终结果保持幂等。
- 下一步生产化压测应增加 5 万、10 万旅程档位，记录各 Job 的 Kafka Lag 峰值、反压、Checkpoint P95、状态大小、CPU 和内存，并进行扩并行度前后对比。
