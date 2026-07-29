# 电商用户行为实时数仓项目报告

## 1. 项目背景

项目以电商用户浏览、加购、下单和支付行为为例，构建一个可在本地运行的实时数据处理 Demo。目标不是模拟生产集群规模，而是验证数据从事件产生到指标展示的完整过程，并能够解释每层粒度、口径和技术边界。

## 2. 技术架构

| 组件 | 版本 | 职责 |
| --- | --- | --- |
| Kafka | Confluent 7.6.1 | ODS、DWD 和 ADS 消息传输 |
| Flink SQL | 1.18 | 实时清洗、维表关联和窗口聚合 |
| MySQL | 8.0 | 商品、店铺、地区维表 |
| ClickHouse | 24.3 | DWD 明细、ADS 指标和告警 |
| Python | 3.x | 事件生成、批量装载、质量报告和告警 |
| Streamlit | - | 指标展示 |
| Docker Compose | - | 6 个本地服务编排 |

主链路：

```text
Python -> Kafka ODS -> Flink SQL -> Kafka DWD/ADS
                                -> Python Batch Loader -> ClickHouse -> Streamlit
                    MySQL dimensions -----^
```

## 3. 数据模型

### 3.1 DWD 事实表

`dwd_user_behavior` 的粒度是一条用户行为事件，业务键为 `event_id`。记录用户、商品、店铺、渠道、金额和事件时间，并补充商品名、品类、店铺名、省份和城市。

### 3.2 维表

- `dim_product(product_id, product_name, category_id, category_name, price)`
- `dim_shop(shop_id, shop_name, region_id)`
- `dim_region(region_id, province, city)`

维表通过 processing-time JDBC Temporal Join 关联。当前未实现 SCD2、CDC 和 Lookup Cache。

### 3.3 ADS 粒度

| 表 | 粒度 |
| --- | --- |
| `ads_realtime_overview` | 1 分钟窗口 |
| `ads_product_rank` | 5 分钟窗口 + 商品 |
| `ads_category_rank` | 5 分钟窗口 + 品类 |
| `ads_channel_funnel` | 1 分钟窗口 + 渠道 |

## 4. 实时计算

### 4.1 时间语义

Flink 使用事件时间，并设置 5 秒 Watermark。有限批次生成器按事件时间递增发送，并注入默认 3 秒乱序抖动，避免用“跨 2 小时完全随机乱序”制造与 Watermark 配置不匹配的迟到数据。1 分钟和 5 分钟 TUMBLE 窗口互不重叠，适合固定周期运营指标。

### 4.2 DWD 清洗

过滤规则：

- `event_id IS NOT NULL`
- `user_id IS NOT NULL`
- `event_type IN ('view','cart','order','pay')`

商品或店铺维度未命中时，LEFT JOIN 保留明细，由质量报告统计命中率。

### 4.3 指标口径

- PV：`view` 事件数
- UV：发生 `view` 的去重用户数
- 加购/下单/支付用户：对应行为的去重用户数
- 支付金额：`pay` 事件金额总和
- 商品/品类指标：5 分钟支付聚合
- 渠道指标：1 分钟各阶段去重用户数

商品表不包含 Flink TopN，Top 10 在看板侧选择。渠道指标不构成严格路径漏斗。

## 5. 存储装载

Flink 将 1 个 DWD 和 4 个 ADS 结果写入 Kafka。Python Loader 按目标表缓存记录，批量使用 ClickHouse HTTP `JSONEachRow` 写入；批次成功后同步提交 Kafka offset。

相较逐行写入：

- 减少 HTTP 请求次数
- 降低 ClickHouse 小批写入压力
- 避免自动提交导致消息未落库却已推进 offset

局限：写入成功、提交 offset 失败时仍可能重复，因此不是端到端 Exactly Once。

## 6. 可视化与告警

Streamlit 展示：

- 最新 1 分钟 PV、UV、阶段用户和支付金额
- 支付金额趋势
- 最新 5 分钟窗口商品 Top 10
- 最新 5 分钟窗口品类分布
- 最新 1 分钟窗口渠道阶段人数
- ClickHouse 告警记录

ADS 查询使用 `FINAL`，避免 ReplacingMergeTree 后台尚未合并时展示重复窗口版本。

## 7. 数据质量

质量脚本检查：

1. DWD 和 4 类 ADS 核心表非空
2. 空事件 ID
3. 非法事件类型
4. 重复事件 ID
5. 商品、店铺、地区维表命中率
6. `UV <= PV`

ODS 保存在 Kafka，ClickHouse 质量报告不估算 ODS 行数，也不计算没有真实来源的清洗率。

## 8. 实际问题与处理

### Python Kafka 客户端兼容

Python 3.12 环境下原客户端存在兼容问题，切换到 `confluent-kafka`，同时调整 Producer 和 Consumer API。

### Flink Slot 不足

第 5 个 Job 无法运行时，通过 Flink Web UI 和日志定位到可用 Slot 不足，调整 TaskManager Slot 后重新提交并验证 5 个 Job 状态。

### ClickHouse 返回类型

ClickHouse HTTP `JSONEachRow` 中数值字段可能以字符串进入 Pandas，导致 Plotly 渲染异常。Dashboard 查询层增加数值转换。

### 口径修正

早期 `COUNT(*)` 把全部行为统计为 PV。复核指标定义后改为只统计 `view`，并把低转化告警统一为 `pay_users / uv`。

### 事件时间顺序

早期生成器把过去 2 小时事件完全随机发送，而 Watermark 只允许 5 秒，导致大量窗口事件被判定为迟到数据。有限批次模式改为按事件时间递增发送并注入 3 秒抖动，使模拟数据与 Watermark 设计一致。

## 9. 验证方式

静态检查：

- Python `py_compile`
- Shell `bash -n`
- Docker Compose `config`
- 文档与 SQL 口径检索

运行验证：

- 6 个容器运行
- 5 个 Flink Job 正常
- Kafka 6 个 Topic
- ClickHouse 5 类核心结果有数据
- 质量报告全部必选项通过

## 10. 已知限制和后续演进

- 配置 Flink Checkpoint、状态后端和重启策略
- Kafka 增加分区、副本和监控
- 使用成熟 Connector 或幂等设计增强一致性
- 为业务事件增加 `order_id`、`session_id` 并实现严格漏斗
- 维表接入 CDC，并根据历史分析需求实现 SCD2
- 将指标口径、血缘和质量规则纳入元数据治理

## 11. 面试表述原则

该项目应被描述为“完整链路可运行的个人学习项目”，重点展示：

- 能说明事实粒度和指标口径
- 能解释各组件为什么存在
- 能从 Topic、Job 和表逐层排障
- 能识别 TopN、漏斗、维表版本和一致性边界

不应描述为生产高可用集群或已经具备严格业务交易语义。
