# 数据模型与指标粒度

## 1. 业务过程

本项目围绕“用户对商品发生一次行为”建模。行为类型包括 `view`、`cart`、`order` 和 `pay`。当前模拟器随机生成行为，适合验证实时技术链路，不代表真实交易状态机。

## 2. DWD 事实粒度

`dwd_user_behavior` 的粒度是：

> 一个用户在一个事件时间点，对一个商品发生的一次行为。

业务键使用 `event_id`。`user_id`、`product_id`、`shop_id` 是维度外键，`amount` 是金额度量。

| 字段组 | 字段 |
| --- | --- |
| 业务键 | `event_id` |
| 维度外键 | `user_id`、`product_id`、`shop_id` |
| 退化维度 | `event_type`、`channel` |
| 度量 | `amount` |
| 时间 | `event_ts` |
| 维度属性快照 | 商品名、品类、店铺名、省份、城市 |

## 3. 维度模型

```text
                    dim_product
                         |
                         | product_id
                         |
dim_region <- dim_shop <- dwd_user_behavior
 region_id    shop_id
```

`dim_shop.region_id -> dim_region.region_id` 是雪花化关系。为了便于分析，DWD 输出同时携带常用维度属性。

当前维表是静态 MySQL 表，Temporal Join 读取处理时刻可见值。项目没有 SCD2，因此不能回答“事件发生时商品属于哪个历史品类”这类历史追溯问题。

## 4. ADS 粒度

| 表 | 粒度 |
| --- | --- |
| `ads_realtime_overview` | 1 分钟窗口一行 |
| `ads_product_rank` | 5 分钟窗口 + 商品一行 |
| `ads_category_rank` | 5 分钟窗口 + 品类一行 |
| `ads_channel_funnel` | 1 分钟窗口 + 渠道一行 |
| `ads_realtime_alert` | 一条被触发的告警一行 |

表名 `ads_product_rank` 沿用早期命名，但表内保存的是“商品窗口聚合”，不含排名字段。Top 10 在看板查询层计算。

## 5. 指标定义

| 指标 | 公式 | 可加性 |
| --- | --- | --- |
| PV | `SUM(event_type='view')` | 可按不重叠窗口求和 |
| UV | `COUNT(DISTINCT view user_id)` | 跨窗口不可直接求和 |
| 支付人数 | `COUNT(DISTINCT pay user_id)` | 跨窗口不可直接求和 |
| 支付金额 | `SUM(pay amount)` | 可按不重叠窗口求和 |
| 商品支付次数 | 商品维度下 `COUNT(pay event)` | 可按不重叠窗口求和 |

## 6. 渠道阶段指标的限制

`view_users`、`cart_users`、`order_users`、`pay_users` 分别是同一窗口和渠道内的去重人数。可以计算阶段人数比率，但不能证明分子用户属于分母用户集合。

要实现严格漏斗，需要：

1. 增加 `session_id` 或 `order_id`
2. 保证事件存在可追踪的业务序列
3. 按用户/会话做状态或序列匹配
4. 明确跨窗口归因规则

面试时可把这点作为“我识别到的模型边界和下一步改进”。
