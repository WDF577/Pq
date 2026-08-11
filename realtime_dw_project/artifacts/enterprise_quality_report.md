# Data Quality Report

> Generated: 2026-08-11 21:14:34

## Scope

The ODS layer is stored in Kafka, so this report does not infer an ODS row count from DWD. It validates the ClickHouse DWD/ADS serving layer only.

## Volume Overview

| Layer | Table | Rows |
| --- | --- | ---: |
| DWD | dwd_user_behavior | 39,009 |
| DLQ | dwd_dirty_behavior | 825 |
| ADS | ads_realtime_overview | 307 |
| ADS | ads_product_rank | 2,541 |
| ADS | ads_category_rank | 479 |
| ADS | ads_channel_funnel | 55 |
| ADS | ads_realtime_alert | 0 |
| DWD | dwd_order_detail | 3,081 |
| DIM | dim_product_scd2 | 109 |
| ADS | ads_order_lifecycle | 5 |
| ADS | ads_order_daily | 5 |

## Dimension Join Quality

| Dimension | Hit rate |
| --- | ---: |
| Product | 100.00% |
| Shop | 100.00% |
| Region | 100.00% |

## Acceptance Checklist (24/24 PASS, 0 FAIL)

| Check | Result | Evidence |
| --- | --- | --- |
| DWD has data | PASS | rows=39,009 |
| event_id is not empty | PASS | invalid=0 |
| event_type is valid | PASS | invalid=0 |
| event_id is unique | PASS | duplicate keys=0 |
| product dimension hit rate | PASS | 100.00% |
| shop dimension hit rate | PASS | 100.00% |
| region dimension hit rate | PASS | 100.00% |
| overview output has data | PASS | rows=307 |
| product output has data | PASS | rows=2,541 |
| category output has data | PASS | rows=479 |
| channel output has data | PASS | rows=55 |
| UV does not exceed PV | PASS | invalid windows=0 |
| strict funnel stages are monotonic | PASS | invalid windows=0 |
| order DWD has data | PASS | rows=3,081 |
| order detail key is unique | PASS | duplicate keys=0 |
| order status is valid | PASS | invalid=0 |
| paid orders have successful payment | PASS | invalid=0 |
| refunded orders have valid refund | PASS | invalid=0 |
| historical product price matches | PASS | mismatch=0 |
| SCD2 has one current version | PASS | invalid products=0 |
| SCD2 validity ranges do not overlap | PASS | overlaps=0 |
| order lifecycle ADS has data | PASS | rows=5 |
| order daily ADS has data | PASS | rows=5 |
| order lifecycle metrics are consistent | PASS | invalid=0 |

## Interpretation Notes

- `pv` counts only `view` events; `uv` counts distinct users with a `view` event.
- Product/category tables are 5-minute payment aggregates. The dashboard selects Top 10.
- The journey generator carries session/order identifiers. Channel metrics count sessions that reached each stage in timestamp order within a 30-minute window.
- Acceptance queries use FINAL on ReplacingMergeTree tables so Kafka replay does not inflate logical business rows.
- Order CDC checks validate mutable status, payment/refund consistency and the event-time SCD2 product version selected for each detail row.
- The alert table is optional and is not included in the pass count.
