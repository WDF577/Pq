# Data Quality Report

> Generated: 2026-07-29 18:54:45

## Scope

The ODS layer is stored in Kafka, so this report does not infer an ODS row count from DWD. It validates the ClickHouse DWD/ADS serving layer only.

## Volume Overview

| Layer | Table | Rows |
| --- | --- | ---: |
| DWD | dwd_user_behavior | 98,020 |
| ADS | ads_realtime_overview | 120 |
| ADS | ads_product_rank | 2,082 |
| ADS | ads_category_rank | 259 |
| ADS | ads_channel_funnel | 600 |
| ADS | ads_realtime_alert | 0 |

## Dimension Join Quality

| Dimension | Hit rate |
| --- | ---: |
| Product | 99.02% |
| Shop | 99.52% |
| Region | 99.52% |

## Acceptance Checklist (12/12 PASS, 0 FAIL)

| Check | Result | Evidence |
| --- | --- | --- |
| DWD has data | PASS | rows=98,020 |
| event_id is not empty | PASS | invalid=0 |
| event_type is valid | PASS | invalid=0 |
| event_id is unique | PASS | duplicate keys=0 |
| product dimension hit rate | PASS | 99.02% |
| shop dimension hit rate | PASS | 99.52% |
| region dimension hit rate | PASS | 99.52% |
| overview output has data | PASS | rows=120 |
| product output has data | PASS | rows=2,082 |
| category output has data | PASS | rows=259 |
| channel output has data | PASS | rows=600 |
| UV does not exceed PV | PASS | invalid windows=0 |

## Interpretation Notes

- `pv` counts only `view` events; `uv` counts distinct users with a `view` event.
- Product/category tables are 5-minute payment aggregates. The dashboard selects Top 10.
- Channel output compares distinct users at each stage. Because mock events do not carry an order/session path, it is not a strict user-path funnel.
- The alert table is optional and is not included in the pass count.
