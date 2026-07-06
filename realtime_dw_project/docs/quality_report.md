# Data Quality Report
> Generated: 2026-07-06 16:24:06

## Volume Overview
| Layer | Table | Rows |
|-------|-------|------|
| ODS | ods_user_behavior | ~249,980 |
| DWD | dwd_user_behavior | 249,980 |
| ADS | ads_realtime_overview | 93 |
| ADS | ads_product_rank | 254 |
| ADS | ads_category_rank | 60 |
| ADS | ads_channel_funnel | 398 |
| ADS | ads_realtime_alert | 3 |

## Cleaning Statistics
| Metric | Value |
|--------|-------|
| DWD total rows | 249,980 |
| null event_id in DWD | 0 |
| null user_id in DWD | 0 |
| invalid event_type in DWD | 0 |
| Cleaning rate | 100.0% |

## Dimension Join Quality
| Metric | Value |
|--------|-------|
| Rows with province/city | 249,038 |
| Region dim hit rate | 99.6% |

## Acceptance Checklist (5/7 PASS)
| ODS | target: >= 100,000 | actual: 249,980 | PASS |
| DWD | target: >= 90,000 | actual: 249,980 | PASS |
| overview | target: >= 100 | actual: 93 | FAIL |
| product_rank | target: >= 100 | actual: 254 | PASS |
| category_rank | target: >= 100 | actual: 60 | FAIL |
| channel_funnel | target: >= 100 | actual: 398 | PASS |
| province_rate | target: >= 80 | actual: 99.6231698535883 | PASS |

## Issues Encountered
1. kafka-python 2.0.2 incompatible with Python 3.12 — fixed by switching to confluent-kafka.
2. Git Bash MSYS path conversion breaks docker exec paths — fixed with MSYS_NO_PATHCONV=1 and double-slash prefix.
3. MySQL port 3306 conflict with local MySQL — fixed by remapping to 3307 in docker-compose.yml.
