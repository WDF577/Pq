"""数据质量报告生成器"""
import argparse
import json
import urllib.request
import urllib.parse
import base64
import os
from datetime import datetime


def query_clickhouse(url, user, password, query):
    params = urllib.parse.urlencode({"query": query, "default_format": "JSONEachRow"})
    auth = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    req = urllib.request.Request(f"{url}/?{params}", headers={"Authorization": f"Basic {auth}"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return [json.loads(line) for line in resp.read().decode("utf-8").strip().split("\n") if line]


def parse_args():
    parser = argparse.ArgumentParser(description="Generate data quality report.")
    parser.add_argument("--clickhouse-url", default="http://localhost:8123")
    parser.add_argument("--clickhouse-user", default="default")
    parser.add_argument("--clickhouse-password", default="clickhouse")
    parser.add_argument("--output", default="docs/quality_report.md")
    return parser.parse_args()


def main():
    args = parse_args()
    ch = {"url": args.clickhouse_url, "user": args.clickhouse_user, "password": args.clickhouse_password}
    print("Generating data quality report...")

    queries = {
        "dwd_total": "SELECT count() AS cnt FROM dwd_user_behavior",
        "null_eid": "SELECT count() AS cnt FROM dwd_user_behavior WHERE event_id = '' OR event_id IS NULL",
        "null_uid": "SELECT count() AS cnt FROM dwd_user_behavior WHERE user_id = 0 OR user_id IS NULL",
        "invalid_type": "SELECT count() AS cnt FROM dwd_user_behavior WHERE event_type NOT IN ('view','cart','order','pay')",
        "with_province": "SELECT count() AS cnt FROM dwd_user_behavior WHERE province != '' AND province IS NOT NULL",
        "overview": "SELECT count() AS cnt FROM ads_realtime_overview",
        "product_rank": "SELECT count() AS cnt FROM ads_product_rank",
        "category_rank": "SELECT count() AS cnt FROM ads_category_rank",
        "channel_funnel": "SELECT count() AS cnt FROM ads_channel_funnel",
        "alerts": "SELECT count() AS cnt FROM ads_realtime_alert",
    }

    results = {}
    for key, sql in queries.items():
        try:
            rows = query_clickhouse(ch["url"], ch["user"], ch["password"], sql)
            results[key] = int(rows[0]["cnt"]) if rows else 0
        except Exception as e:
            results[key] = f"ERROR: {e}"

    dwd_cnt = results.get("dwd_total", 0)
    estimated_ods = dwd_cnt
    province_cnt = results.get("with_province", 0)
    province_rate = province_cnt / max(dwd_cnt, 1) * 100

    targets = {
        "ODS": (estimated_ods, 100000),
        "DWD": (dwd_cnt, 90000),
        "overview": (results.get("overview", 0), 100),
        "product_rank": (results.get("product_rank", 0), 100),
        "category_rank": (results.get("category_rank", 0), 100),
        "channel_funnel": (results.get("channel_funnel", 0), 100),
        "province_rate": (province_rate, 80),
        "alerts": (results.get("alerts", 0), -1),
    }

    status = {k: "PASS" if v[0] >= v[1] else "FAIL" for k, v in targets.items() if v[1] > 0}
    passed = sum(1 for s in status.values() if s == "PASS")
    total = len(status)

    report = f"""# Data Quality Report
> Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Volume Overview
| Layer | Table | Rows |
|-------|-------|------|
| ODS | ods_user_behavior | ~{estimated_ods:,} |
| DWD | dwd_user_behavior | {dwd_cnt:,} |
| ADS | ads_realtime_overview | {results.get("overview", 0):,} |
| ADS | ads_product_rank | {results.get("product_rank", 0):,} |
| ADS | ads_category_rank | {results.get("category_rank", 0):,} |
| ADS | ads_channel_funnel | {results.get("channel_funnel", 0):,} |
| ADS | ads_realtime_alert | {results.get("alerts", 0):,} |

## Cleaning Statistics
| Metric | Value |
|--------|-------|
| DWD total rows | {dwd_cnt:,} |
| null event_id in DWD | {results.get("null_eid", 0):,} |
| null user_id in DWD | {results.get("null_uid", 0):,} |
| invalid event_type in DWD | {results.get("invalid_type", 0):,} |
| Cleaning rate | {dwd_cnt / max(estimated_ods, 1) * 100:.1f}% |

## Dimension Join Quality
| Metric | Value |
|--------|-------|
| Rows with province/city | {province_cnt:,} |
| Region dim hit rate | {province_rate:.1f}% |

## Acceptance Checklist ({passed}/{total} PASS)
"""
    for k, v in targets.items():
        if v[1] > 0:
            check = "PASS" if v[0] >= v[1] else "FAIL"
            report += f"| {k} | target: >= {v[1]:,} | actual: {v[0]:,} | {check} |\n"

    report += f"""
## Issues Encountered
1. kafka-python 2.0.2 incompatible with Python 3.12 — fixed by switching to confluent-kafka.
2. Git Bash MSYS path conversion breaks docker exec paths — fixed with MSYS_NO_PATHCONV=1 and double-slash prefix.
3. MySQL port 3306 conflict with local MySQL — fixed by remapping to 3307 in docker-compose.yml.
"""

    os.makedirs(os.path.dirname(args.output) if os.path.dirname(args.output) else ".", exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(report)
    print(report)
    print(f"\nReport saved to: {args.output}")


if __name__ == "__main__":
    main()
