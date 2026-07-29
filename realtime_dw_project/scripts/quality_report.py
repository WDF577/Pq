"""Generate a data-quality report from the ClickHouse serving layer."""

import argparse
import base64
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime


def query_clickhouse(url, user, password, query):
    params = urllib.parse.urlencode({"query": query, "default_format": "JSONEachRow"})
    auth = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    request = urllib.request.Request(
        f"{url}/?{params}",
        headers={"Authorization": f"Basic {auth}"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = response.read().decode("utf-8").strip()
        return [json.loads(line) for line in payload.splitlines() if line]


def scalar(url, user, password, query):
    rows = query_clickhouse(url, user, password, query)
    return int(rows[0]["value"]) if rows else 0


def parse_args():
    parser = argparse.ArgumentParser(description="Generate a ClickHouse data-quality report.")
    parser.add_argument("--clickhouse-url", default="http://localhost:8123")
    parser.add_argument("--clickhouse-user", default="default")
    parser.add_argument("--clickhouse-password", default="clickhouse")
    parser.add_argument("--output", default="docs/quality_report.md")
    return parser.parse_args()


def main():
    args = parse_args()
    ch = (args.clickhouse_url, args.clickhouse_user, args.clickhouse_password)

    queries = {
        "dwd_total": "SELECT count() AS value FROM dwd_user_behavior",
        "empty_event_id": (
            "SELECT count() AS value FROM dwd_user_behavior WHERE event_id = ''"
        ),
        "invalid_event_type": (
            "SELECT count() AS value FROM dwd_user_behavior "
            "WHERE event_type NOT IN ('view','cart','order','pay')"
        ),
        "duplicate_event_keys": (
            "SELECT count() AS value FROM ("
            "SELECT event_id FROM dwd_user_behavior "
            "GROUP BY event_id HAVING count() > 1)"
        ),
        "product_dim_hits": (
            "SELECT count() AS value FROM dwd_user_behavior WHERE product_name != ''"
        ),
        "shop_dim_hits": (
            "SELECT count() AS value FROM dwd_user_behavior WHERE shop_name != ''"
        ),
        "region_dim_hits": (
            "SELECT count() AS value FROM dwd_user_behavior WHERE province != ''"
        ),
        "overview_rows": "SELECT count() AS value FROM ads_realtime_overview",
        "product_rows": "SELECT count() AS value FROM ads_product_rank",
        "category_rows": "SELECT count() AS value FROM ads_category_rank",
        "channel_rows": "SELECT count() AS value FROM ads_channel_funnel",
        "invalid_pv_uv": (
            "SELECT count() AS value FROM ads_realtime_overview WHERE uv > pv"
        ),
        "alert_rows": "SELECT count() AS value FROM ads_realtime_alert",
    }

    results = {}
    errors = {}
    for name, query in queries.items():
        try:
            results[name] = scalar(*ch, query)
        except Exception as exc:
            results[name] = 0
            errors[name] = str(exc)

    dwd_total = results["dwd_total"]

    def hit_rate(hit_count):
        return hit_count / dwd_total * 100 if dwd_total else 0.0

    product_hit_rate = hit_rate(results["product_dim_hits"])
    shop_hit_rate = hit_rate(results["shop_dim_hits"])
    region_hit_rate = hit_rate(results["region_dim_hits"])

    checks = [
        ("DWD has data", dwd_total > 0, f"rows={dwd_total:,}"),
        (
            "event_id is not empty",
            results["empty_event_id"] == 0,
            f"invalid={results['empty_event_id']:,}",
        ),
        (
            "event_type is valid",
            results["invalid_event_type"] == 0,
            f"invalid={results['invalid_event_type']:,}",
        ),
        (
            "event_id is unique",
            results["duplicate_event_keys"] == 0,
            f"duplicate keys={results['duplicate_event_keys']:,}",
        ),
        (
            "product dimension hit rate",
            product_hit_rate >= 95,
            f"{product_hit_rate:.2f}%",
        ),
        (
            "shop dimension hit rate",
            shop_hit_rate >= 95,
            f"{shop_hit_rate:.2f}%",
        ),
        (
            "region dimension hit rate",
            region_hit_rate >= 95,
            f"{region_hit_rate:.2f}%",
        ),
        (
            "overview output has data",
            results["overview_rows"] > 0,
            f"rows={results['overview_rows']:,}",
        ),
        (
            "product output has data",
            results["product_rows"] > 0,
            f"rows={results['product_rows']:,}",
        ),
        (
            "category output has data",
            results["category_rows"] > 0,
            f"rows={results['category_rows']:,}",
        ),
        (
            "channel output has data",
            results["channel_rows"] > 0,
            f"rows={results['channel_rows']:,}",
        ),
        (
            "UV does not exceed PV",
            results["invalid_pv_uv"] == 0,
            f"invalid windows={results['invalid_pv_uv']:,}",
        ),
    ]

    passed = sum(1 for _, success, _ in checks if success)
    failed = len(checks) - passed
    report_lines = [
        "# Data Quality Report",
        "",
        f"> Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## Scope",
        "",
        "The ODS layer is stored in Kafka, so this report does not infer an ODS row count "
        "from DWD. It validates the ClickHouse DWD/ADS serving layer only.",
        "",
        "## Volume Overview",
        "",
        "| Layer | Table | Rows |",
        "| --- | --- | ---: |",
        f"| DWD | dwd_user_behavior | {dwd_total:,} |",
        f"| ADS | ads_realtime_overview | {results['overview_rows']:,} |",
        f"| ADS | ads_product_rank | {results['product_rows']:,} |",
        f"| ADS | ads_category_rank | {results['category_rows']:,} |",
        f"| ADS | ads_channel_funnel | {results['channel_rows']:,} |",
        f"| ADS | ads_realtime_alert | {results['alert_rows']:,} |",
        "",
        "## Dimension Join Quality",
        "",
        "| Dimension | Hit rate |",
        "| --- | ---: |",
        f"| Product | {product_hit_rate:.2f}% |",
        f"| Shop | {shop_hit_rate:.2f}% |",
        f"| Region | {region_hit_rate:.2f}% |",
        "",
        f"## Acceptance Checklist ({passed}/{len(checks)} PASS, {failed} FAIL)",
        "",
        "| Check | Result | Evidence |",
        "| --- | --- | --- |",
    ]
    for name, success, evidence in checks:
        report_lines.append(f"| {name} | {'PASS' if success else 'FAIL'} | {evidence} |")

    if errors:
        report_lines.extend(["", "## Query Errors", ""])
        for name, error in errors.items():
            report_lines.append(f"- `{name}`: {error}")

    report_lines.extend(
        [
            "",
            "## Interpretation Notes",
            "",
            "- `pv` counts only `view` events; `uv` counts distinct users with a `view` event.",
            "- Product/category tables are 5-minute payment aggregates. The dashboard selects Top 10.",
            "- Channel output compares distinct users at each stage. Because mock events do not "
            "carry an order/session path, it is not a strict user-path funnel.",
            "- The alert table is optional and is not included in the pass count.",
            "",
        ]
    )

    output_dir = os.path.dirname(args.output)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    report = "\n".join(report_lines)
    with open(args.output, "w", encoding="utf-8") as file:
        file.write(report)
    print(report)
    print(f"\nReport saved to: {args.output}")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
