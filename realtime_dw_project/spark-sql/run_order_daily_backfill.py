#!/usr/bin/env python3
"""Run a date-range Spark SQL backfill and reconcile it with realtime ADS."""

from __future__ import annotations

import base64
import datetime as dt
import json
import os
import pathlib
import sys
import urllib.parse
import urllib.request
from decimal import Decimal


BASE_DIR = pathlib.Path(__file__).resolve().parent
METRIC_COLUMNS = (
    "total_orders",
    "paid_orders",
    "cancelled_orders",
    "refunded_orders",
    "order_amount",
    "paid_amount",
    "refund_amount",
)


def parse_date(value: str, name: str) -> dt.date:
    try:
        parsed = dt.date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{name} must use YYYY-MM-DD: {value!r}") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"{name} must use zero-padded YYYY-MM-DD: {value!r}")
    return parsed


def validate_date_range(start_value: str, end_value: str) -> tuple[dt.date, dt.date]:
    start_date = parse_date(start_value, "BACKFILL_START_DATE")
    end_date = parse_date(end_value, "BACKFILL_END_DATE")
    if start_date >= end_date:
        raise ValueError("BACKFILL_END_DATE must be later than BACKFILL_START_DATE")
    return start_date, end_date


def build_source_queries(start_date: dt.date, end_date: dt.date) -> dict[str, str]:
    start = start_date.isoformat()
    end = end_date.isoformat()
    predicate = f"o.create_time >= '{start} 00:00:00' AND o.create_time < '{end} 00:00:00'"
    return {
        "order_info": (
            "(SELECT o.order_id, o.order_status, o.order_amount, o.channel, o.create_time "
            f"FROM order_info o WHERE {predicate}) AS order_info_range"
        ),
        "payment_info": (
            "(SELECT p.order_id, p.payment_status, p.payment_amount FROM payment_info p "
            f"INNER JOIN order_info o ON p.order_id = o.order_id WHERE {predicate}) AS payment_info_range"
        ),
        "refund_info": (
            "(SELECT r.order_id, r.refund_status, r.refund_amount FROM refund_info r "
            f"INNER JOIN order_info o ON r.order_id = o.order_id WHERE {predicate}) AS refund_info_range"
        ),
    }


def read_sql(path: pathlib.Path) -> str:
    sql = path.read_text(encoding="utf-8").strip().rstrip(";")
    if not sql:
        raise ValueError(f"SQL file is empty: {path}")
    return sql


def fetch_realtime_rows(
    url: str,
    user: str,
    password: str,
    start_date: dt.date,
    end_date: dt.date,
    timeout: float = 30.0,
) -> list[dict[str, object]]:
    query = f"""
        SELECT order_date, channel, total_orders, paid_orders, cancelled_orders,
               refunded_orders, order_amount, paid_amount, refund_amount
        FROM default.ads_order_daily FINAL
        WHERE is_deleted = 0
          AND order_date >= toDate('{start_date.isoformat()}')
          AND order_date < toDate('{end_date.isoformat()}')
        ORDER BY order_date, channel
        FORMAT JSONEachRow
    """.strip()
    endpoint = f"{url.rstrip('/')}?{urllib.parse.urlencode({'query': query})}"
    request = urllib.request.Request(endpoint)
    token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    request.add_header("Authorization", f"Basic {token}")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read().decode("utf-8")
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def write_markdown_report(
    path: pathlib.Path,
    start_date: dt.date,
    end_date: dt.date,
    spark_version: str,
    source_orders: int,
    offline_rows: int,
    realtime_rows: int,
    previous_partition_rows: int | None,
    persisted_partition_rows: int,
    mismatch_rows: list[object],
) -> None:
    status = "PASS" if not mismatch_rows else "FAIL"
    lines = [
        "# Spark SQL 离线回补与实时对账报告",
        "",
        f"> 生成时间（UTC）：{dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')}",
        "",
        "| 项目 | 结果 |",
        "| --- | ---: |",
        f"| Spark 版本 | {spark_version} |",
        f"| 回补范围 | `[{start_date}, {end_date})` |",
        f"| MySQL 源订单数 | {source_orders} |",
        f"| 离线日期×渠道行数 | {offline_rows} |",
        f"| 实时日期×渠道行数 | {realtime_rows} |",
        f"| 覆盖前该范围 Parquet 行数 | {previous_partition_rows if previous_partition_rows is not None else '首次运行'} |",
        f"| 覆盖后该范围 Parquet 行数 | {persisted_partition_rows} |",
        f"| 不一致行数 | {len(mismatch_rows)} |",
        f"| 验收 | **{status}** |",
        "",
        "离线结果由 MySQL 事务表经 Spark SQL 重算，按 `order_date` 动态覆盖 Parquet 分区；",
        "实时结果来自 ClickHouse `ads_order_daily FINAL`。日期范围采用左闭右开区间。",
    ]
    if mismatch_rows:
        lines.extend(["", "## 不一致样例（最多 20 行）", "", "```text"])
        lines.extend(str(row.asDict(recursive=True)) for row in mismatch_rows[:20])
        lines.append("```")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    try:
        start_date, end_date = validate_date_range(
            os.environ.get("BACKFILL_START_DATE", "1970-01-01"),
            os.environ.get("BACKFILL_END_DATE", "2100-01-01"),
        )
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.types import (
        DateType,
        DecimalType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    spark = (
        SparkSession.builder.appName("rtdw-order-daily-backfill")
        .config("spark.sql.session.timeZone", os.environ.get("BUSINESS_TIMEZONE", "Asia/Shanghai"))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    mysql_url = os.environ.get(
        "MYSQL_JDBC_URL",
        "jdbc:mysql://mysql:3306/ecommerce?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai",
    )
    jdbc_options = {
        "url": mysql_url,
        "user": os.environ.get("MYSQL_USER", "root"),
        "password": os.environ.get("MYSQL_PASSWORD", "root"),
        "driver": "com.mysql.cj.jdbc.Driver",
        "fetchsize": os.environ.get("SPARK_JDBC_FETCH_SIZE", "1000"),
    }

    try:
        for view_name, query in build_source_queries(start_date, end_date).items():
            (
                spark.read.format("jdbc")
                .options(**jdbc_options, dbtable=query)
                .load()
                .createOrReplaceTempView(view_name)
            )

        source_orders = spark.table("order_info").count()

        offline = spark.sql(read_sql(BASE_DIR / "order_daily_backfill.sql")).cache()
        offline_count = offline.count()
        if offline_count == 0:
            raise RuntimeError(f"no source orders found in [{start_date}, {end_date})")

        warehouse_root = pathlib.PurePosixPath(os.environ.get("SPARK_WAREHOUSE_ROOT", "/opt/rtdw/warehouse"))
        offline_path = str(warehouse_root / "ads_order_daily")
        previous_partition_rows = None
        if pathlib.Path(offline_path).exists():
            previous_partition_rows = (
                spark.read.parquet(offline_path)
                .where((F.col("order_date") >= F.lit(start_date)) & (F.col("order_date") < F.lit(end_date)))
                .count()
            )
        (
            offline.repartition("order_date")
            .write.mode("overwrite")
            .partitionBy("order_date")
            .parquet(offline_path)
        )
        persisted_partition_rows = (
            spark.read.parquet(offline_path)
            .where((F.col("order_date") >= F.lit(start_date)) & (F.col("order_date") < F.lit(end_date)))
            .count()
        )
        if persisted_partition_rows != offline_count:
            raise RuntimeError(
                f"partition overwrite verification failed: expected {offline_count}, got {persisted_partition_rows}"
            )
        offline.createOrReplaceTempView("offline_order_daily")

        realtime_raw = fetch_realtime_rows(
            os.environ.get("CLICKHOUSE_URL", "http://clickhouse:8123"),
            os.environ.get("CLICKHOUSE_USER", "default"),
            os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse"),
            start_date,
            end_date,
        )
        schema = StructType(
            [
                StructField("order_date", DateType(), False),
                StructField("channel", StringType(), False),
                *[StructField(name, LongType(), True) for name in METRIC_COLUMNS[:4]],
                *[StructField(name, DecimalType(22, 2), True) for name in METRIC_COLUMNS[4:]],
            ]
        )
        realtime_rows = [
            (
                dt.date.fromisoformat(str(row["order_date"])),
                str(row["channel"]),
                *[int(row.get(name) or 0) for name in METRIC_COLUMNS[:4]],
                *[Decimal(str(row.get(name) or 0)) for name in METRIC_COLUMNS[4:]],
            )
            for row in realtime_raw
        ]
        spark.createDataFrame(realtime_rows, schema).createOrReplaceTempView("realtime_order_daily")

        reconciliation = spark.sql(read_sql(BASE_DIR / "order_daily_reconciliation.sql")).cache()
        mismatches = reconciliation.where("NOT is_match").orderBy("order_date", "channel").collect()
        reconciliation_path = str(
            warehouse_root
            / "reconciliation"
            / "order_daily"
            / f"start_date={start_date.isoformat()}"
            / f"end_date={end_date.isoformat()}"
        )
        reconciliation.write.mode("overwrite").parquet(reconciliation_path)

        report_path = pathlib.Path(
            os.environ.get("SPARK_REPORT_PATH", "/opt/rtdw/artifacts/spark_reconciliation_report.md")
        )
        write_markdown_report(
            report_path,
            start_date,
            end_date,
            spark.version,
            source_orders,
            offline_count,
            len(realtime_rows),
            previous_partition_rows,
            persisted_partition_rows,
            mismatches,
        )
        print(
            f"Spark backfill complete: offline_rows={offline_count}, "
            f"realtime_rows={len(realtime_rows)}, mismatches={len(mismatches)}, report={report_path}"
        )
        if mismatches and os.environ.get("ALLOW_RECONCILIATION_MISMATCH", "false").lower() != "true":
            return 1
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
