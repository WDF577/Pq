import datetime as dt
import importlib.util
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "spark-sql" / "run_order_daily_backfill.py"
SPEC = importlib.util.spec_from_file_location("spark_backfill", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def test_date_range_is_half_open_and_strict():
    assert MODULE.validate_date_range("2026-08-11", "2026-08-12") == (
        dt.date(2026, 8, 11),
        dt.date(2026, 8, 12),
    )
    with pytest.raises(ValueError):
        MODULE.validate_date_range("2026-8-11", "2026-08-12")
    with pytest.raises(ValueError):
        MODULE.validate_date_range("2026-08-12", "2026-08-12")


def test_source_queries_push_date_predicate_into_mysql():
    queries = MODULE.build_source_queries(dt.date(2026, 8, 11), dt.date(2026, 8, 13))
    assert set(queries) == {"order_info", "payment_info", "refund_info"}
    for query in queries.values():
        assert "o.create_time >= '2026-08-11 00:00:00'" in query
        assert "o.create_time < '2026-08-13 00:00:00'" in query
    assert "INNER JOIN order_info" in queries["payment_info"]
    assert "INNER JOIN order_info" in queries["refund_info"]


def test_business_logic_stays_in_spark_sql_and_collapses_to_order_grain():
    backfill_sql = (ROOT / "spark-sql" / "order_daily_backfill.sql").read_text(encoding="utf-8")
    reconcile_sql = (ROOT / "spark-sql" / "order_daily_reconciliation.sql").read_text(encoding="utf-8")
    assert "order_current AS" in backfill_sql
    assert "GROUP BY order_date, channel" in backfill_sql
    assert "order_status IN ('PAID', 'REFUNDED')" in backfill_sql
    assert "FULL OUTER JOIN realtime_order_daily" in reconcile_sql
    assert "THEN true ELSE false END AS is_match" in reconcile_sql


def test_compose_declares_one_shot_spark_profile_and_partition_storage():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "spark-backfill:" in compose
    assert 'profiles: ["batch"]' in compose
    assert "apache/spark:3.5.9-scala2.12-java17-python3-ubuntu" in compose
    assert "./data/spark-warehouse:/opt/rtdw/warehouse" in compose
    assert "mysql-connector-j-8.3.0.jar" in compose
    assert "SPARK_MYSQL_USER:-spark_batch" in compose
    assert "MYSQL_USER: root" not in compose


def test_spark_mysql_account_has_table_scoped_read_only_grants():
    ddl = (ROOT / "scripts" / "create_mysql_business_tables.sql").read_text(encoding="utf-8")
    assert "CREATE USER IF NOT EXISTS 'spark_batch'@'%'" in ddl
    assert "GRANT SELECT ON ecommerce.order_info TO 'spark_batch'@'%'" in ddl
    assert "GRANT SELECT ON ecommerce.payment_info TO 'spark_batch'@'%'" in ddl
    assert "GRANT SELECT ON ecommerce.refund_info TO 'spark_batch'@'%'" in ddl
    assert "GRANT ALL" not in "\n".join(
        line for line in ddl.splitlines() if "spark_batch" in line
    )
