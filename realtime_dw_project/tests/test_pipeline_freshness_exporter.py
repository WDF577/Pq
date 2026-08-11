import importlib.util
import json
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "export_pipeline_freshness.py"
SPEC = importlib.util.spec_from_file_location("export_pipeline_freshness", MODULE_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class FakeClient:
    def __init__(self, values):
        self.values = iter(values)

    def query_scalar(self, _query):
        value = next(self.values)
        if isinstance(value, Exception):
            raise value
        return value


def test_successful_collection_exposes_age_and_control_metrics():
    collector = MODULE.FreshnessCollector(
        FakeClient([990, 980, 970]),
        enabled=True,
        stale_after_seconds=300,
        clock=lambda: 1000,
    )

    collector.collect()
    metrics = collector.render_metrics()

    assert "pipeline_freshness_monitor_enabled 1" in metrics
    assert "pipeline_freshness_stale_threshold_seconds 300" in metrics
    assert "pipeline_freshness_collection_success 1" in metrics
    assert "pipeline_freshness_collection_failures_total 0" in metrics
    assert "pipeline_freshness_last_collection_unixtime_seconds 1000.000000" in metrics
    assert "pipeline_freshness_last_success_unixtime_seconds 1000.000000" in metrics
    assert 'pipeline_data_age_seconds{pipeline="behavior_dwd",table="dwd_user_behavior"} 10.000000' in metrics
    assert 'pipeline_data_age_seconds{pipeline="order_dwd",table="dwd_order_detail"} 20.000000' in metrics
    assert 'pipeline_data_age_seconds{pipeline="behavior_ads",table="ads_realtime_overview"} 30.000000' in metrics


def test_partial_failure_is_counted_and_does_not_publish_missing_age():
    collector = MODULE.FreshnessCollector(
        FakeClient([990, RuntimeError("query failed"), 0]),
        enabled=False,
        stale_after_seconds=300,
        clock=lambda: 1000,
    )

    collector.collect()
    metrics = collector.render_metrics()

    assert "pipeline_freshness_monitor_enabled 0" in metrics
    assert "pipeline_freshness_collection_success 0" in metrics
    assert "pipeline_freshness_collection_failures_total 1" in metrics
    assert "pipeline_freshness_last_success_unixtime_seconds 0.000000" in metrics
    assert 'pipeline_data_available{pipeline="order_dwd",table="dwd_order_detail"} 0' in metrics
    assert 'pipeline_data_age_seconds{pipeline="order_dwd",table="dwd_order_detail"}' not in metrics
    assert 'pipeline_data_available{pipeline="behavior_ads",table="ads_realtime_overview"} 0' in metrics


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("true", True), ("YES", True), ("1", True), ("false", False), ("off", False), ("0", False)],
)
def test_parse_bool(raw, expected):
    assert MODULE.parse_bool(raw) is expected


def test_invalid_boolean_is_rejected():
    with pytest.raises(ValueError, match="invalid boolean"):
        MODULE.parse_bool("maybe")


def test_queries_cover_behavior_order_and_ads_business_timestamps():
    targets = {target.table: " ".join(target.query.split()) for target in MODULE.TARGETS}

    assert "max(event_ts)" in targets["dwd_user_behavior"]
    assert "order_update_time" in targets["dwd_order_detail"]
    assert "payment_time" in targets["dwd_order_detail"]
    assert "refund_time" in targets["dwd_order_detail"]
    assert "is_deleted = 0" in targets["dwd_order_detail"]
    assert "max(window_end)" in targets["ads_realtime_overview"]
    assert all("Asia/Shanghai" in query for query in targets.values())
    assert "FROM default.dwd_user_behavior FINAL" in targets["dwd_user_behavior"]
    assert "FROM default.dwd_order_detail FINAL WHERE" in targets["dwd_order_detail"]
    assert "FROM default.ads_realtime_overview FINAL" in targets["ads_realtime_overview"]


def test_business_timezone_is_validated_before_building_sql():
    with pytest.raises(ValueError, match="invalid PIPELINE_BUSINESS_TIMEZONE"):
        MODULE.build_targets("Asia/Shanghai'; DROP TABLE x; --")


def test_freshness_service_monitoring_alerts_and_dashboard_are_wired():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    prometheus = (ROOT / "monitoring" / "prometheus.yml").read_text(encoding="utf-8")
    alerts = (ROOT / "monitoring" / "alert_rules.yml").read_text(encoding="utf-8")
    dockerfile = (ROOT / "docker" / "freshness-exporter" / "Dockerfile").read_text(encoding="utf-8")
    dashboard = json.loads(
        (ROOT / "monitoring" / "grafana" / "dashboards" / "realtime-dw-overview.json").read_text(
            encoding="utf-8"
        )
    )

    assert "pipeline-freshness-exporter:" in compose
    assert "PIPELINE_FRESHNESS_MONITOR_ENABLED" in compose
    assert "PIPELINE_BUSINESS_TIMEZONE" in compose
    assert "healthcheck:" in compose
    assert "job_name: pipeline-freshness-exporter" in prometheus
    assert 'targets: ["pipeline-freshness-exporter:9420"]' in prometheus
    assert "alert: PipelineDataStale" in alerts
    assert "alert: ExporterDown" in alerts
    assert "pipeline_freshness_monitor_enabled" in alerts
    stale_alert = alerts.split("- alert: PipelineDataStale", 1)[1].split("- alert:", 1)[0]
    exporter_down_alert = alerts.split("- alert: ExporterDown", 1)[1]
    assert "pipeline_freshness_collection_success == 1" in stale_alert
    assert "pipeline_freshness_monitor_enabled == 1" in stale_alert
    assert 'expr: up{job="pipeline-freshness-exporter"} == 0' in exporter_down_alert
    assert "max_over_time" not in exporter_down_alert
    assert "pipeline_freshness_monitor_enabled" not in exporter_down_alert
    assert "USER freshness" in dockerfile
    assert any(
        "pipeline_data_age_seconds" in target.get("expr", "")
        for panel in dashboard["panels"]
        for target in panel.get("targets", [])
    )
