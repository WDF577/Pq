from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_prometheus_scrapes_loader_and_has_required_alerts():
    prometheus = (ROOT / "monitoring" / "prometheus.yml").read_text(encoding="utf-8")
    alerts = (ROOT / "monitoring" / "alert_rules.yml").read_text(encoding="utf-8")

    assert "job_name: clickhouse-loader" in prometheus
    assert 'targets: ["clickhouse-loader:9410"]' in prometheus
    assert "alert: LoaderDown" in alerts
    assert "alert: LoaderWriteStalled" in alerts
    assert "alert: LoaderPoisonMessageDetected" in alerts
    assert "alert: LoaderDlqPublishFailed" in alerts
    assert 'consumergroup="clickhouse_loader"' in alerts
    assert "clickhouse_loader_last_success_unixtime_seconds" in alerts
    assert "clickhouse_loader_start_unixtime_seconds" in alerts
    assert "clickhouse_loader_decode_failures_total" in alerts
    assert "clickhouse_loader_dlq_publish_failures_total" in alerts
