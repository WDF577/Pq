import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).parents[1]
SHELL_SCRIPT = (PROJECT_ROOT / "scripts" / "run_demo.sh").read_text(encoding="utf-8")
POWERSHELL_SCRIPT = (PROJECT_ROOT / "scripts" / "run_demo.ps1").read_text(
    encoding="utf-8"
)


def assert_order(text, *needles):
    positions = [text.index(needle) for needle in needles]
    assert positions == sorted(positions), f"unexpected order for {needles}"


def test_shell_initializes_topics_and_tables_before_starting_the_loader():
    core_start = (
        "docker compose up -d --build \\\n"
        "  zookeeper kafka mysql clickhouse \\\n"
        "  flink-volume-init flink-jobmanager flink-taskmanager"
    )
    service_start = (
        "docker compose up -d --build \\\n"
        "  clickhouse-loader pipeline-freshness-exporter prometheus grafana"
    )

    assert core_start in SHELL_SCRIPT
    assert service_start in SHELL_SCRIPT
    assert_order(
        SHELL_SCRIPT,
        core_start,
        "bash scripts/create_kafka_topics.sh",
        "scripts/create_clickhouse_tables.sql",
        service_start,
        "bash scripts/run_flink_sql.sh",
        "bash scripts/verify_result.sh",
    )
    assert "python3 scripts/load_kafka_to_clickhouse.py" not in SHELL_SCRIPT
    assert "--group-id demo_loader" not in SHELL_SCRIPT
    assert "rtdw_clickhouse_loader" in SHELL_SCRIPT
    assert "rtdw_pipeline_freshness_exporter" in SHELL_SCRIPT


def test_powershell_initializes_topics_and_tables_before_starting_the_loader():
    core_start = (
        "docker compose up -d --build zookeeper kafka mysql clickhouse "
        "flink-volume-init flink-jobmanager flink-taskmanager"
    )
    service_start = (
        "docker compose up -d --build clickhouse-loader "
        "pipeline-freshness-exporter prometheus grafana"
    )

    assert core_start in POWERSHELL_SCRIPT
    assert service_start in POWERSHELL_SCRIPT
    assert_order(
        POWERSHELL_SCRIPT,
        core_start,
        'Write-Host "6. Create Kafka topics"',
        'Write-Host "7. Initialize ClickHouse tables"',
        service_start,
        'Write-Host "9. Submit behavior and order CDC Flink SQL jobs"',
        '& "$PSScriptRoot\\verify_result.ps1"',
    )
    assert "python scripts/load_kafka_to_clickhouse.py" not in POWERSHELL_SCRIPT
    assert "--group-id demo_loader" not in POWERSHELL_SCRIPT
    assert "rtdw_clickhouse_loader" in POWERSHELL_SCRIPT
    assert "rtdw_pipeline_freshness_exporter" in POWERSHELL_SCRIPT


def test_powershell_creates_dlq_and_reapplies_every_declared_cleanup_policy():
    assert '@{ Name = "clickhouse_loader_dlq"; Policy = "compact" }' in POWERSHELL_SCRIPT
    assert "kafka-configs --bootstrap-server kafka:29092" in POWERSHELL_SCRIPT
    assert '--alter --add-config "cleanup.policy=$($topic.Policy)"' in POWERSHELL_SCRIPT
    assert POWERSHELL_SCRIPT.index("kafka-topics --bootstrap-server") < POWERSHELL_SCRIPT.index(
        "kafka-configs --bootstrap-server"
    )


def test_shell_dlq_uses_pure_compaction_and_reconciles_existing_topic_policy():
    topic_script = (PROJECT_ROOT / "scripts" / "create_kafka_topics.sh").read_text(
        encoding="utf-8"
    )
    assert 'create_topic clickhouse_loader_dlq "compact"' in topic_script
    assert "kafka-configs" in topic_script
    assert '--add-config "cleanup.policy=$cleanup_policy"' in topic_script


def test_connector_downloads_are_checksum_verified_on_both_hosts():
    download_script = (PROJECT_ROOT / "scripts" / "download_connectors.sh").read_text(
        encoding="utf-8"
    )
    assert "sha256sum --check --status" in download_script
    assert download_script.count('download "https://repo1.maven.org/') == 7
    assert "Get-FileHash -Algorithm SHA256" in POWERSHELL_SCRIPT
    assert POWERSHELL_SCRIPT.count("Sha256 = ") == 7


def test_demo_step_numbers_are_contiguous_after_removing_batch_loader():
    shell_steps = [int(value) for value in re.findall(r'^echo "(\d+)\.', SHELL_SCRIPT, re.M)]
    powershell_steps = [
        int(value) for value in re.findall(r'^Write-Host "(\d+)\.', POWERSHELL_SCRIPT, re.M)
    ]

    assert shell_steps == list(range(1, 14))
    assert powershell_steps == list(range(1, 14))
