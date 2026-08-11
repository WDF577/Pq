# Docker Compose writes normal progress to native stderr on Windows. We check
# native exit codes explicitly so those progress lines are not terminating.
$ErrorActionPreference = "Continue"

$projectDir = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $projectDir

function Assert-LastExitCode {
    param([Parameter(Mandatory)][string]$Step)
    if ($LASTEXITCODE -ne 0) { throw "$Step failed with exit code $LASTEXITCODE." }
}

function Wait-Until {
    param(
        [Parameter(Mandatory)][scriptblock]$Condition,
        [Parameter(Mandatory)][string]$Label,
        [int]$TimeoutSeconds = 180
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try { if (& $Condition) { return } } catch { }
        Start-Sleep -Seconds 3
    } while ((Get-Date) -lt $deadline)
    throw "Timed out waiting for $Label."
}

function Send-FileToContainer {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string[]]$DockerArguments
    )
    Get-Content -Raw -Encoding utf8 $Path | & docker @DockerArguments
    Assert-LastExitCode "Loading $Path"
}

function Submit-FlinkSql {
    param(
        [Parameter(Mandatory)][string]$Label,
        [Parameter(Mandatory)][string[]]$Paths
    )
    $sql = ($Paths | ForEach-Object { Get-Content -Raw -Encoding utf8 $_ }) -join "`n"
    $output = @($sql | docker exec -i rtdw_flink_jobmanager /opt/flink/bin/sql-client.sh 2>&1)
    $nativeExitCode = $LASTEXITCODE
    $outputText = ($output | ForEach-Object { "$_" }) -join "`n"
    if ($outputText) { Write-Host $outputText }
    # Flink SQL Client may return native exit code 0 even after printing [ERROR].
    if ($nativeExitCode -ne 0 -or $outputText -match "(?m)^\[ERROR\]") {
        throw "$Label failed (native exit code $nativeExitCode)."
    }
}

Write-Host "== Realtime data warehouse Windows demo =="
Write-Host "Warning: this resets the local Docker demo volumes."

Write-Host "1. Check Python dependencies"
python -c "import confluent_kafka, mysql.connector"
if ($LASTEXITCODE -ne 0) {
    python -m pip install -r requirements.txt
    Assert-LastExitCode "Installing Python dependencies"
}

Write-Host "2. Check Flink connector jars"
$jars = @(
    @{ Name = "flink-sql-connector-kafka-3.1.0-1.18.jar"; Url = "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar"; Sha256 = "ad001ada5a43aca44341790a1e28135c808034be2e6b6b47e0394ad8d4d2bf0b" },
    @{ Name = "flink-connector-jdbc-3.1.2-1.18.jar"; Url = "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar"; Sha256 = "141b294306ceabc82fee624e12c6844cf4777ee0dd529f376d18ee98af2d9c9c" },
    @{ Name = "mysql-connector-j-8.3.0.jar"; Url = "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar"; Sha256 = "94e7fa815370cdcefed915db7f53f88445fac110f8c3818392b992ec9ee6d295" },
    @{ Name = "flink-sql-connector-mysql-cdc-3.2.1.jar"; Url = "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-mysql-cdc/3.2.1/flink-sql-connector-mysql-cdc-3.2.1.jar"; Sha256 = "7cd1c46d722c02fdfa5ee421f67ec9492b2275a2c512e4c9999053bdd41025b1" },
    @{ Name = "flink-statebackend-rocksdb-1.18.1.jar"; Url = "https://repo1.maven.org/maven2/org/apache/flink/flink-statebackend-rocksdb/1.18.1/flink-statebackend-rocksdb-1.18.1.jar"; Sha256 = "9f2ac426d0e5ca91cfcb41bd8575dbbce9e69373acabe72555220bec0cbe56c0" },
    @{ Name = "frocksdbjni-6.20.3-ververica-2.0.jar"; Url = "https://repo1.maven.org/maven2/com/ververica/frocksdbjni/6.20.3-ververica-2.0/frocksdbjni-6.20.3-ververica-2.0.jar"; Sha256 = "5e6e5063b75196a17fbeaf656f088a807f83177525d9ecbec7125b9f3630b966" },
    @{ Name = "flink-metrics-prometheus-1.18.1.jar"; Url = "https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.18.1/flink-metrics-prometheus-1.18.1.jar"; Sha256 = "3f8a0e13d0cb33df0264272f649af63fde34ba5f6ebb8132afd2797710f89f76" }
)
New-Item -ItemType Directory -Force "flink-lib" | Out-Null
foreach ($jar in $jars) {
    $target = Join-Path "flink-lib" $jar.Name
    $valid = (Test-Path $target) -and (Get-Item $target).Length -gt 0 -and
        (Get-FileHash -Algorithm SHA256 $target).Hash.ToLowerInvariant() -eq $jar.Sha256
    if (-not $valid) {
        $temporary = "$target.download"
        Invoke-WebRequest -Uri $jar.Url -OutFile $temporary
        $actualHash = (Get-FileHash -Algorithm SHA256 $temporary).Hash.ToLowerInvariant()
        if ($actualHash -ne $jar.Sha256) {
            Remove-Item -LiteralPath $temporary -ErrorAction SilentlyContinue
            throw "Checksum mismatch for $($jar.Name): $actualHash"
        }
        Move-Item -LiteralPath $temporary -Destination $target -Force
    }
}

Write-Host "3. Rebuild only the core infrastructure"
docker compose down -v --remove-orphans
Assert-LastExitCode "Stopping old environment"
docker compose up -d --build zookeeper kafka mysql clickhouse flink-volume-init flink-jobmanager flink-taskmanager
Assert-LastExitCode "Starting core infrastructure"

Write-Host "4. Wait for infrastructure health"
Wait-Until -Label "MySQL" -Condition { docker exec rtdw_mysql mysql -uroot -proot -e "SELECT 1" 2>$null | Out-Null; $LASTEXITCODE -eq 0 }
Wait-Until -Label "Kafka" -Condition { docker exec rtdw_kafka kafka-topics --bootstrap-server kafka:29092 --list 2>$null | Out-Null; $LASTEXITCODE -eq 0 }
Wait-Until -Label "ClickHouse" -Condition { (Invoke-WebRequest http://localhost:8123/ping -UseBasicParsing).Content.Trim() -eq "Ok." }
Wait-Until -Label "Flink" -Condition { (Invoke-RestMethod http://localhost:8081/overview).taskmanagers -ge 1 }

Write-Host "5. Initialize MySQL dimensions"
Send-FileToContainer -Path "scripts/create_mysql_dim_expanded.sql" -DockerArguments @("exec", "-i", "rtdw_mysql", "mysql", "-uroot", "-proot", "ecommerce")
Send-FileToContainer -Path "scripts/create_mysql_business_tables.sql" -DockerArguments @("exec", "-i", "rtdw_mysql", "mysql", "-uroot", "-proot", "ecommerce")

Write-Host "6. Create Kafka topics"
$topics = @(
    @{ Name = "ods_user_behavior"; Policy = "delete" },
    @{ Name = "dwd_dirty_behavior"; Policy = "delete" },
    @{ Name = "dwd_user_behavior"; Policy = "compact,delete" },
    @{ Name = "ads_realtime_overview"; Policy = "compact,delete" },
    @{ Name = "ads_product_rank"; Policy = "compact,delete" },
    @{ Name = "ads_channel_funnel"; Policy = "compact,delete" },
    @{ Name = "ads_category_rank"; Policy = "compact,delete" },
    @{ Name = "ods_order_info"; Policy = "compact,delete" },
    @{ Name = "ods_order_detail"; Policy = "compact,delete" },
    @{ Name = "ods_payment_info"; Policy = "compact,delete" },
    @{ Name = "ods_refund_info"; Policy = "compact,delete" },
    @{ Name = "ods_dim_product_scd2"; Policy = "compact,delete" },
    @{ Name = "dwd_order_detail"; Policy = "compact,delete" },
    @{ Name = "ads_order_lifecycle"; Policy = "compact,delete" },
    @{ Name = "ads_order_daily"; Policy = "compact,delete" },
    # Pure compaction has no time-based delete expiry for unresolved DLQ rows.
    @{ Name = "clickhouse_loader_dlq"; Policy = "compact" }
)
$partitions = if ($env:KAFKA_PARTITIONS) { $env:KAFKA_PARTITIONS } else { "3" }
foreach ($topic in $topics) {
    docker exec rtdw_kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic $topic.Name --partitions $partitions --replication-factor 1 --config "cleanup.policy=$($topic.Policy)"
    Assert-LastExitCode "Creating topic $($topic.Name)"
    docker exec rtdw_kafka kafka-configs --bootstrap-server kafka:29092 --entity-type topics --entity-name $topic.Name --alter --add-config "cleanup.policy=$($topic.Policy)"
    Assert-LastExitCode "Applying cleanup policy to topic $($topic.Name)"
}

Write-Host "7. Initialize ClickHouse tables"
Send-FileToContainer -Path "scripts/create_clickhouse_tables.sql" -DockerArguments @("exec", "-i", "rtdw_clickhouse", "clickhouse-client", "--password", "clickhouse", "--multiquery")

Write-Host "8. Start the long-running loader, freshness exporter, and monitoring stack"
docker compose up -d --build clickhouse-loader pipeline-freshness-exporter prometheus grafana
Assert-LastExitCode "Starting loader and monitoring services"
Wait-Until -Label "ClickHouse loader health" -Condition {
    ((docker inspect --format '{{.State.Health.Status}}' rtdw_clickhouse_loader 2>$null) -join "").Trim() -eq "healthy"
}
Wait-Until -Label "pipeline freshness exporter health" -Condition {
    ((docker inspect --format '{{.State.Health.Status}}' rtdw_pipeline_freshness_exporter 2>$null) -join "").Trim() -eq "healthy"
}

Write-Host "9. Submit behavior and order CDC Flink SQL jobs"
Submit-FlinkSql -Label "Behavior pipeline" -Paths @(
    "flink-sql/01_create_source_tables.sql",
    "flink-sql/02_create_dwd_tables.sql",
    "flink-sql/03_create_dws_ads_tables.sql"
)
Submit-FlinkSql -Label "MySQL CDC pipeline" -Paths @("flink-sql/04_mysql_cdc_to_ods.sql")
Submit-FlinkSql -Label "Order DWD pipeline" -Paths @("flink-sql/05_order_dwd.sql")
Submit-FlinkSql -Label "Order ADS pipeline" -Paths @("flink-sql/06_order_ads.sql")
Wait-Until -Label "nine running Flink jobs" -TimeoutSeconds 300 -Condition {
    $jobs = (Invoke-RestMethod "http://localhost:8081/jobs/overview").jobs
    $bad = @($jobs | Where-Object { $_.state -ne "RUNNING" -or $_.tasks.running -ne $_.tasks.total })
    $jobs.Count -eq 9 -and $bad.Count -eq 0
}

Write-Host "10. Generate 5,000 traceable order journeys"
python scripts/generate_order_journeys.py --journeys 5000 --interval 0 --time-span-minutes 120
Assert-LastExitCode "Generating order journeys"
python scripts/generate_order_transactions.py --orders 500
Assert-LastExitCode "Generating mutable orders"

Write-Host "11. Rehearse an additive schema change"
& "$PSScriptRoot\schema_evolution_drill.ps1"
if (-not $?) { throw "Schema evolution drill failed." }

Write-Host "12. Wait 30 seconds for Flink output to reach the long-running loader"
Start-Sleep -Seconds 30

Write-Host "13. Verify the data"
& "$PSScriptRoot\verify_result.ps1"
python scripts/quality_report.py --output artifacts/latest_quality_report.md
Assert-LastExitCode "Generating quality report"

Write-Host "Demo complete: Flink http://localhost:8081; Prometheus http://localhost:9090; Grafana http://localhost:3000"
