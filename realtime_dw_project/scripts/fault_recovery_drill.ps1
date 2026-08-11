# Windows PowerShell classifies native stderr as an error record. Docker writes
# normal progress and container logs to stderr, so native exit codes are checked
# explicitly instead of making every stderr line terminating.
$ErrorActionPreference = "Continue"

$projectDir = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $projectDir

function Wait-FlinkJobs {
    param([int]$TimeoutSeconds = 180)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            $jobs = (Invoke-RestMethod "http://localhost:8081/jobs/overview").jobs
            $unhealthy = @($jobs | Where-Object {
                $_.state -ne "RUNNING" -or $_.tasks.running -ne $_.tasks.total
            })
            if ($jobs.Count -eq 9 -and $unhealthy.Count -eq 0) {
                return
            }
        } catch {
            # Flink REST may be briefly unavailable during recovery.
        }
        Start-Sleep -Seconds 3
    } while ((Get-Date) -lt $deadline)
    throw "Flink jobs did not recover within $TimeoutSeconds seconds."
}

function Get-RestoreEvidenceCount {
    $lines = @(docker logs rtdw_flink_jobmanager 2>&1 | Select-String -Pattern "Restoring job .* from Checkpoint")
    return $lines.Count
}

function Wait-NewRestoreEvidence {
    param(
        [Parameter(Mandatory)][int]$PreviousCount,
        [int]$TimeoutSeconds = 180
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        if ((Get-RestoreEvidenceCount) -gt $PreviousCount) { return }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw "No new checkpoint restore evidence appeared within $TimeoutSeconds seconds."
}

Write-Host "== Flink checkpoint recovery drill =="
Write-Host "0. Confirm all nine jobs are healthy"
Wait-FlinkJobs

Write-Host "1. Start a bounded producer"
$journeys = if ($env:JOURNEYS) { $env:JOURNEYS } else { "10000" }
$producer = Start-Process python -ArgumentList @(
    "scripts/generate_order_journeys.py", "--journeys", $journeys,
    "--time-span-minutes", "20", "--interval", "0.001"
) -PassThru -NoNewWindow

Write-Host "2. Let checkpoints advance"
Start-Sleep -Seconds 15

Write-Host "3. Kill and relaunch TaskManager"
$restoreCountBefore = Get-RestoreEvidenceCount
$faultStartedAt = Get-Date
docker kill rtdw_flink_taskmanager | Out-Null
if ($LASTEXITCODE -ne 0) { throw "Fault injection failed." }
docker compose up -d flink-taskmanager | Out-Null
if ($LASTEXITCODE -ne 0) { throw "TaskManager relaunch failed." }
Wait-NewRestoreEvidence -PreviousCount $restoreCountBefore
Wait-FlinkJobs
$recoveredAt = Get-Date
$recoverySeconds = [math]::Round(($recoveredAt - $faultStartedAt).TotalSeconds, 2)
$restoreCountAfter = Get-RestoreEvidenceCount

$producer.WaitForExit()
$producer.Refresh()
if ($null -ne $producer.ExitCode -and $producer.ExitCode -ne 0) {
    throw "Event producer failed with exit code $($producer.ExitCode)."
}
Start-Sleep -Seconds 20

Write-Host "4. Replay outputs and verify business-key idempotency"
$groupId = "fault_drill_loader_$([DateTimeOffset]::UtcNow.ToUnixTimeSeconds())"
python scripts/load_kafka_to_clickhouse.py --group-id $groupId --max-messages 0 --idle-timeout 20
if ($LASTEXITCODE -ne 0) { throw "Kafka-to-ClickHouse loader failed." }

& "$PSScriptRoot\verify_result.ps1"
if (-not $?) { throw "Acceptance checks failed." }

$duplicateBehavior = docker exec rtdw_clickhouse clickhouse-client --password clickhouse --query `
    "SELECT count() FROM (SELECT event_id FROM dwd_user_behavior FINAL GROUP BY event_id HAVING count() > 1)"
$duplicateOrder = docker exec rtdw_clickhouse clickhouse-client --password clickhouse --query `
    "SELECT count() FROM (SELECT detail_id FROM dwd_order_detail FINAL WHERE is_deleted=0 GROUP BY detail_id HAVING count() > 1)"
$historicalMismatch = docker exec rtdw_clickhouse clickhouse-client --password clickhouse --query `
    "SELECT count() FROM dwd_order_detail FINAL WHERE is_deleted=0 AND unit_price != catalog_price"

New-Item -ItemType Directory -Force artifacts | Out-Null
$reportPath = Join-Path $projectDir "artifacts\fault_recovery_$((Get-Date).ToString('yyyyMMdd_HHmmss')).md"
$report = @(
    "# Fault recovery drill",
    "",
    "> Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')",
    "",
    "| Item | Result |",
    "| --- | ---: |",
    "| Injected failure | Kill TaskManager |",
    "| Flink jobs | 9 |",
    "| Producer journeys | $journeys |",
    "| Recovery time | $recoverySeconds s |",
    "| New checkpoint restore log lines | $($restoreCountAfter - $restoreCountBefore) |",
    "| Duplicate behavior keys | $duplicateBehavior |",
    "| Duplicate order detail keys | $duplicateOrder |",
    "| Historical price mismatches | $historicalMismatch |",
    "| Acceptance | PASS |",
    "",
    "Recovery time starts immediately before `docker kill` and ends after all nine jobs " +
    "and every task report RUNNING. This is a single-node recovery rehearsal, not HA failover.",
    ""
) -join "`n"
[System.IO.File]::WriteAllText($reportPath, $report, [System.Text.UTF8Encoding]::new($false))
Write-Host "Fault drill passed in $recoverySeconds seconds. Report: $reportPath"
