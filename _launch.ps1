$ErrorActionPreference = 'Continue'
$root = $PSScriptRoot
$env:Path = 'D:\Docker\DockerDesktop\resources\bin;' + (Join-Path $root 'realtime_dw_project\.venv\Scripts') + ';' + $env:Path

Write-Host '=================================================='
Write-Host ' E-commerce realtime warehouse - one-click launcher'
Write-Host '=================================================='
Write-Host ('project: ' + $root)

# 1) Docker Desktop must be running
if (-not (Get-Process 'Docker Desktop' -ErrorAction SilentlyContinue)) {
    Write-Host '[1/3] Starting Docker Desktop...'
    Start-Process 'D:\Docker\DockerDesktop\Docker Desktop.exe'
} else {
    Write-Host '[1/3] Docker Desktop already running'
}

# 2) Wait for the engine
Write-Host '[2/3] Waiting for Docker engine...'
$ok = $false
for ($i = 0; $i -lt 36; $i++) {
    docker version --format '{{.Server.Version}}' *> $null
    if ($LASTEXITCODE -eq 0) { $ok = $true; break }
    Start-Sleep -Seconds 5
}
if (-not $ok) {
    Write-Host 'ERROR: Docker engine not available. Open Docker Desktop, then run again.'
    exit 1
}
Write-Host '      Docker engine is up.'

# 3) Run the demo
Write-Host '[3/3] Running the demo (this resets data, ~8 min)...'
try { & (Join-Path $root 'start_demo.ps1') } catch { Write-Host ('demo reported: ' + $_.Exception.Message) }

# Safety net: the ClickHouse loader occasionally misses the ads_realtime_overview topic.
$cnt = (& docker exec rtdw_clickhouse clickhouse-client --password clickhouse --query 'SELECT count() FROM ads_realtime_overview') 2>$null
Write-Host ('ads_realtime_overview rows = ' + $cnt)
if (([int]($cnt -as [int])) -eq 0) {
    Write-Host 'ads_realtime_overview empty -> applying loader catch-up fix...'
    & docker stop rtdw_clickhouse_loader | Out-Null
    Start-Sleep -Seconds 6
    & docker exec rtdw_kafka kafka-consumer-groups --bootstrap-server kafka:29092 --delete --group clickhouse_loader 2>&1 | Out-Null
    Start-Sleep -Seconds 3
    & docker start rtdw_clickhouse_loader | Out-Null
    Start-Sleep -Seconds 45
    Write-Host 'Re-verifying...'
    try { & (Join-Path $root 'realtime_dw_project\scripts\verify_result.ps1') } catch { Write-Host ('verify reported: ' + $_.Exception.Message) }
}

Write-Host ''
Write-Host 'Done. Open these in your browser:'
Write-Host '  Flink         http://localhost:8081'
Write-Host '  Grafana       http://localhost:3000   (admin / admin)'
Write-Host '  Prometheus    http://localhost:9090'
Write-Host '  Alertmanager  http://localhost:9093'
