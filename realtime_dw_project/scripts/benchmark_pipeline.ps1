param(
    [int]$Journeys = 10000,
    [int]$Orders = 1000,
    [int]$IdleTimeout = 20
)

$ErrorActionPreference = "Stop"
$projectDir = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $projectDir

function Get-TopicEndOffset {
    param([Parameter(Mandatory)][string]$Topic)
    $lines = docker exec rtdw_kafka kafka-run-class kafka.tools.GetOffsetShell `
        --broker-list kafka:9092 --topic $Topic --time -1
    if ($LASTEXITCODE -ne 0) { throw "Cannot read end offsets for $Topic." }
    $sum = 0L
    foreach ($line in $lines) {
        if ("$line" -match ":(\d+)$") { $sum += [long]$Matches[1] }
    }
    return $sum
}

function Assert-NineHealthyJobs {
    $jobs = (Invoke-RestMethod "http://localhost:8081/jobs/overview").jobs
    $bad = @($jobs | Where-Object {
        $_.state -ne "RUNNING" -or $_.tasks.running -ne $_.tasks.total
    })
    if ($jobs.Count -ne 9 -or $bad.Count -ne 0) {
        throw "Benchmark requires nine fully running Flink jobs."
    }
}

Assert-NineHealthyJobs
New-Item -ItemType Directory -Force artifacts | Out-Null
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$loaderOut = Join-Path $projectDir "artifacts\benchmark_loader_$stamp.log"
$loaderErr = Join-Path $projectDir "artifacts\benchmark_loader_$stamp.err.log"
$groupId = "benchmark_live_$stamp"

$beforeBehavior = Get-TopicEndOffset "dwd_user_behavior"
$beforeOrder = Get-TopicEndOffset "dwd_order_detail"

$loader = Start-Process -FilePath python -ArgumentList @(
    "scripts/load_kafka_to_clickhouse.py",
    "--group-id", $groupId,
    "--auto-offset-reset", "latest",
    "--max-messages", "0",
    "--idle-timeout", "$IdleTimeout",
    "--batch-size", "1000"
) -RedirectStandardOutput $loaderOut -RedirectStandardError $loaderErr `
  -PassThru -WindowStyle Hidden

# Give the latest-offset consumer time to receive its partition assignment.
Start-Sleep -Seconds 4
$startedAt = Get-Date
$journeyOutput = python scripts/generate_order_journeys.py `
    --journeys $Journeys --time-span-minutes 120 --interval 0 2>&1
if ($LASTEXITCODE -ne 0) { throw "Journey producer failed." }
$journeyOutput | ForEach-Object { Write-Host $_ }

$orderOutput = python scripts/generate_order_transactions.py --orders $Orders 2>&1
if ($LASTEXITCODE -ne 0) { throw "Order producer failed." }
$orderOutput | ForEach-Object { Write-Host $_ }

$stablePolls = 0
$lastBehavior = -1L
$lastOrder = -1L
$deadline = (Get-Date).AddMinutes(5)
do {
    Start-Sleep -Seconds 2
    $currentBehavior = Get-TopicEndOffset "dwd_user_behavior"
    $currentOrder = Get-TopicEndOffset "dwd_order_detail"
    if ($currentBehavior -gt $beforeBehavior -and $currentOrder -gt $beforeOrder `
        -and $currentBehavior -eq $lastBehavior -and $currentOrder -eq $lastOrder) {
        $stablePolls++
    } else {
        $stablePolls = 0
    }
    $lastBehavior = $currentBehavior
    $lastOrder = $currentOrder
} while ($stablePolls -lt 3 -and (Get-Date) -lt $deadline)

if ($stablePolls -lt 3) { throw "Flink output offsets did not settle within five minutes." }
$flinkSettledAt = Get-Date

if (-not $loader.WaitForExit(180000)) {
    Stop-Process -Id $loader.Id -Force
    throw "Benchmark loader did not finish after the output became idle."
}
$loader.Refresh()
$loaderExitCode = $loader.ExitCode
$loaderErrorText = if (Test-Path $loaderErr) {
    $rawLoaderError = Get-Content -Raw -Encoding utf8 $loaderErr
    if ($null -eq $rawLoaderError) { "" } else { $rawLoaderError.Trim() }
} else { "" }
$loaderOutputText = if (Test-Path $loaderOut) {
    $rawLoaderOutput = Get-Content -Raw -Encoding utf8 $loaderOut
    if ($null -eq $rawLoaderOutput) { "" } else { "$rawLoaderOutput" }
} else { "" }
if (($null -ne $loaderExitCode -and $loaderExitCode -ne 0) `
    -or $loaderErrorText `
    -or $loaderOutputText -notmatch "done, consumed \d+ messages and inserted \d+ rows") {
    throw "Benchmark loader failed. See $loaderErr."
}

$afterBehavior = Get-TopicEndOffset "dwd_user_behavior"
$afterOrder = Get-TopicEndOffset "dwd_order_detail"
$behaviorDelta = $afterBehavior - $beforeBehavior
$orderDelta = $afterOrder - $beforeOrder
$flinkSeconds = [math]::Round(($flinkSettledAt - $startedAt).TotalSeconds, 2)
$outputPerSecond = if ($flinkSeconds -gt 0) {
    [math]::Round(($behaviorDelta + $orderDelta) / $flinkSeconds, 2)
} else { 0 }

& "$PSScriptRoot\verify_result.ps1"
if (-not $?) { throw "Post-benchmark acceptance checks failed." }

$cpu = (Get-CimInstance Win32_Processor | Select-Object -First 1 -ExpandProperty Name).Trim()
$memoryGb = [math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1GB, 1)
$reportPath = Join-Path $projectDir "artifacts\benchmark_$stamp.md"
$report = @(
    "# Pipeline benchmark",
    "",
    "> Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')",
    "",
    "| Item | Result |",
    "| --- | ---: |",
    "| Host CPU | $cpu |",
    "| Host memory | $memoryGb GB |",
    "| Flink jobs | 9 |",
    "| Kafka partitions/topic | 3 |",
    "| Journeys submitted | $Journeys |",
    "| Mutable orders submitted | $Orders |",
    "| DWD behavior output records | $behaviorDelta |",
    "| DWD order changelog records | $orderDelta |",
    "| Producer start to Flink offsets settled | $flinkSeconds s |",
    "| Combined DWD output rate | $outputPerSecond records/s |",
    "| Post-run acceptance | PASS |",
    "",
    "This is a single-node Docker benchmark. The rate measures Kafka DWD output records " +
    "until offsets remain stable for three polls; it is not a production capacity claim. " +
    "ClickHouse was consumed concurrently from latest offsets and then passed logical-key checks.",
    ""
) -join "`n"
[System.IO.File]::WriteAllText($reportPath, $report, [System.Text.UTF8Encoding]::new($false))
Write-Host $report
Write-Host "Benchmark report: $reportPath"
