param(
    [Parameter(Mandatory = $true)][string]$StartDate,
    [Parameter(Mandatory = $true)][string]$EndDate,
    [switch]$AllowMismatch
)

$ErrorActionPreference = "Stop"
$null = [datetime]::ParseExact($StartDate, "yyyy-MM-dd", [Globalization.CultureInfo]::InvariantCulture)
$null = [datetime]::ParseExact($EndDate, "yyyy-MM-dd", [Globalization.CultureInfo]::InvariantCulture)
if ($StartDate -ge $EndDate) { throw "EndDate must be later than StartDate" }

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$Driver = Join-Path $ProjectRoot "flink-lib\mysql-connector-j-8.3.0.jar"
if (-not (Test-Path -LiteralPath $Driver)) {
    throw "Missing $Driver. Run the connector download step before Spark backfill."
}
New-Item -ItemType Directory -Force -Path (Join-Path $ProjectRoot "data\spark-warehouse") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $ProjectRoot "artifacts") | Out-Null

$Mismatch = if ($AllowMismatch) { "true" } else { "false" }
Push-Location $ProjectRoot
try {
    docker compose --profile batch run --rm `
        -e "BACKFILL_START_DATE=$StartDate" `
        -e "BACKFILL_END_DATE=$EndDate" `
        -e "ALLOW_RECONCILIATION_MISMATCH=$Mismatch" `
        spark-backfill
    if ($LASTEXITCODE -ne 0) { throw "Spark backfill failed with exit code $LASTEXITCODE" }
} finally {
    Pop-Location
}
