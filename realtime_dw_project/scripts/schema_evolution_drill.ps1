$ErrorActionPreference = "Continue"
$projectDir = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $projectDir

Get-Content -Raw -Encoding utf8 "scripts/schema_evolution_additive.sql" |
    docker exec -i rtdw_mysql mysql -uroot -proot ecommerce
if ($LASTEXITCODE -ne 0) { throw "Applying additive schema change failed." }

Start-Sleep -Seconds 20
$jobs = (Invoke-RestMethod "http://localhost:8081/jobs/overview").jobs
$failed = @($jobs | Where-Object { $_.state -ne "RUNNING" })
if ($jobs.Count -lt 9 -or $failed.Count -gt 0) {
    throw "Schema drill failed: jobs=$($jobs.Count), failed=$($failed.Count)."
}

docker exec rtdw_mysql mysql -uroot -proot ecommerce -e "SELECT table_name, schema_version, compatible_change, applied_at FROM cdc_schema_contract ORDER BY table_name, schema_version;"
if ($LASTEXITCODE -ne 0) { throw "Reading schema contract failed." }
Write-Host "Schema evolution drill passed: $($jobs.Count) jobs remain RUNNING."
