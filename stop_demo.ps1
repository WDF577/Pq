param(
    [switch]$ShutdownDockerDesktop
)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "realtime_dw_project")

# Stop containers without deleting volumes, checkpoints, or generated results.
docker compose stop
if ($LASTEXITCODE -ne 0) { throw "Stopping the demo containers failed." }

if ($ShutdownDockerDesktop) {
    docker desktop stop
    if ($LASTEXITCODE -ne 0) {
        throw "Containers stopped, but Docker Desktop could not be shut down by the CLI."
    }
}
