$ErrorActionPreference = "Stop"

Set-Location (Join-Path $PSScriptRoot "realtime_dw_project")
& (Join-Path $PSScriptRoot "realtime_dw_project\scripts\run_demo.ps1")
