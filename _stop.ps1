$root = $PSScriptRoot
$env:Path = 'D:\Docker\DockerDesktop\resources\bin;' + $env:Path
Write-Host 'Stopping the demo containers (data is kept)...'
& (Join-Path $root 'stop_demo.ps1')
Write-Host 'Stopped. Docker Desktop is still running.'
