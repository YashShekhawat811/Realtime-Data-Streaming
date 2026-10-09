$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

Write-Host "Stopping project Compose services..." -ForegroundColor Yellow
docker compose stop

if ($LASTEXITCODE -ne 0) {
    throw "Failed to stop Compose services."
}

Write-Host "Services stopped. Containers and volumes were not removed." -ForegroundColor Green
