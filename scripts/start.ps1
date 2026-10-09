
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

Write-Host "Starting Realtime Data Streaming services..." -ForegroundColor Cyan

docker compose up -d

if ($LASTEXITCODE -ne 0) {
    throw "Docker Compose failed to start the services."
}

Write-Host "`nContainer status:" -ForegroundColor Green
docker compose ps
