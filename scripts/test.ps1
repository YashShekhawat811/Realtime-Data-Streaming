$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

if (-not (Test-Path ".\docker-compose.yml") -and
    -not (Test-Path ".\docker-compose.yaml") -and
    -not (Test-Path ".\compose.yml") -and
    -not (Test-Path ".\compose.yaml")) {
    throw "No Docker Compose file found in $ProjectRoot"
}

Write-Host "`n1. Checking Compose services..." -ForegroundColor Cyan
docker compose ps
if ($LASTEXITCODE -ne 0) {
    throw "Could not read Compose service status."
}

Write-Host "`n2. Checking Kafka topic access..." -ForegroundColor Cyan
docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list
if ($LASTEXITCODE -ne 0) {
    throw "Kafka is not ready or topic listing failed."
}

Write-Host "`n3. Checking Cassandra schema access..." -ForegroundColor Cyan
docker compose exec -T cassandra cqlsh -e "SELECT keyspace_name FROM system_schema.keyspaces;"
if ($LASTEXITCODE -ne 0) {
    throw "Cassandra is not ready or its schema query failed."
}

Write-Host "`nInfrastructure smoke checks passed." -ForegroundColor Green
