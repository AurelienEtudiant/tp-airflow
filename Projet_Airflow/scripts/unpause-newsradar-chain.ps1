# Active les DAGs NLP (newsradar_*). Usage : .\scripts\unpause-newsradar-chain.ps1
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

$ids = @("newsradar_enrich_ml", "newsradar_nlp_endpoints_demo")
foreach ($id in $ids) {
    Write-Host "Unpause $id ..." -ForegroundColor Cyan
    docker compose exec -T airflow-scheduler airflow dags unpause $id -y
}

Write-Host "Termine." -ForegroundColor Green
