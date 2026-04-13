# Supprime les runs FAILED de newsradar_enrich_ml puis declenche une nouvelle execution.
# A lancer depuis le dossier Projet_Airflow, Docker doit etre up.

$ErrorActionPreference = "Stop"
$scheduler = "projet-airflow-airflow-scheduler-1"

Write-Host "=== Suppression des executions FAILED (newsradar_enrich_ml) ===" -ForegroundColor Cyan
docker exec $scheduler python /opt/airflow/scripts/delete_failed_newsradar_runs.py

Write-Host "`n=== Declenchement du DAG newsradar_enrich_ml ===" -ForegroundColor Cyan
docker exec $scheduler airflow dags trigger newsradar_enrich_ml

Write-Host "`nOK. Verifie http://localhost:8080" -ForegroundColor Green
