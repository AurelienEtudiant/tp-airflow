# Verification rapide des ports exposes par docker-compose.yml (hote Windows).
# Usage : depuis la racine Projet_Airflow : .\scripts\verify-stack.ps1
# Preconditions : docker compose up -d (stack demarree).

$ErrorActionPreference = "Continue"
$checks = @(
    @{ Name = "Airflow API";    Url = "http://127.0.0.1:8080/api/v2/monitor/health" },
    @{ Name = "NLP service";    Url = "http://127.0.0.1:8000/health" },
    @{ Name = "API simulee";    Url = "http://127.0.0.1:5001/health" },
    @{ Name = "MinIO live";     Url = "http://127.0.0.1:9000/minio/health/live" },
    @{ Name = "OpenSearch";     Url = "http://127.0.0.1:9200" }
)

Write-Host "Verification HTTP (timeout 15s par URL)..." -ForegroundColor Cyan
$ok = 0
foreach ($c in $checks) {
    try {
        $r = Invoke-WebRequest -Uri $c.Url -UseBasicParsing -TimeoutSec 15
        Write-Host "  OK  $($c.Name) [$($r.StatusCode)]" -ForegroundColor Green
        $ok++
    }
    catch {
        Write-Host "  --  $($c.Name) : $($_.Exception.Message)" -ForegroundColor Yellow
    }
}
Write-Host "Resultat : $ok / $($checks.Count) URLs joignables." -ForegroundColor Cyan
