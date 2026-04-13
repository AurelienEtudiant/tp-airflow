# ============================================================
# NewsRadar - Makefile
# Usage : make <cible>
# ============================================================

.PHONY: help up down reset logs ps init-opensearch check-health shell-worker shell-pg

help:
	@echo ""
	@echo "NewsRadar — commandes disponibles"
	@echo "=================================="
	@echo "  make up               Démarre tous les services"
	@echo "  make down             Arrête tout (garde les volumes)"
	@echo "  make reset            Arrête ET supprime tous les volumes (DESTRUCTIF)"
	@echo "  make logs             Logs de tous les services"
	@echo "  make ps               Status des containers"
	@echo "  make check-health     Vérifie que tous les services répondent"
	@echo "  make init-opensearch  (Re)crée l'index OpenSearch"
	@echo "  make shell-worker     Shell dans airflow-worker"
	@echo "  make shell-pg         psql sur postgres-newsradar"
	@echo "  make test             Lance les tests pytest"
	@echo ""

# Créer le fichier .env si absent
.env:
	@echo "AIRFLOW_UID=$$(id -u 2>/dev/null || echo 50000)" > .env
	@echo "📝 Fichier .env créé"

up: .env
	@echo "🚀 Démarrage NewsRadar..."
	docker compose up -d
	@echo ""
	@echo "⏳ Attente initialisation (60s)..."
	@sleep 30
	@echo "Services :"
	@echo "  Airflow    : http://localhost:8080  (admin / admin)"
	@echo "  MinIO      : http://localhost:9001  (minioadmin / minioadmin)"
	@echo "  OpenSearch : http://localhost:9200"
	@echo "  Dashboards : http://localhost:5601"
	@echo "  NLP Service: http://localhost:8000/health"
	@echo "  API News   : http://localhost:5500/health"

down:
	docker compose down

reset:
	@echo "⚠️  Suppression de TOUS les volumes (données perdues !)"
	@read -p "Confirmer ? (oui/non) : " confirm && [ "$$confirm" = "oui" ]
	docker compose down -v
	@echo "✅ Reset complet"

logs:
	docker compose logs -f --tail=50

ps:
	docker compose ps

check-health:
	@echo "🔍 Vérification des services..."
	@echo -n "Airflow    : " && curl -sf http://localhost:8080/api/v2/monitor/health | python3 -c "import sys,json; d=json.load(sys.stdin); print('✅ OK' if d.get('status')=='healthy' else '❌ ' + str(d))" 2>/dev/null || echo "❌ Non disponible"
	@echo -n "OpenSearch : " && curl -sf "http://localhost:9200/_cluster/health" | python3 -c "import sys,json; d=json.load(sys.stdin); print('✅ ' + d.get('status','?'))" 2>/dev/null || echo "❌ Non disponible"
	@echo -n "NLP Service: " && curl -sf http://localhost:8000/health | python3 -c "import sys,json; d=json.load(sys.stdin); print('✅ OK - modèles:', d.get('models_loaded',{}))" 2>/dev/null || echo "❌ Non disponible (normal au premier démarrage, les modèles téléchargent)"
	@echo -n "MinIO      : " && curl -sf http://localhost:9000/minio/health/live && echo "✅ OK" || echo "❌ Non disponible"
	@echo -n "API News   : " && curl -sf http://localhost:5500/health | python3 -c "import sys,json; d=json.load(sys.stdin); print('✅ OK -', d.get('articles_count','?'), 'articles')" 2>/dev/null || echo "❌ Non disponible"
	@echo -n "PostgreSQL : " && docker compose exec -T postgres-newsradar pg_isready -U news_user -d newsradar 2>/dev/null && echo "✅ OK" || echo "❌ Non disponible"

init-opensearch:
	@echo "📦 (Re)création index OpenSearch..."
	./opensearch/init_opensearch.sh localhost:9200

shell-worker:
	docker compose exec airflow-worker bash

shell-pg:
	docker compose exec postgres-newsradar psql -U news_user -d newsradar

test:
	docker compose exec airflow-worker pytest /opt/airflow/tests/ -v

stop-opensearch:
	@echo "⏸️  Arrêt OpenSearch (libère ~1Go RAM pendant le J1)..."
	docker compose stop opensearch opensearch-dashboards

start-opensearch:
	@echo "▶️  Démarrage OpenSearch..."
	docker compose start opensearch opensearch-dashboards

check-dupes:
	@echo "🔍 Vérification doublons..."
	docker compose exec postgres-newsradar psql -U news_user -d newsradar -c \
		"SELECT url_hash, COUNT(*) FROM fact_article GROUP BY url_hash HAVING COUNT(*) > 1;"
	@echo "(0 lignes = pas de doublons ✅)"

stats:
	docker compose exec postgres-newsradar psql -U news_user -d newsradar -c \
		"SELECT source_id, name, type, total_articles, enriched_articles, indexed_articles, last_ingestion FROM v_ingest_stats ORDER BY total_articles DESC;"
