#!/bin/bash
# ============================================================
# Script d'initialisation de l'index OpenSearch
# Usage : ./opensearch/init_opensearch.sh [HOST]
# Ex    : ./opensearch/init_opensearch.sh localhost:9200
# ============================================================

HOST=${1:-localhost:9200}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "🔌 Connexion à OpenSearch : http://$HOST"

# Attendre que OpenSearch soit prêt
MAX_RETRIES=20
RETRY=0
until curl -sf "http://$HOST/_cluster/health" > /dev/null 2>&1; do
    RETRY=$((RETRY + 1))
    if [ $RETRY -ge $MAX_RETRIES ]; then
        echo "❌ OpenSearch ne répond pas après $MAX_RETRIES tentatives"
        exit 1
    fi
    echo "⏳ Attente OpenSearch... ($RETRY/$MAX_RETRIES)"
    sleep 5
done

CLUSTER_STATUS=$(curl -sf "http://$HOST/_cluster/health" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['status'])" 2>/dev/null)
echo "✅ OpenSearch prêt (status: $CLUSTER_STATUS)"

# Supprimer l'index s'il existe déjà (utile en dev/reset)
EXISTING=$(curl -sf -o /dev/null -w "%{http_code}" "http://$HOST/articles")
if [ "$EXISTING" = "200" ]; then
    echo "🗑️  Suppression index articles existant..."
    curl -s -X DELETE "http://$HOST/articles"
    echo ""
fi

# Créer l'index avec le mapping
echo "📦 Création de l'index 'articles'..."
RESULT=$(curl -s -X PUT "http://$HOST/articles" \
    -H "Content-Type: application/json" \
    -d @"$SCRIPT_DIR/articles_mapping.json")

echo "$RESULT"

# Vérification
STATUS=$(echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('acknowledged', 'false'))" 2>/dev/null)
if [ "$STATUS" = "True" ] || [ "$STATUS" = "true" ]; then
    echo ""
    echo "✅ Index 'articles' créé avec succès"
    echo ""
    echo "Mapping créé :"
    curl -s "http://$HOST/articles/_mapping" | python3 -m json.tool 2>/dev/null | head -40
else
    echo ""
    echo "❌ Erreur lors de la création de l'index"
    exit 1
fi

echo ""
echo "🎯 OpenSearch prêt pour NewsRadar !"
echo "   Dashboard : http://localhost:5601"
echo "   Dev Tools  : http://localhost:5601/app/dev_tools"
echo ""
echo "Test rapide :"
echo "  curl 'http://$HOST/articles/_search?pretty&size=0'"
