"""
API News Simulée — NewsRadar
Simule une API de news payante avec auth Bearer.

Endpoints:
  GET /health
  GET /news?from=YYYY-MM-DD&to=YYYY-MM-DD&topic=tech&limit=20
  GET /news/<id>
"""

import json
import os
import random
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path

from flask import Flask, jsonify, request, abort

app = Flask(__name__)

VALID_TOKEN = os.getenv("API_TOKEN", "formation-token-2026")

# Charger les articles depuis le fichier JSON
DATA_FILE = Path(__file__).parent / "articles.json"
if DATA_FILE.exists():
    with open(DATA_FILE) as f:
        ARTICLES = json.load(f)
else:
    # Données de fallback si fichier absent
    ARTICLES = [
        {
            "id": f"api-{i:04d}",
            "title": f"Article API #{i} — Actualités du jour",
            "body": f"Contenu de l'article #{i}. Ce texte est généré pour la formation NewsRadar. Il couvre les sujets d'actualité du moment.",
            "url": f"http://api-news-simulee/articles/{i:04d}",
            "published_at": (datetime.utcnow() - timedelta(hours=i)).isoformat(),
            "topic": random.choice(["tech", "politics", "business", "sports", "culture", "science", "health"]),
            "source": "api_news",
        }
        for i in range(1, 101)
    ]


def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer ") or auth.split(" ", 1)[1] != VALID_TOKEN:
            abort(401, description="Token Bearer invalide. Utilisez : formation-token-2026")
        return f(*args, **kwargs)
    return decorated


@app.route("/health")
def health():
    return jsonify({"status": "ok", "articles_count": len(ARTICLES)})


@app.route("/news")
@require_auth
def get_news():
    from_date = request.args.get("from")
    to_date   = request.args.get("to")
    topic     = request.args.get("topic")
    limit     = int(request.args.get("limit", 20))

    results = ARTICLES[:]

    if topic:
        results = [a for a in results if a.get("topic", "").lower() == topic.lower()]

    if from_date:
        results = [a for a in results if a.get("published_at", "") >= from_date]

    if to_date:
        results = [a for a in results if a.get("published_at", "") <= to_date + "T23:59:59"]

    results = results[:limit]

    return jsonify({
        "count": len(results),
        "articles": results,
    })


@app.route("/news/<article_id>")
@require_auth
def get_article(article_id):
    article = next((a for a in ARTICLES if a["id"] == article_id), None)
    if not article:
        abort(404, description=f"Article {article_id} non trouvé")
    return jsonify(article)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5500, debug=False)
