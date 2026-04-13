"""
DAG NewsRadar — pipeline ML visible dans le graphe Airflow :

1. Chargement des articles (MinIO JSON ou demo)
2. Classification thematique : POST /classify (par article)
3. Analyse de sentiment : POST /sentiment (par article)
4. Fusion des resultats (= enrichissement logique : topic + sentiment par article)
5. Placeholder persistance (Postgres / OpenSearch plus tard)

Meme charge ML que POST /enrich (2 modeles par article), mais 3 taches HTTP distinctes
pour que le graphe Airflow montre bien les etapes.

Prerequis : `nlp_service_url`, optionnel MinIO (`newsradar_minio_*`) — voir README_NLP.md.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

DEFAULT_LABELS = ["tech", "politics", "business", "sports"]
MINIO_CONN_ID = "minio_local"


def _article_key(article: dict, index: int) -> str:
    aid = article.get("id")
    return str(aid) if aid is not None else f"row-{index}"


def _airflow_var(key: str, default: str = "") -> str:
    env_key = "AIRFLOW_VAR_" + key.upper()
    val = os.environ.get(env_key)
    if val is not None and str(val).strip() != "":
        return str(val).strip()
    return Variable.get(key, default_var=default)


def _nlp_base_url() -> str:
    return _airflow_var("nlp_service_url", "http://nlp-service:8000").rstrip("/")


def _http_post_json(url: str, payload: dict, timeout: int = 600) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _article_from_json_doc(doc: dict) -> dict | None:
    body = doc.get("body") or doc.get("text") or doc.get("content") or doc.get("article_body")
    if not body or not str(body).strip():
        return None
    aid = doc.get("id") or doc.get("article_id") or doc.get("_id")
    if aid is not None:
        aid = str(aid)
    return {
        "id": aid,
        "url": doc.get("url"),
        "title": doc.get("title"),
        "body": str(body).strip(),
    }


def _demo_articles() -> list[dict]:
    return [
        {
            "id": "demo-1",
            "url": "https://example.com/1",
            "title": "IA et marches",
            "body": "AI startups report strong growth in software markets.",
        },
        {
            "id": "demo-2",
            "url": "https://example.com/2",
            "title": "Elections",
            "body": "The parliament voted on a new budget package yesterday.",
        },
        {
            "id": "demo-3",
            "url": "https://example.com/3",
            "title": "Sport",
            "body": "The national team won the championship final in overtime.",
        },
    ]


@dag(
    dag_id="newsradar_enrich_ml",
    # Lecture MinIO (JSON articles_*.json) ou demo ; 15 min limite la charge NLP
    schedule=timedelta(minutes=15),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["newsradar", "nlp", "ml"],
    doc_md=__doc__,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    max_active_runs=1,
)
def newsradar_enrich_ml():
    @task()
    def check_nlp_health() -> str:
        base = _nlp_base_url()
        health_url = f"{base}/health"
        req = urllib.request.Request(health_url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except urllib.error.URLError as e:
            raise RuntimeError(f"nlp-service injoignable ({health_url}): {e}") from e
        if body.get("status") != "ok":
            raise RuntimeError(f"health inattendu: {body}")
        return base

    @task()
    def load_articles_to_enrich() -> list[dict]:
        prefix = _airflow_var("newsradar_minio_prefix", "").strip()
        if not prefix:
            print("newsradar_minio_prefix vide -> articles demo (pas de lecture MinIO).")
            return _demo_articles()

        bucket = _airflow_var("newsradar_minio_bucket", "data-lake")
        max_files = int(_airflow_var("newsradar_minio_max_files", "3"))

        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        keys = hook.list_keys(bucket_name=bucket, prefix=prefix) or []
        json_keys = [k for k in keys if k.endswith(".json")]
        json_keys = sorted(json_keys)[:max_files]

        if not json_keys:
            print(f"Aucun .json sous s3://{bucket}/{prefix} -> fallback demo.")
            return _demo_articles()

        articles: list[dict] = []
        for key in json_keys:
            raw = hook.read_key(key=key, bucket_name=bucket)
            doc = json.loads(raw)
            iterable = doc if isinstance(doc, list) else [doc]
            for item in iterable:
                if not isinstance(item, dict):
                    continue
                row = _article_from_json_doc(item)
                if row:
                    articles.append(row)

        if not articles:
            print("JSON presents mais aucun body exploitable -> fallback demo.")
            return _demo_articles()

        max_articles = int(_airflow_var("newsradar_max_articles", "0"))
        if max_articles > 0:
            articles = articles[:max_articles]
            print(f"Limite newsradar_max_articles={max_articles} -> {len(articles)} article(s) pour le NLP.")

        print(f"MinIO: {len(articles)} article(s) charge(s) depuis {len(json_keys)} fichier(s).")
        return articles

    @task(execution_timeout=timedelta(minutes=45))
    def nlp_classify_all(base_url: str, articles: list[dict]) -> list[dict]:
        """Etape 1 (graphe) : classification zero-shot par article via POST /classify."""
        endpoint = f"{base_url}/classify"
        out: list[dict] = []
        for idx, art in enumerate(articles):
            key = _article_key(art, idx)
            payload = {"text": art["body"], "labels": DEFAULT_LABELS}
            r = _http_post_json(endpoint, payload, timeout=2400)
            row = {
                "id": key,
                "topic": r.get("topic"),
                "confidence": r.get("confidence"),
                "model": r.get("model", ""),
            }
            out.append(row)
            print(f"CLASSIFY [{key}] -> {json.dumps(row, ensure_ascii=False)}")
        print(f"Classification terminee : {len(out)} article(s).")
        return out

    @task(execution_timeout=timedelta(minutes=45))
    def nlp_sentiment_all(base_url: str, articles: list[dict]) -> list[dict]:
        """Etape 2 (graphe) : sentiment par article via POST /sentiment."""
        endpoint = f"{base_url}/sentiment"
        out: list[dict] = []
        for idx, art in enumerate(articles):
            key = _article_key(art, idx)
            payload = {"text": art["body"]}
            r = _http_post_json(endpoint, payload, timeout=2400)
            row = {
                "id": key,
                "label": r.get("label"),
                "score": r.get("score"),
                "model": r.get("model", ""),
            }
            out.append(row)
            print(f"SENTIMENT [{key}] -> {json.dumps(row, ensure_ascii=False)}")
        print(f"Sentiment termine : {len(out)} article(s).")
        return out

    @task()
    def merge_enrichment(
        articles: list[dict],
        classifications: list[dict],
        sentiments: list[dict],
    ) -> list[dict]:
        """Etape 3 (graphe) : fusion = enrichissement (topic + sentiment + meta article)."""
        cls_by = {x["id"]: x for x in classifications}
        sent_by = {x["id"]: x for x in sentiments}
        enriched: list[dict] = []
        for idx, art in enumerate(articles):
            key = _article_key(art, idx)
            c = cls_by.get(key, {})
            s = sent_by.get(key, {})
            item = {
                "id": art.get("id"),
                "url": art.get("url"),
                "title": art.get("title"),
                "topic": c.get("topic"),
                "confidence": c.get("confidence"),
                "sentiment_label": s.get("label"),
                "sentiment_score": s.get("score"),
                "model": " + ".join(
                    p for p in (c.get("model"), s.get("model")) if p
                ),
            }
            enriched.append(item)
            print(f"ENRICHISSEMENT [{key}] -> {json.dumps(item, ensure_ascii=False)}")
        print(f"Fusion / enrichissement : {len(enriched)} ligne(s).")
        return enriched

    @task()
    def persist_enrichments_placeholder(enriched: list[dict]) -> int:
        print(f"Total enrichissements a persister : {len(enriched)}")
        return len(enriched)

    base = check_nlp_health()
    articles = load_articles_to_enrich()
    classified = nlp_classify_all(base, articles)
    sentiments = nlp_sentiment_all(base, articles)
    merged = merge_enrichment(articles, classified, sentiments)
    persist_enrichments_placeholder(merged)


newsradar_enrich_ml()
