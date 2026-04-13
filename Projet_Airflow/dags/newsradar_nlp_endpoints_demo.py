"""
Demo NewsRadar — enchaine les endpoints JSON du nlp-service :

GET /health → POST /classify → POST /sentiment → POST /enrich

Prerequis : copier ce DAG dans le dossier `dags/` du LAB ou definir la Variable
`nlp_service_url` (ex. http://nlp-service:8000 ou http://host.docker.internal:8000).
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

LABELS = ["tech", "politics", "business", "sports"]


def _airflow_var(key: str, default: str = "") -> str:
    env_key = "AIRFLOW_VAR_" + key.upper()
    val = os.environ.get(env_key)
    if val is not None and str(val).strip() != "":
        return str(val).strip()
    return Variable.get(key, default_var=default)


def _nlp_base_url() -> str:
    return _airflow_var("nlp_service_url", "http://nlp-service:8000").rstrip("/")


def _http_get_json(url: str, timeout: int = 60) -> dict:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _http_post_json(url: str, payload: dict, timeout: int = 900) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


@dag(
    dag_id="newsradar_nlp_endpoints_demo",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["newsradar", "nlp", "demo"],
    doc_md=__doc__,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
)
def newsradar_nlp_endpoints_demo():
    @task()
    def step_health() -> dict:
        base = _nlp_base_url()
        url = f"{base}/health"
        try:
            body = _http_get_json(url, timeout=60)
        except urllib.error.URLError as e:
            raise RuntimeError(f"Injoignable: {url} ({e})") from e
        if body.get("status") != "ok":
            raise RuntimeError(f"Health inattendu: {body}")
        print("HEALTH JSON:", json.dumps(body, ensure_ascii=False))
        return {"base_url": base, "health": body}

    @task(execution_timeout=timedelta(minutes=45))
    def step_classify(prev: dict) -> dict:
        b = prev["base_url"]
        payload = {
            "text": "AI startups report strong growth in software markets.",
            "labels": LABELS,
        }
        out = _http_post_json(f"{b}/classify", payload, timeout=2400)
        print("CLASSIFY JSON:", json.dumps(out, ensure_ascii=False))
        return {**prev, "classify": out}

    @task(execution_timeout=timedelta(minutes=45))
    def step_sentiment(prev: dict) -> dict:
        b = prev["base_url"]
        payload = {"text": "I love this product, amazing quality!"}
        out = _http_post_json(f"{b}/sentiment", payload, timeout=2400)
        print("SENTIMENT JSON:", json.dumps(out, ensure_ascii=False))
        return {**prev, "sentiment": out}

    @task(execution_timeout=timedelta(minutes=60))
    def step_enrich(prev: dict) -> dict:
        b = prev["base_url"]
        payload = {
            "batch": [
                {
                    "id": "lab-1",
                    "url": "https://example.com/a",
                    "title": "Tech news",
                    "body": "Chip makers invest billions in new fabs.",
                },
                {
                    "id": "lab-2",
                    "url": "https://example.com/b",
                    "title": "Politics",
                    "body": "Senators debated the reform bill late into the night.",
                },
            ],
            "labels": LABELS,
        }
        out = _http_post_json(f"{b}/enrich", payload, timeout=3600)
        print("ENRICH JSON:", json.dumps(out, ensure_ascii=False))
        return {**prev, "enrich": out}

    @task()
    def step_summary(prev: dict) -> None:
        print("=== Recapitulatif JSON (cles principales) ===")
        print("health.status:", prev.get("health", {}).get("status"))
        print("classify:", prev.get("classify"))
        print("sentiment:", prev.get("sentiment"))
        en = prev.get("enrich") or {}
        print("enrich.count:", en.get("count"), "modele:", en.get("model"))

    h = step_health()
    c = step_classify(h)
    s = step_sentiment(c)
    e = step_enrich(s)
    step_summary(e)


newsradar_nlp_endpoints_demo()
