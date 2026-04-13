"""
NewsRadar NLP Service
FastAPI + HuggingFace Transformers
Endpoints : /health, /classify, /sentiment, /enrich (batch)

IMPORTANT : les modèles sont chargés une seule fois au démarrage.
Ne pas importer torch/transformers au niveau du DAG Airflow (OOM).
"""

import os
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from prometheus_fastapi_instrumentator import Instrumentator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# Configuration
# ============================================================
MODEL_SENTIMENT = os.getenv("MODEL_SENTIMENT", "distilbert-base-uncased-finetuned-sst-2-english")
MODEL_CLASSIFY  = os.getenv("MODEL_CLASSIFY",  "facebook/bart-large-mnli")

TOPICS = ["tech", "politics", "business", "sports", "culture", "science", "health"]

# ============================================================
# Chargement des modèles (une seule fois au démarrage)
# ============================================================
sentiment_pipeline = None
classify_pipeline  = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global sentiment_pipeline, classify_pipeline

    logger.info(f"⏳ Chargement modèle sentiment : {MODEL_SENTIMENT}")
    try:
        from transformers import pipeline as hf_pipeline
        sentiment_pipeline = hf_pipeline(
            "sentiment-analysis",
            model=MODEL_SENTIMENT,
            truncation=True,
            max_length=512,
        )
        logger.info("✅ Modèle sentiment chargé")
    except Exception as e:
        logger.error(f"❌ Erreur chargement sentiment : {e}")

    logger.info(f"⏳ Chargement modèle classification : {MODEL_CLASSIFY}")
    try:
        from transformers import pipeline as hf_pipeline
        classify_pipeline = hf_pipeline(
            "zero-shot-classification",
            model=MODEL_CLASSIFY,
            truncation=True,
        )
        logger.info("✅ Modèle classification chargé")
    except Exception as e:
        logger.error(f"❌ Erreur chargement classification : {e}")

    yield
    logger.info("🔴 Arrêt du NLP service")


# ============================================================
# Application
# ============================================================
app = FastAPI(
    title="NewsRadar NLP Service",
    description="Service ML pour classification thématique et analyse de sentiment",
    version="1.0.0",
    lifespan=lifespan,
)

# Métriques Prometheus (bonus observabilité)
Instrumentator().instrument(app).expose(app)


# ============================================================
# Schémas Pydantic
# ============================================================
class TextRequest(BaseModel):
    text: str


class BatchItem(BaseModel):
    title: str
    body: Optional[str] = ""


class BatchRequest(BaseModel):
    batch: list[BatchItem]


class ClassifyResponse(BaseModel):
    topic: str
    confidence: float


class SentimentResponse(BaseModel):
    label: str
    score: float


class EnrichmentResult(BaseModel):
    topic: str
    topic_confidence: float
    sentiment_label: str
    sentiment_score: float


# ============================================================
# Endpoints
# ============================================================

@app.get("/health")
def health():
    return {
        "status": "ok",
        "models_loaded": {
            "sentiment":      sentiment_pipeline is not None,
            "classification": classify_pipeline  is not None,
        },
        "topics": TOPICS,
    }


@app.post("/classify", response_model=ClassifyResponse)
def classify(req: TextRequest):
    if classify_pipeline is None:
        raise HTTPException(status_code=503, detail="Modèle de classification non chargé")
    try:
        result = classify_pipeline(req.text[:1024], candidate_labels=TOPICS)
        return ClassifyResponse(
            topic=result["labels"][0],
            confidence=round(float(result["scores"][0]), 4),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sentiment", response_model=SentimentResponse)
def sentiment(req: TextRequest):
    if sentiment_pipeline is None:
        raise HTTPException(status_code=503, detail="Modèle de sentiment non chargé")
    try:
        result = sentiment_pipeline(req.text[:512])[0]
        return SentimentResponse(
            label=result["label"].upper(),
            score=round(float(result["score"]), 4),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/enrich", response_model=list[EnrichmentResult])
def enrich_batch(req: BatchRequest):
    """
    Enrichissement en batch : TOUJOURS utiliser cet endpoint depuis Airflow.
    Appels unitaires = 8000 req HTTP/jour. Batch de 50 = 160 req/jour (50x moins).
    """
    if not req.batch:
        return []
    if sentiment_pipeline is None or classify_pipeline is None:
        raise HTTPException(status_code=503, detail="Modèles non chargés")

    texts = [f"{item.title}. {item.body}"[:1024] for item in req.batch]
    short_texts = [t[:512] for t in texts]

    try:
        sentiments   = sentiment_pipeline(short_texts, batch_size=16)
        topics       = classify_pipeline(texts, candidate_labels=TOPICS, batch_size=4)

        results = []
        for i in range(len(req.batch)):
            # classify_pipeline peut retourner une liste ou un seul dict selon la version
            topic_result = topics[i] if isinstance(topics, list) else topics
            results.append(EnrichmentResult(
                topic=topic_result["labels"][0],
                topic_confidence=round(float(topic_result["scores"][0]), 4),
                sentiment_label=sentiments[i]["label"].upper(),
                sentiment_score=round(float(sentiments[i]["score"]), 4),
            ))
        return results

    except Exception as e:
        logger.error(f"Erreur enrichissement batch : {e}")
        raise HTTPException(status_code=500, detail=str(e))
