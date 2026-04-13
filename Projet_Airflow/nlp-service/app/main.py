from functools import lru_cache

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from transformers import pipeline


DEFAULT_TOPICS = ["tech", "politics", "business", "sports"]
TOPIC_MODEL_NAME = "valhalla/distilbart-mnli-12-3"
SENTIMENT_MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment-latest"


class ClassifyRequest(BaseModel):
    text: str = Field(..., min_length=1, description="Texte a classifier")
    labels: list[str] = Field(
        default_factory=lambda: DEFAULT_TOPICS,
        min_length=1,
        description="Labels cibles",
    )


class ClassifyResponse(BaseModel):
    topic: str
    confidence: float
    model: str


class SentimentRequest(BaseModel):
    text: str = Field(..., min_length=1, description="Texte pour sentiment")


class SentimentResponse(BaseModel):
    label: str
    score: float
    model: str


class EnrichArticle(BaseModel):
    id: str | None = None
    url: str | None = None
    title: str | None = None
    body: str = Field(..., min_length=1)


class EnrichRequest(BaseModel):
    batch: list[EnrichArticle] = Field(..., min_length=1, max_length=50)
    labels: list[str] = Field(default_factory=lambda: DEFAULT_TOPICS, min_length=1)


class EnrichItem(BaseModel):
    id: str | None = None
    url: str | None = None
    topic: str
    confidence: float
    sentiment_label: str
    sentiment_score: float


class EnrichResponse(BaseModel):
    count: int
    items: list[EnrichItem]
    model: str


app = FastAPI(title="nlp-service", version="2.0.0")


@lru_cache(maxsize=1)
def _topic_classifier():
    return pipeline(
        task="zero-shot-classification",
        model=TOPIC_MODEL_NAME,
    )


@lru_cache(maxsize=1)
def _sentiment_classifier():
    return pipeline(
        task="sentiment-analysis",
        model=SENTIMENT_MODEL_NAME,
    )


def _classify_text(text: str, labels: list[str]) -> tuple[str, float]:
    clf = _topic_classifier()
    result = clf(text, candidate_labels=labels, multi_label=False)
    top_label = result["labels"][0]
    top_score = float(result["scores"][0])
    return top_label, round(top_score, 4)


def _normalize_sentiment_label(raw_label: str) -> str:
    label = raw_label.lower()
    if "positive" in label:
        return "positive"
    if "negative" in label:
        return "negative"
    return "neutral"


def _sentiment_text(text: str) -> tuple[str, float]:
    clf = _sentiment_classifier()
    result = clf(text)[0]
    label = _normalize_sentiment_label(result["label"])
    score = round(float(result["score"]), 4)
    return label, score


@app.get("/health")
def health() -> dict[str, object]:
    return {
        "status": "ok",
        "topic_model": TOPIC_MODEL_NAME,
        "sentiment_model": SENTIMENT_MODEL_NAME,
    }


@app.post("/classify", response_model=ClassifyResponse)
def classify(payload: ClassifyRequest) -> ClassifyResponse:
    labels = [lbl.strip() for lbl in payload.labels if lbl and lbl.strip()]
    if not labels:
        raise HTTPException(status_code=400, detail="labels ne peut pas etre vide")

    topic, confidence = _classify_text(payload.text, labels)
    return ClassifyResponse(topic=topic, confidence=confidence, model=TOPIC_MODEL_NAME)


@app.post("/sentiment", response_model=SentimentResponse)
def sentiment(payload: SentimentRequest) -> SentimentResponse:
    label, score = _sentiment_text(payload.text)
    return SentimentResponse(label=label, score=score, model=SENTIMENT_MODEL_NAME)


@app.post("/enrich", response_model=EnrichResponse)
def enrich(payload: EnrichRequest) -> EnrichResponse:
    labels = [lbl.strip() for lbl in payload.labels if lbl and lbl.strip()]
    if not labels:
        raise HTTPException(status_code=400, detail="labels ne peut pas etre vide")

    items: list[EnrichItem] = []
    for article in payload.batch:
        topic, confidence = _classify_text(article.body, labels)
        sentiment_label, sentiment_score = _sentiment_text(article.body)
        items.append(
            EnrichItem(
                id=article.id,
                url=article.url,
                topic=topic,
                confidence=confidence,
                sentiment_label=sentiment_label,
                sentiment_score=sentiment_score,
            )
        )

    return EnrichResponse(
        count=len(items),
        items=items,
        model=f"{TOPIC_MODEL_NAME} + {SENTIMENT_MODEL_NAME}",
    )
