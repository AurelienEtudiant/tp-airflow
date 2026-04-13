# Requêtes OpenSearch utiles — NewsRadar

Copiez-collez ces requêtes dans **Dev Tools** (`http://localhost:5601/app/dev_tools`).

## Santé du cluster

```
GET /_cluster/health
```

## Voir les indices

```
GET /_cat/indices?v
```

## Voir le mapping de l'index articles

```
GET /articles/_mapping
```

---

## Recherche full-text (US-02)

```json
GET /articles/_search
{
  "query": {
    "multi_match": {
      "query": "intelligence artificielle",
      "fields": ["title^2", "body"]
    }
  },
  "size": 10
}
```

## Filtrer par topic ET sentiment (US-03)

```json
GET /articles/_search
{
  "query": {
    "bool": {
      "must": [
        { "match": { "body": "intelligence" } }
      ],
      "filter": [
        { "term": { "topic": "tech" } },
        { "term": { "sentiment_label": "POSITIVE" } }
      ]
    }
  }
}
```

## Articles du jour par topic (US-01)

```json
GET /articles/_search
{
  "query": {
    "range": {
      "published_at": {
        "gte": "now-1d/d",
        "lt": "now/d"
      }
    }
  },
  "aggs": {
    "by_topic": {
      "terms": {
        "field": "topic",
        "size": 10
      }
    }
  },
  "size": 0
}
```

## Répartition sentiments par topic (US-06)

```json
GET /articles/_search
{
  "aggs": {
    "by_topic": {
      "terms": { "field": "topic" },
      "aggs": {
        "by_sentiment": {
          "terms": { "field": "sentiment_label" }
        },
        "avg_sentiment_score": {
          "avg": { "field": "sentiment_score" }
        }
      }
    }
  },
  "size": 0
}
```

## Évolution articles sur 7 jours (US-06)

```json
GET /articles/_search
{
  "query": {
    "range": {
      "published_at": {
        "gte": "now-7d/d"
      }
    }
  },
  "aggs": {
    "articles_par_jour": {
      "date_histogram": {
        "field": "published_at",
        "calendar_interval": "day"
      }
    }
  },
  "size": 0
}
```

## Compter les articles indexés

```
GET /articles/_count
```

## Reset complet (index corrompu)

```
DELETE /articles
```

Puis relancer le DAG `newsradar_index_opensearch_daily`.
