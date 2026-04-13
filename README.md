# tp-airflow — ingestion RSS / Kafka / MinIO

Stack **tp-airflow-ingest** : Airflow, Kafka, **MinIO** (`data-lake`), DAGs **`rss_producer`** / **`rss_consumer`**, NLP, etc.

## Vue d’ensemble du module TP

Le contexte global (liaison avec **Projet_Airflow**, ports, enchaînement) est décrit dans le README du dossier parent :

**[`../README.md`](../README.md)**

## Lancement (rappel)

```powershell
docker compose -f compose.yaml -f compose.parallel-projet.yaml up -d
```

- Airflow UI : **http://localhost:38080**  
- MinIO : API **39000**, console **39001**

Les commentaires en tête de **`compose.yaml`** décrivent les profils optionnels (`search`, `ui`, `seed`).
