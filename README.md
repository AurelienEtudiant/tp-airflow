# Projet Newsradar — Équipe 6

## Membres
- Aurélien L — Ecriture des flux RSS (avec dags et kafka, stockage de ceux-ci dans MinIO)
- Massaer D - Ecriture du service NLP (classification zero-shot + sentiment) et intégration dans les DAGs Airflow

## Stack
- Airflow 2.7.1
- Kafka 3 (Bitnami)
- MinIO (stockage objet)
- Zookeeper 3
- Docker Compose

| DAG           | Rôle                                                                 | Schedule                    |
|---------------|----------------------------------------------------------------------|-----------------------------|
| rss_fetcher   | Récupère les flux RSS, normalise les articles et publie sur Kafka `articles` | Configurable (ex: 5 min)    |
| rss_consumer  | Consomme `articles`, traite chaque message (NLP + enrichissement) et stocke dans MinIO `articles-bucket` | Continu (polling) |

# Topics et groupes
Topic Kafka : articles
Groupe consumer : airflow-rss-consumer

# 1) Lancer les services
docker compose up -d

# 2) Attendre ~60s que tout démarre
sleep 60

# 3) Créer le topic Kafka
docker exec -it $(docker ps -qf "name=kafka") bash -c "kafka-topics.sh --create --topic articles --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1" || echo "Topic exists ou erreur."

# 4) Créer le bucket MinIO (optionnel)
docker run --rm --network host minio/mc alias set myminio http://localhost:9000 minioadmin minioadmin && docker run --rm --network host minio/mc mb myminio/articles-bucket || echo "Bucket exists."

# 5) Accéder à l'UI Airflow
# → http://localhost:8080 (admin / admin)

# 6) Vérifier les logs
docker compose logs -f airflow-scheduler

## Tests

```bash
# Lancer tous les tests
docker compose exec airflow-worker pytest tests/ -v

# Lancer les tests du fetcher RSS
docker compose exec airflow-worker pytest tests/test_rss_producer.py -v

# Lancer les tests du consumer RSS
docker compose exec airflow-worker pytest tests/test_rss_consumer.py -v

# Lancer les tests avec rapport de couverture
docker compose exec airflow-worker pytest tests/ -v --cov=dags --cov-report=html


## Choix techniques
Airflow + SequentialExecutor : orchestration simple et lisible pour le développement. Permet de gérer le consumer Kafka proprement avec close() entre les exécutions et évite les deadlocks.
Kafka (Bitnami) : découplage producteur/consommateur, garantit la durabilité des messages et facilite le scaling futur.
MinIO : stockage objet compatible S3, simple à déployer en local et migrable vers S3/GCS en production.
Commit manuel : garantit que les partitions Kafka se libèrent après traitement et upload réussi dans MinIO. Évite les accumulations d'offsets et les messages non traités.
DAG rss_consumer avec polling continu : permet de traiter les articles dès leur arrivée sur Kafka sans attendre un schedule fixe.

## Architecture

```mermaid
flowchart LR
  subgraph infra["Infrastructure (docker-compose)"]
    direction TB
    ZK["Zookeeper"]
    KAFKA["Kafka broker"]
    MINIO["MinIO"]
    AIRFLOW["Airflow (web / scheduler)"]
  end

  RSS["Sources RSS"] --> FETCHER["DAG: rss_fetcher<br/>(producer)"]
  FETCHER -->|publie sur| TOPIC["Topic: articles"]
  TOPIC -->|s'abonne| CONSUMER["DAG: rss_consumer<br/>groupe: airflow-rss-consumer"]
  CONSUMER --> PROCESS["Traitement (validation / enrichissement)"]
  PROCESS --> MINIO_BUCKET["MinIO: articles-bucket<br/>(storage)"]

  PROCESS -->|si succès → commit & fermer| COMMIT["consumer.commit()<br/>consumer.close()"]
  COMMIT -->|libère| PARTITION["Partition libérée"]

  CONSUMER -.-> EMPTY_ASSIGN["Assignment vide → partitions = []"]
  NOTE1["Problèmes courants:<br/>- enable_auto_commit=false et commit explicite<br/>- fermer consumer pour libérer partition<br/>- vérifier MINIO_ENDPOINT"]:::note
  PARTITION -.-> NOTE1

  classDef note fill:#fff3cd,stroke:#856404,color:#856404;
