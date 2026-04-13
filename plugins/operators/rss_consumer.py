from airflow.models import BaseOperator
from kafka import KafkaConsumer
import json
from datetime import datetime
from io import BytesIO
import sys
import os
import time
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from hooks.minio_hook import MinioHook


class RSSArticleConsumerOperator(BaseOperator):

    def __init__(
        self,
        kafka_bootstrap_servers=None,
        kafka_topic="articles",
        kafka_group_id="airflow-consumer",
        max_messages=50,
        minio_conn_id="minio_default",
        minio_bucket="data-lake",
        poll_timeout_ms=5000,
        consume_time_limit_sec=120,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.kafka_bootstrap_servers = kafka_bootstrap_servers or ["kafka:29092"]
        self.kafka_topic = kafka_topic
        self.kafka_group_id = kafka_group_id
        self.max_messages = max_messages
        self.minio_conn_id = minio_conn_id
        self.minio_bucket = minio_bucket
        self.poll_timeout_ms = poll_timeout_ms
        self.consume_time_limit_sec = consume_time_limit_sec

    def execute(self, context):
        minio_hook = MinioHook(self.minio_conn_id)
        minio_client = minio_hook.get_client()

        # Générer un group_id unique pour cette exécution
        unique_group_id = f"{self.kafka_group_id}-{uuid.uuid4()}"
        self.log.info(f"Utilisation du group_id: {unique_group_id}")

        consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_bootstrap_servers,
            group_id=unique_group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            max_poll_records=self.max_messages,
            session_timeout_ms=30000,
            request_timeout_ms=60000,
            heartbeat_interval_ms=10000,
            enable_auto_commit=True,
        )

        # Ne pas utiliser « for message in consumer » : l’itérateur bloque sans fin
        # tant qu’il n’a pas assez de messages (souvent < max_messages) → timeout Airflow.
        articles = []
        deadline = time.monotonic() + self.consume_time_limit_sec
        try:
            while len(articles) < self.max_messages and time.monotonic() < deadline:
                records = consumer.poll(timeout_ms=self.poll_timeout_ms)
                if not records:
                    continue
                for _tp, batch in records.items():
                    for message in batch:
                        article = message.value
                        self.log.info(f"Article reçu: {article['title']}")
                        articles.append(article)
                        if len(articles) >= self.max_messages:
                            break
                    if len(articles) >= self.max_messages:
                        break
        finally:
            consumer.close()
            self.log.info("Consumer fermé")

        if articles:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"articles_{timestamp}.json"

            articles_json = json.dumps(articles, indent=2).encode("utf-8")
            self.log.info(f"Sauvegarde de {len(articles)} articles")

            minio_client.put_object(
                self.minio_bucket,
                file_name,
                BytesIO(articles_json),
                length=len(articles_json),
                content_type="application/json",
            )

            self.log.info(f"Batch sauvegardé dans {self.minio_bucket}/{file_name}")
        else:
            self.log.warning(f"Aucun article reçu")

        return {"status": "success", "articles_count": len(articles)}
