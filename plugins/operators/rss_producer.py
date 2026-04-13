from airflow.models import BaseOperator
from kafka import KafkaProducer
import json
import random
from faker import Faker
from datetime import datetime


class RSSArticleProducerOperator(BaseOperator):

    def __init__(
        self,
        kafka_bootstrap_servers=None,
        kafka_topic="articles",
        num_articles_range=(3, 50),
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.kafka_bootstrap_servers = kafka_bootstrap_servers or ["kafka:29092"]
        self.kafka_topic = kafka_topic
        self.num_articles_range = num_articles_range

    def execute(self, context):
        fake = Faker()

        producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        num_articles = random.randint(
            self.num_articles_range[0], self.num_articles_range[1]
        )

        for _ in range(num_articles):
            article = {
                "title": fake.sentence(nb_words=8),
                "date": fake.date_time_this_month().isoformat(),
                "body": fake.paragraph(nb_sentences=5),
                "pub_date": datetime.now().isoformat(),
            }

            producer.send(self.kafka_topic, value=article)
            self.log.info(f"Article envoyé: {article['title']}")

        producer.flush()
        producer.close()

        return {"status": "success", "articles_count": num_articles}
