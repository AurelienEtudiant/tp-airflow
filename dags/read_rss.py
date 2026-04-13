from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from operators.rss_consumer import RSSArticleConsumerOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "rss_consumer",
    default_args=default_args,
    description="Consomme les articles depuis Kafka",
    schedule="*/1 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["kafka", "articles"],
)

consume_task = RSSArticleConsumerOperator(
    task_id="consume_articles",
    kafka_bootstrap_servers=["kafka:29092"],
    kafka_topic="articles",
    kafka_group_id="airflow-rss-consumer",
    max_messages=10,
    dag=dag,
)

consume_task
