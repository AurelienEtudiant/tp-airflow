from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from operators.rss_producer import RSSArticleProducerOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "rss_producer",
    default_args=default_args,
    description="Produit des articles simulés dans Kafka toutes les 2 minutes",
    schedule="*/2 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["kafka", "articles"],
)

produce_task = RSSArticleProducerOperator(
    task_id="produce_articles",
    kafka_bootstrap_servers=["kafka:29092"],
    kafka_topic="articles",
    num_articles_range=(3, 5),
    dag=dag,
)

produce_task
