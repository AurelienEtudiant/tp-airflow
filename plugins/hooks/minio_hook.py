"""
MinioHook — wrapper autour du provider Amazon S3 d'Airflow
pour le stockage des articles bruts dans MinIO.
"""

import gzip
import json
import logging
from datetime import datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

MINIO_CONN_ID = "minio_s3"
DEFAULT_BUCKET = "newsradar-raw"


class MinioHook(S3Hook):
    """
    Hook Airflow pour MinIO (S3-compatible).
    Hérite de S3Hook (provider officiel Amazon).

    Configuration Connection :
        conn_id     : minio_s3
        conn_type   : Amazon Web Services
        extra       : {"endpoint_url": "http://minio:9000", "region_name": "us-east-1"}
        login       : minioadmin
        password    : minioadmin
    """

    conn_name_attr = "aws_conn_id"
    default_conn_name = MINIO_CONN_ID

    def __init__(self, aws_conn_id: str = MINIO_CONN_ID):
        super().__init__(aws_conn_id=aws_conn_id)

    # ------------------------------------------------------------------ #
    # Helpers métier                                                        #
    # ------------------------------------------------------------------ #

    @staticmethod
    def build_key(source_id: str, article_id: str, published_at: datetime | None = None) -> str:
        """
        Construit la clé S3 pour un article.
        Format : raw/{source_id}/{YYYY}/{MM}/{DD}/{article_id}.json.gz
        """
        dt = published_at or datetime.utcnow()
        return (
            f"raw/{source_id}/{dt.year:04d}/{dt.month:02d}/{dt.day:02d}"
            f"/{article_id}.json.gz"
        )

    def store_article(
        self,
        article: dict,
        source_id: str,
        article_id: str,
        published_at: datetime | None = None,
        bucket: str = DEFAULT_BUCKET,
    ) -> str:
        """
        Compresse et stocke un article JSON dans MinIO.
        Retourne le chemin S3 (s3://<bucket>/<key>).
        """
        key = self.build_key(source_id, article_id, published_at)
        body = gzip.compress(json.dumps(article, ensure_ascii=False, default=str).encode("utf-8"))
        self.load_bytes(
            bytes_data=body,
            key=key,
            bucket_name=bucket,
            replace=True,
        )
        s3_path = f"s3://{bucket}/{key}"
        logger.debug(f"Article stocké : {s3_path}")
        return s3_path

    def load_article(self, s3_path: str) -> dict:
        """
        Charge et décompresse un article depuis MinIO.
        Accepte un chemin complet s3://bucket/key ou juste la key.
        """
        if s3_path.startswith("s3://"):
            parts = s3_path[5:].split("/", 1)
            bucket, key = parts[0], parts[1]
        else:
            bucket, key = DEFAULT_BUCKET, s3_path

        obj = self.get_key(key, bucket_name=bucket)
        raw = obj.get()["Body"].read()
        return json.loads(gzip.decompress(raw).decode("utf-8"))

    def bulk_store_articles(
        self,
        articles: list[dict],
        source_id: str,
        bucket: str = DEFAULT_BUCKET,
    ) -> list[str]:
        """
        Stocke une liste d'articles et retourne leurs chemins S3.
        Chaque article doit avoir 'article_id' et optionnellement 'published_at'.
        """
        paths = []
        for article in articles:
            article_id = article["article_id"]
            published_at = article.get("published_at")
            if isinstance(published_at, str):
                try:
                    published_at = datetime.fromisoformat(published_at)
                except ValueError:
                    published_at = None
            path = self.store_article(article, source_id, article_id, published_at, bucket)
            paths.append(path)
        logger.info(f"bulk_store_articles : {len(paths)} articles stockés dans {bucket}")
        return paths
