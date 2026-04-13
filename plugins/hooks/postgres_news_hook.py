"""
PostgresNewsHook — wrapper autour du provider Airflow PostgreSQL
pour la base newsradar (DWH).
"""

import logging
from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

NEWS_CONN_ID = "postgres_newsradar"


class PostgresNewsHook(PostgresHook):
    """
    Hook Airflow pour la base NewsRadar.
    Hérite de PostgresHook (provider officiel).

    Configuration Connection :
        conn_id : postgres_newsradar
        host    : postgres-newsradar
        port    : 5432
        schema  : newsradar
        login   : news_user
        password: news_password
    """

    conn_name_attr = "postgres_conn_id"
    default_conn_name = NEWS_CONN_ID

    def __init__(self, postgres_conn_id: str = NEWS_CONN_ID):
        super().__init__(postgres_conn_id=postgres_conn_id)

    # ------------------------------------------------------------------ #
    # Helpers métier                                                        #
    # ------------------------------------------------------------------ #

    def article_exists(self, url_hash: str) -> bool:
        """Vérifie si un article existe déjà (déduplication)."""
        result = self.get_first(
            "SELECT 1 FROM fact_article WHERE url_hash = %s LIMIT 1",
            parameters=(url_hash,),
        )
        return result is not None

    def insert_article(self, article: dict) -> str | None:
        """
        Insère un article. Retourne l'article_id créé ou None si doublon.
        Utilise ON CONFLICT DO NOTHING pour la déduplication.
        """
        sql = """
            INSERT INTO fact_article
                (source_id, url, url_hash, title, title_hash, body,
                 published_at, s3_path)
            VALUES
                (%(source_id)s, %(url)s, %(url_hash)s, %(title)s,
                 %(title_hash)s, %(body)s, %(published_at)s, %(s3_path)s)
            ON CONFLICT (url_hash) DO NOTHING
            RETURNING article_id
        """
        result = self.get_first(sql, parameters=article)
        if result:
            return str(result[0])
        return None  # doublon ignoré

    def bulk_insert_articles(self, articles: list[dict]) -> int:
        """
        Insère une liste d'articles en une transaction.
        Retourne le nombre d'articles effectivement insérés (hors doublons).
        """
        if not articles:
            return 0

        inserted = 0
        conn = self.get_conn()
        with conn.cursor() as cur:
            for article in articles:
                cur.execute(
                    """
                    INSERT INTO fact_article
                        (source_id, url, url_hash, title, title_hash, body,
                         published_at, s3_path)
                    VALUES
                        (%(source_id)s, %(url)s, %(url_hash)s, %(title)s,
                         %(title_hash)s, %(body)s, %(published_at)s, %(s3_path)s)
                    ON CONFLICT (url_hash) DO NOTHING
                    """,
                    article,
                )
                if cur.rowcount:
                    inserted += 1
        conn.commit()
        logger.info(f"bulk_insert_articles : {inserted}/{len(articles)} insérés")
        return inserted

    def get_articles_to_enrich(self, limit: int = 100) -> list[dict]:
        """Retourne les articles non encore enrichis."""
        rows = self.get_records(
            """
            SELECT article_id, title, body, source_id
            FROM   fact_article
            WHERE  enriched = FALSE
            ORDER BY ingested_at
            LIMIT %s
            """,
            parameters=(limit,),
        )
        return [
            {
                "article_id": str(r[0]),
                "title": r[1],
                "body": r[2],
                "source_id": r[3],
            }
            for r in rows
        ]

    def get_articles_to_index(self, limit: int = 500) -> list[dict]:
        """Retourne les articles enrichis mais pas encore indexés dans OpenSearch."""
        rows = self.get_records(
            """
            SELECT
                fa.article_id, fa.title, fa.body, fa.url,
                fa.published_at, fa.source_id, fa.s3_path,
                fe.topic, fe.topic_confidence,
                fe.sentiment_label, fe.sentiment_score,
                fe.language, fe.word_count
            FROM   fact_article fa
            LEFT JOIN fact_enrichment fe USING (article_id)
            WHERE  fa.indexed = FALSE
              AND  fa.enriched = TRUE
            ORDER BY fa.ingested_at
            LIMIT %s
            """,
            parameters=(limit,),
        )
        return [
            {
                "article_id": str(r[0]),
                "title": r[1],
                "body": r[2],
                "url": r[3],
                "published_at": r[4].isoformat() if r[4] else None,
                "source_id": r[5],
                "s3_path": r[6],
                "topic": r[7],
                "topic_confidence": r[8],
                "sentiment_label": r[9],
                "sentiment_score": r[10],
                "language": r[11],
                "word_count": r[12],
            }
            for r in rows
        ]

    def mark_enriched(self, article_ids: list[str]) -> None:
        """Marque les articles comme enrichis."""
        if not article_ids:
            return
        self.run(
            "UPDATE fact_article SET enriched = TRUE WHERE article_id = ANY(%s)",
            parameters=(article_ids,),
        )

    def mark_indexed(self, article_ids: list[str]) -> None:
        """Marque les articles comme indexés dans OpenSearch."""
        if not article_ids:
            return
        self.run(
            "UPDATE fact_article SET indexed = TRUE WHERE article_id = ANY(%s)",
            parameters=(article_ids,),
        )

    def log_ingest(
        self,
        source_id: str,
        articles_fetched: int,
        articles_new: int,
        articles_duplicate: int,
        status: str = "success",
        error_message: str | None = None,
    ) -> None:
        """Enregistre le résultat d'un cycle d'ingestion dans fact_ingest_log."""
        self.run(
            """
            INSERT INTO fact_ingest_log
                (source_id, articles_fetched, articles_new,
                 articles_duplicate, status, error_message)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            parameters=(
                source_id,
                articles_fetched,
                articles_new,
                articles_duplicate,
                status,
                error_message,
            ),
        )

    def get_active_sources(self) -> list[dict]:
        """Retourne toutes les sources actives."""
        rows = self.get_records(
            "SELECT source_id, name, type, url FROM dim_source WHERE is_active = TRUE"
        )
        return [
            {"source_id": r[0], "name": r[1], "type": r[2], "url": r[3]}
            for r in rows
        ]
