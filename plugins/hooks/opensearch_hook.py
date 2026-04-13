"""
OpenSearchHook — wrapper autour d'opensearch-py
Encapsule la connexion et les opérations courantes sur l'index 'articles'.
"""

import logging
from typing import Any

from airflow.hooks.base import BaseHook
from opensearchpy import OpenSearch, helpers

logger = logging.getLogger(__name__)

ARTICLES_INDEX = "articles"


class OpenSearchHook(BaseHook):
    """
    Hook Airflow pour OpenSearch.
    Utilise la Connection Airflow 'opensearch_default' (type HTTP).

    Configuration Connection :
        conn_id : opensearch_default
        host    : opensearch
        port    : 9200
        schema  : http
    """

    conn_name_attr = "opensearch_conn_id"
    default_conn_name = "opensearch_default"

    def __init__(self, opensearch_conn_id: str = "opensearch_default"):
        super().__init__()
        self.opensearch_conn_id = opensearch_conn_id
        self._client: OpenSearch | None = None

    def get_conn(self) -> OpenSearch:
        if self._client is None:
            conn = self.get_connection(self.opensearch_conn_id)
            host = conn.host or "opensearch"
            port = conn.port or 9200
            self._client = OpenSearch(
                hosts=[{"host": host, "port": int(port)}],
                use_ssl=False,
                verify_certs=False,
                timeout=30,
            )
            logger.info(f"Connexion OpenSearch : {host}:{port}")
        return self._client

    def health(self) -> dict:
        return self.get_conn().cluster.health()

    def index_document(self, doc: dict, doc_id: str | None = None) -> dict:
        """Indexe un document unique."""
        return self.get_conn().index(
            index=ARTICLES_INDEX,
            body=doc,
            id=doc_id,
            refresh=False,
        )

    def bulk_index(self, documents: list[dict]) -> tuple[int, list]:
        """
        Indexation en bulk (5-10x plus rapide qu'article par article).
        Chaque document doit avoir un champ 'article_id' utilisé comme _id.
        Retourne (success_count, errors).
        """
        if not documents:
            return 0, []

        actions = [
            {
                "_index": ARTICLES_INDEX,
                "_id": doc["article_id"],
                "_source": doc,
            }
            for doc in documents
        ]

        success, errors = helpers.bulk(
            self.get_conn(),
            actions,
            raise_on_error=False,
            raise_on_exception=False,
        )
        if errors:
            logger.warning(f"Bulk index : {len(errors)} erreurs sur {len(documents)} documents")
        logger.info(f"Bulk index : {success}/{len(documents)} documents indexés")
        return success, errors

    def count(self) -> int:
        result = self.get_conn().count(index=ARTICLES_INDEX)
        return result["count"]

    def search(self, query: dict, size: int = 10) -> dict:
        return self.get_conn().search(index=ARTICLES_INDEX, body=query, size=size)

    def delete_index(self) -> None:
        if self.get_conn().indices.exists(index=ARTICLES_INDEX):
            self.get_conn().indices.delete(index=ARTICLES_INDEX)
            logger.info(f"Index {ARTICLES_INDEX} supprimé")
