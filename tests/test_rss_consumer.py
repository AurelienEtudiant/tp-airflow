import pytest
from unittest.mock import patch, MagicMock
import json
from dags.rss_consumer import consume_articles, upload_to_minio


def test_consume_articles_success():
    mock_message = {
        "title": "Article Test",
        "url": "https://example.com/test",
        "summary": "Test summary"
    }

    with patch('dags.rss_consumer.KafkaConsumer') as mock_consumer_class, \
         patch('dags.rss_consumer.upload_to_minio') as mock_upload:

        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        mock_consumer.__iter__.return_value = [
            MagicMock(value=json.dumps(mock_message).encode())
        ]

        result = consume_articles(topic="articles", max_messages=1)

        assert result == 1
        mock_upload.assert_called_once()
        mock_consumer.close.assert_called_once()


def test_upload_to_minio_success():
    article_data = {
        "title": "Article",
        "url": "https://example.com",
        "sentiment": "positive"
    }

    with patch('dags.rss_consumer.minio_client.put_object') as mock_put:

        upload_to_minio(article_data, object_name="article_1.json")

        mock_put.assert_called_once()
        call_args = mock_put.call_args
        assert call_args[1]["bucket_name"] == "articles-bucket"


