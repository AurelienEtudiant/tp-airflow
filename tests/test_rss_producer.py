import pytest
from unittest.mock import patch, MagicMock
import feedparser
from dags.rss_fetcher import fetch_rss_articles, normalize_article


def test_producer_rss_articles_success():
    mock_articles = [
        {
            "title": "Article 1",
            "link": "https://example.com/1",
            "summary": "Summary 1",
            "published": "Mon, 01 Jan 2024 10:00:00 GMT"
        }
    ]

    with patch('feedparser.parse') as mock_parse, \
         patch('dags.rss_fetcher.producer.send') as mock_send:

        mock_parse.return_value.entries = mock_articles

        result = fetch_rss_articles(feed_url="https://example.com/feed")

        assert result == 1
        mock_send.assert_called_once()


def test_normalize_article():
    raw_article = {
        "title": "Test Article",
        "link": "https://example.com/article",
        "summary": "Test summary",
        "published": "Mon, 01 Jan 2024 10:00:00 GMT"
    }

    normalized = normalize_article(raw_article)

    assert normalized["title"] == "Test Article"
    assert normalized["url"] == "https://example.com/article"
    assert "timestamp" in normalized
    assert normalized["source"] == "rss"


def test_producer_rss_articles_empty_feed():
    with patch('feedparser.parse') as mock_parse, \
         patch('dags.rss_fetcher.producer.send') as mock_send:

        mock_parse.return_value.entries = []

        result = fetch_rss_articles(feed_url="https://example.com/feed")

        assert result == 0
        mock_send.assert_not_called()
