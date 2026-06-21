from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from edgar_etl.api import create_app
from edgar_etl.config import Settings
from edgar_etl.query import SearchResult


@pytest.fixture
def client() -> TestClient:
    with patch("edgar_etl.api.get_embedding_model"):
        app = create_app(Settings(database_url="postgresql://invalid/test"))
        with TestClient(app) as test_client:
            yield test_client


def test_index_returns_html(client: TestClient) -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert "SEC EDGAR Semantic Search" in response.text
    assert 'id="search-form"' in response.text


@patch("edgar_etl.paradedb_search.is_bm25_ready", return_value=True)
@patch("edgar_etl.api.psycopg.connect")
def test_stats(mock_connect, _mock_bm25, client: TestClient) -> None:
    mock_conn = mock_connect.return_value.__enter__.return_value
    mock_conn.execute.return_value.fetchone.side_effect = [(42,), (1000,)]

    response = client.get("/api/stats")
    assert response.status_code == 200
    assert response.json() == {
        "filing_count": 42,
        "chunk_count": 1000,
        "bm25_ready": True,
    }


@patch("edgar_etl.api.search_filings")
def test_search_default_top_k(mock_search, client: TestClient) -> None:
    mock_search.return_value = [
        SearchResult(
            content="Revenue increased 12%.",
            distance=0.31,
            accession_number="0001104659-26-063184",
            chunk_index=2,
            metadata={"ticker": "AEE", "form": "10-Q"},
        )
    ]

    response = client.get("/api/search", params={"q": "revenue growth"})
    assert response.status_code == 200
    data = response.json()
    assert data["query"] == "revenue growth"
    assert data["mode"] == "semantic"
    assert data["top_k"] == 10
    assert data["count"] == 1
    assert data["results"][0]["similarity"] == 0.69
    mock_search.assert_called_once()
    assert mock_search.call_args.kwargs["top_k"] == 10


@patch("edgar_etl.api.search_filings")
def test_search_custom_top_k_and_filters(mock_search, client: TestClient) -> None:
    mock_search.return_value = []

    response = client.get(
        "/api/search",
        params={"q": "directors", "top_k": 25, "ticker": "aee", "form": "8-k"},
    )
    assert response.status_code == 200
    assert response.json()["top_k"] == 25
    mock_search.assert_called_once()
    kwargs = mock_search.call_args.kwargs
    assert kwargs["top_k"] == 25
    assert kwargs["ticker"] == "aee"
    assert kwargs["form"] == "8-k"


def test_search_rejects_empty_query(client: TestClient) -> None:
    response = client.get("/api/search", params={"q": "   "})
    assert response.status_code == 400
