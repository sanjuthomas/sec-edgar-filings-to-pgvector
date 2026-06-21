from unittest.mock import ANY, patch

import pytest
from fastapi.testclient import TestClient

from edgar_etl.admin_api import create_admin_app
from edgar_etl.config import Settings
from edgar_etl.kafka_manager import KafkaConsumerManager


@pytest.fixture
def admin_client() -> TestClient:
    settings = Settings(database_url="postgresql://invalid/test")
    manager = KafkaConsumerManager(settings)
    app = create_admin_app(settings, kafka_manager=manager)
    with TestClient(app) as client:
        client.app.state.test_manager = manager
        yield client


def test_admin_index_returns_html(admin_client: TestClient) -> None:
    response = admin_client.get("/")
    assert response.status_code == 200
    assert "SEC EDGAR ETL Admin" in response.text
    assert 'id="truncate-btn"' in response.text
    assert "pg_search — BM25 keyword search" in response.text
    assert 'id="load-embedding-backend"' in response.text


@patch("edgar_etl.admin_api.get_paradedb_config")
@patch("edgar_etl.admin_api.get_database_schema")
@patch("edgar_etl.admin_api.get_connectivity")
@patch("edgar_etl.admin_api.is_bm25_ready", return_value=True)
@patch("edgar_etl.admin_api.psycopg.connect")
def test_admin_status(
    mock_connect,
    _mock_bm25,
    mock_connectivity,
    mock_schema,
    mock_paradedb,
    admin_client: TestClient,
) -> None:
    from edgar_etl.admin_service import ParadeDbConfigInfo, SchemaColumnInfo, SchemaTableInfo
    from edgar_etl.connectivity import ServiceStatus

    mock_paradedb.return_value = ParadeDbConfigInfo(
        engine="ParadeDB pg_search",
        extension="pg_search",
        extension_installed=True,
        extension_version="0.15.0",
        table="public.filing_chunks",
        index_name="idx_filing_chunks_bm25",
        index_exists=True,
        index_type="BM25",
        key_field="id",
        indexed_columns=["id", "content", "metadata", "accession_number", "chunk_index"],
        indexed_chunks=100,
        rank_function="pdb.score(id)",
        query_operator="||| (match any) / &&& (match all)",
        example_query="SELECT content FROM filing_chunks WHERE content ||| 'revenue'",
        index_definition="CREATE INDEX idx_filing_chunks_bm25 ON public.filing_chunks USING bm25 (...)",
        docker_image="paradedb/paradedb:latest-pg17",
    )

    mock_connectivity.return_value = [
        ServiceStatus("postgres", True, "connected"),
        ServiceStatus("paradedb", True, "BM25 index ready, 100 indexed chunks"),
        ServiceStatus("mongodb", True, "connected"),
        ServiceStatus("kafka", True, "connected"),
        ServiceStatus("kafka_consumer", True, "ready"),
    ]
    mock_conn = mock_connect.return_value.__enter__.return_value
    mock_conn.execute.return_value.fetchone.side_effect = [(5,), (100,)]
    mock_schema.return_value = [
        SchemaTableInfo(
            name="filing_chunks",
            columns=[
                SchemaColumnInfo(
                    name="embedding",
                    type="vector(1024)",
                    nullable=False,
                    notes="Dense vector",
                )
            ],
            relationships=["Child table"],
        )
    ]

    response = admin_client.get("/api/admin/status")
    assert response.status_code == 200
    data = response.json()
    assert data["filing_count"] == 5
    assert data["chunk_count"] == 100
    assert data["bm25_indexed_chunks"] == 100
    assert data["bm25_ready"] is True
    assert data["kafka"]["state"] == "stopped"
    assert data["embedding"]["model"] == "BAAI/bge-m3"
    assert data["embedding"]["backend"] == "embedded"
    assert data["embedding"]["backend_label"] == "Embedded — BGE-M3"
    assert data["paradedb"]["engine"] == "ParadeDB pg_search"
    assert data["paradedb"]["extension"] == "pg_search"
    assert data["paradedb"]["extension_version"] == "0.15.0"
    assert data["paradedb"]["index_name"] == "idx_filing_chunks_bm25"
    assert data["paradedb"]["indexed_chunks"] == 100
    assert data["embedding"]["dimensions"] == 1024
    assert data["db_schema"][0]["name"] == "filing_chunks"
    assert len(data["pipeline"]) >= 3


@patch("edgar_etl.admin_api.update_embedding_backend")
def test_admin_set_embedding_backend(mock_update, admin_client: TestClient) -> None:
    from edgar_etl.admin_service import EmbeddingConfigInfo

    mock_update.return_value = EmbeddingConfigInfo(
        backend="ollama",
        backend_label="Ollama — BGE-M3",
        model="bge-m3",
        device="host GPU via Ollama",
        dimensions=1024,
        max_seq_length=512,
        batch_size=16,
        similarity="cosine",
        library="Ollama /api/embed",
        query_prompt=None,
        ollama_base_url="http://host.docker.internal:11434",
    )

    response = admin_client.post(
        "/api/admin/embedding-backend",
        json={"backend": "ollama"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["embedding"]["backend"] == "ollama"
    assert data["embedding"]["model"] == "bge-m3"
    assert "Ollama" in data["message"]
    mock_update.assert_called_once_with(ANY, "ollama")


@patch("edgar_etl.admin_api.truncate_table", return_value="Truncated table filing_chunks")
def test_admin_truncate(mock_truncate, admin_client: TestClient) -> None:
    response = admin_client.post(
        "/api/admin/truncate",
        json={"table": "filing_chunks"},
    )
    assert response.status_code == 200
    assert response.json()["table"] == "filing_chunks"
    mock_truncate.assert_called_once()


@patch(
    "edgar_etl.admin_api.truncate_table",
    return_value="Truncated pg_search BM25 index data (cleared all filing_chunks rows; pgvector embeddings removed too)",
)
def test_admin_truncate_pg_search_bm25(mock_truncate, admin_client: TestClient) -> None:
    response = admin_client.post(
        "/api/admin/truncate",
        json={"table": "pg_search_bm25"},
    )
    assert response.status_code == 200
    assert response.json()["table"] == "pg_search_bm25"
    assert "BM25 index data" in response.json()["message"]
    mock_truncate.assert_called_once_with(ANY, "pg_search_bm25")


@patch("edgar_etl.admin_api.load_ticker")
@patch("edgar_etl.admin_api.update_embedding_backend")
def test_admin_load_ticker(mock_update_backend, mock_load, admin_client: TestClient) -> None:
    from edgar_etl.admin_service import TickerLoadResult

    mock_load.return_value = TickerLoadResult(
        ticker="KKR",
        found=2,
        processed=2,
        skipped=0,
        failed=0,
        total_chunks=400,
        errors=[],
    )

    response = admin_client.post(
        "/api/admin/load-ticker",
        json={"ticker": "kkr", "backend": "embedded"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["ticker"] == "KKR"
    assert data["total_chunks"] == 400
    mock_update_backend.assert_called_once_with(ANY, "embedded")
    mock_load.assert_called_once_with(ANY, "kkr", forms=None)


@patch("edgar_etl.admin_api.load_ticker")
@patch("edgar_etl.admin_api.update_embedding_backend")
def test_admin_load_ticker_with_forms(mock_update_backend, mock_load, admin_client: TestClient) -> None:
    from edgar_etl.admin_service import TickerLoadResult

    mock_load.return_value = TickerLoadResult(
        ticker="GS",
        found=10,
        processed=10,
        skipped=0,
        failed=0,
        total_chunks=100,
        errors=[],
    )

    response = admin_client.post(
        "/api/admin/load-ticker",
        json={"ticker": "gs", "forms": ["10-Q", "8-K"], "backend": "ollama"},
    )
    assert response.status_code == 200
    mock_update_backend.assert_called_once_with(ANY, "ollama")
    mock_load.assert_called_once_with(
        ANY,
        "gs",
        forms=["10-Q", "8-K"],
    )


def test_admin_load_ticker_requires_backend(admin_client: TestClient) -> None:
    response = admin_client.post(
        "/api/admin/load-ticker",
        json={"ticker": "gs"},
    )
    assert response.status_code == 422


def test_admin_kafka_start_and_stop(admin_client: TestClient) -> None:
    manager: KafkaConsumerManager = admin_client.app.state.test_manager

    with patch.object(manager, "start") as mock_start, patch.object(
        manager,
        "status",
        return_value={
            "state": "running",
            "offset_mode": "earliest",
            "topic": "filings",
            "group_id": "edgar-pgvector-etl",
            "last_error": None,
        },
    ):
        response = admin_client.post(
            "/api/admin/kafka/start",
            json={"offset": "earliest"},
        )
        assert response.status_code == 200
        mock_start.assert_called_once_with("earliest")

    with patch.object(manager, "stop") as mock_stop, patch.object(
        manager,
        "status",
        return_value={
            "state": "stopped",
            "offset_mode": None,
            "topic": "filings",
            "group_id": "edgar-pgvector-etl",
            "last_error": None,
        },
    ):
        response = admin_client.post("/api/admin/kafka/stop")
        assert response.status_code == 200
        mock_stop.assert_called_once()
