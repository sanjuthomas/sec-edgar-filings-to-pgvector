"""Verify filing_chunks.content is stored and searchable via ParadeDB BM25."""

from datetime import date, datetime, timezone

import psycopg
import pytest

from edgar_etl.config import Settings
from edgar_etl.models import FilingDownloadedEvent, TextChunk
from edgar_etl.query import search_filings_text
from edgar_etl.store import FilingStore


@pytest.fixture
def db_settings() -> Settings:
    settings = Settings(database_url="postgresql://postgres:postgres@localhost:5433/edgar")
    try:
        with psycopg.connect(settings.database_url, connect_timeout=2) as conn:
            if not conn.execute(
                "SELECT 1 FROM pg_indexes WHERE indexname = 'idx_filing_chunks_bm25'"
            ).fetchone():
                pytest.skip("BM25 index not ready")
    except psycopg.Error:
        pytest.skip("postgres not available at localhost:5433")
    return settings


def test_upserted_chunk_text_is_bm25_searchable(db_settings: Settings) -> None:
    store = FilingStore(db_settings.database_url)
    accession = "bm25-pytest-accession"

    event = FilingDownloadedEvent(
        event_type="filing.downloaded",
        schema_version=1,
        accession_number=accession,
        ticker="TST",
        company_name="Test Co",
        form="10-K",
        filing_date=date(2025, 12, 31),
        document_url="https://example.com",
        local_path="/tmp/test.htm",
        downloaded_at=datetime.now(timezone.utc),
    )
    chunks = [
        TextChunk(
            chunk_index=0,
            content="Revenue increased twelve percent year over year.",
            section="Item 7",
            metadata={"section": "Item 7"},
        )
    ]

    try:
        store.upsert_filing(event, chunks, [[0.0] * 1024])

        with store.connect() as conn:
            stored = conn.execute(
                "SELECT content FROM filing_chunks WHERE accession_number = %s",
                (accession,),
            ).fetchone()
        assert stored is not None
        assert stored[0] == chunks[0].content

        results = search_filings_text("revenue growth", db_settings, top_k=5)
        assert any(r.accession_number == accession for r in results)
    finally:
        with store.connect() as conn:
            conn.execute("DELETE FROM filings WHERE accession_number = %s", (accession,))
            conn.commit()
