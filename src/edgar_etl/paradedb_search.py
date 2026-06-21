"""ParadeDB pg_search (BM25) helpers for filing_chunks full-text search."""

from __future__ import annotations

import psycopg

BM25_INDEX_NAME = "idx_filing_chunks_bm25"
BM25_KEY_FIELD = "id"
BM25_INDEX_COLUMNS = ("id", "content", "metadata", "accession_number", "chunk_index")
BM25_INDEX_DDL = f"""
CREATE INDEX {BM25_INDEX_NAME} ON filing_chunks
USING bm25 ({", ".join(BM25_INDEX_COLUMNS)})
WITH (key_field = '{BM25_KEY_FIELD}')
""".strip()


def pg_search_extension_version(conn: psycopg.Connection) -> str | None:
    row = conn.execute(
        "SELECT extversion FROM pg_extension WHERE extname = 'pg_search'"
    ).fetchone()
    return row[0] if row else None


def bm25_index_definition(conn: psycopg.Connection) -> str | None:
    row = conn.execute(
        """
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = 'public' AND indexname = %s
        """,
        (BM25_INDEX_NAME,),
    ).fetchone()
    return row[0] if row else None


def truncate_bm25_index(database_url: str) -> None:
    """Remove all BM25-indexed documents (clears filing_chunks rows)."""
    with psycopg.connect(database_url) as conn:
        conn.execute("TRUNCATE TABLE filing_chunks")
        conn.commit()


def pg_search_extension_installed(conn: psycopg.Connection) -> bool:
    row = conn.execute(
        "SELECT 1 FROM pg_extension WHERE extname = 'pg_search'"
    ).fetchone()
    return row is not None


def bm25_index_exists(conn: psycopg.Connection) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = 'public' AND indexname = %s
        """,
        (BM25_INDEX_NAME,),
    ).fetchone()
    return row is not None


def is_bm25_ready(conn: psycopg.Connection) -> bool:
    return pg_search_extension_installed(conn) and bm25_index_exists(conn)
