from dataclasses import dataclass
from typing import Any

import psycopg
from pgvector.psycopg import register_vector

from edgar_etl.config import Settings
from edgar_etl.embed import embed_texts, query_prompt_name
from edgar_etl.paradedb_search import is_bm25_ready


@dataclass
class SearchResult:
    content: str
    distance: float
    accession_number: str
    chunk_index: int
    metadata: dict[str, Any]


@dataclass
class TextSearchResult:
    content: str
    rank: float
    accession_number: str
    chunk_index: int
    metadata: dict[str, Any]


def search_filings_text(
    query: str,
    settings: Settings,
    *,
    top_k: int = 5,
    ticker: str | None = None,
    form: str | None = None,
    match_all: bool = False,
) -> list[TextSearchResult]:
    op = "&&&" if match_all else "|||"
    conditions = [f"c.content {op} %s"]
    params: list[Any] = [query]

    if ticker:
        conditions.append("f.ticker = %s")
        params.append(ticker.upper())
    if form:
        conditions.append("f.form = %s")
        params.append(form.upper())

    params.append(top_k)
    where_clause = " AND ".join(conditions)
    sql = f"""
        SELECT
            c.content,
            pdb.score(c.id) AS rank,
            c.accession_number,
            c.chunk_index,
            c.metadata
        FROM filing_chunks c
        JOIN filings f ON f.accession_number = c.accession_number
        WHERE {where_clause}
        ORDER BY pdb.score(c.id) DESC
        LIMIT %s
    """

    with psycopg.connect(settings.database_url) as conn:
        if not is_bm25_ready(conn):
            raise RuntimeError(
                "ParadeDB pg_search BM25 index is not ready "
                "(run sql/003_paradedb_pgsearch.sql or edgar-etl init-db)"
            )
        rows = conn.execute(sql, params).fetchall()

    return [
        TextSearchResult(
            content=row[0],
            rank=float(row[1]),
            accession_number=row[2],
            chunk_index=row[3],
            metadata=row[4] if isinstance(row[4], dict) else {},
        )
        for row in rows
    ]


def format_text_results(results: list[TextSearchResult]) -> str:
    if not results:
        return "No matching chunks found."

    parts: list[str] = []
    for index, result in enumerate(results, start=1):
        meta = result.metadata
        header = (
            f"[{index}] {meta.get('ticker', '?')} {meta.get('form', '?')} "
            f"({result.accession_number}, chunk {result.chunk_index}) "
            f"rank={result.rank:.4f}"
        )
        if meta.get("section"):
            header += f" | {meta['section']}"
        parts.append(header)
        parts.append(result.content.strip())
        parts.append("")

    return "\n".join(parts).rstrip()


def search_filings(
    question: str,
    settings: Settings,
    *,
    top_k: int = 5,
    ticker: str | None = None,
    form: str | None = None,
) -> list[SearchResult]:
    query_vector = embed_texts(
        [question],
        model_name=settings.embedding_model,
        batch_size=1,
        device=settings.embedding_device,
        max_seq_length=settings.embedding_max_seq_length,
        prompt_name=query_prompt_name(settings),
        settings=settings,
    )[0]

    conditions = ["TRUE"]
    params: list[Any] = [query_vector]

    if ticker:
        conditions.append("f.ticker = %s")
        params.append(ticker.upper())
    if form:
        conditions.append("f.form = %s")
        params.append(form.upper())

    params.append(top_k)
    where_clause = " AND ".join(conditions)
    sql = f"""
        SELECT
            c.content,
            c.embedding <=> %s::vector AS distance,
            c.accession_number,
            c.chunk_index,
            c.metadata
        FROM filing_chunks c
        JOIN filings f ON f.accession_number = c.accession_number
        WHERE {where_clause}
        ORDER BY distance
        LIMIT %s
    """

    with psycopg.connect(settings.database_url) as conn:
        register_vector(conn)
        rows = conn.execute(sql, params).fetchall()

    return [
        SearchResult(
            content=row[0],
            distance=float(row[1]),
            accession_number=row[2],
            chunk_index=row[3],
            metadata=row[4] if isinstance(row[4], dict) else {},
        )
        for row in rows
    ]


def format_results(results: list[SearchResult]) -> str:
    if not results:
        return "No matching chunks found."

    parts: list[str] = []
    for index, result in enumerate(results, start=1):
        meta = result.metadata
        header = (
            f"[{index}] {meta.get('ticker', '?')} {meta.get('form', '?')} "
            f"({result.accession_number}, chunk {result.chunk_index}) "
            f"distance={result.distance:.4f}"
        )
        if meta.get("section"):
            header += f" | {meta['section']}"
        parts.append(header)
        parts.append(result.content.strip())
        parts.append("")

    return "\n".join(parts).rstrip()
