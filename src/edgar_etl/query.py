from dataclasses import dataclass
from typing import Any

import psycopg
from pgvector.psycopg import register_vector

from edgar_etl.config import Settings
from edgar_etl.embed import embed_texts


@dataclass
class SearchResult:
    content: str
    distance: float
    accession_number: str
    chunk_index: int
    metadata: dict[str, Any]


def search_filings(
    question: str,
    settings: Settings,
    *,
    top_k: int = 5,
    ticker: str | None = None,
    form: str | None = None,
) -> list[SearchResult]:
    prompt_name = "query" if "bge-m3" in settings.embedding_model.lower() else None
    query_vector = embed_texts(
        [question],
        model_name=settings.embedding_model,
        batch_size=1,
        prompt_name=prompt_name,
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
