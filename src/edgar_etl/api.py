from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import psycopg
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from edgar_etl.config import Settings
from edgar_etl.embed import get_embedding_model
from edgar_etl.query import SearchResult, search_filings

_STATIC_DIR = Path(__file__).resolve().parent / "static"


class SearchResultOut(BaseModel):
    content: str
    distance: float
    similarity: float
    accession_number: str
    chunk_index: int
    metadata: dict[str, Any]


class SearchResponse(BaseModel):
    query: str
    top_k: int
    count: int
    results: list[SearchResultOut]


class StatsResponse(BaseModel):
    filing_count: int
    chunk_count: int


def _to_result_out(result: SearchResult) -> SearchResultOut:
    return SearchResultOut(
        content=result.content,
        distance=result.distance,
        similarity=round(1.0 - result.distance, 4),
        accession_number=result.accession_number,
        chunk_index=result.chunk_index,
        metadata=result.metadata,
    )


def create_app(settings: Settings | None = None) -> FastAPI:
    app_settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        get_embedding_model(app_settings.embedding_model)
        yield

    app = FastAPI(
        title="SEC EDGAR Semantic Search",
        description="Verify pgvector data with semantic search over filing chunks.",
        lifespan=lifespan,
    )

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return (_STATIC_DIR / "index.html").read_text(encoding="utf-8")

    @app.get("/api/stats", response_model=StatsResponse)
    def stats() -> StatsResponse:
        with psycopg.connect(app_settings.database_url) as conn:
            filing_count = conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
            chunk_count = conn.execute("SELECT COUNT(*) FROM filing_chunks").fetchone()[0]
        return StatsResponse(filing_count=filing_count, chunk_count=chunk_count)

    @app.get("/api/search", response_model=SearchResponse)
    def search(
        q: str = Query(..., min_length=1, description="Search term or question"),
        top_k: int = Query(10, ge=1, le=100, description="Number of chunks to return"),
        ticker: str | None = Query(None, description="Filter by ticker, e.g. AEE"),
        form: str | None = Query(None, description="Filter by form, e.g. 10-Q"),
    ) -> SearchResponse:
        query = q.strip()
        if not query:
            raise HTTPException(status_code=400, detail="Query cannot be empty")

        try:
            results = search_filings(
                query,
                app_settings,
                top_k=top_k,
                ticker=ticker,
                form=form,
            )
        except psycopg.OperationalError as exc:
            raise HTTPException(
                status_code=503,
                detail=f"Database unavailable: {exc}",
            ) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Search failed: {exc}",
            ) from exc

        return SearchResponse(
            query=query,
            top_k=top_k,
            count=len(results),
            results=[_to_result_out(result) for result in results],
        )

    return app
