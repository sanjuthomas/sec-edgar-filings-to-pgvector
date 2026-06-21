from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

import psycopg
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from edgar_etl.admin_service import (
    get_connectivity,
    get_database_schema,
    get_embedding_config,
    get_paradedb_config,
    get_pipeline_overview,
    load_ticker,
    truncate_table,
    update_embedding_backend,
)
from edgar_etl.config import Settings
from edgar_etl.connectivity import check_all
from edgar_etl.paradedb_search import is_bm25_ready
from edgar_etl.kafka_manager import KafkaConsumerManager, OffsetMode

_STATIC_DIR = Path(__file__).resolve().parent / "static"


class TruncateRequest(BaseModel):
    table: Literal["filings", "filing_chunks", "pg_search_bm25"]


class TruncateResponse(BaseModel):
    table: str
    message: str


class LoadTickerRequest(BaseModel):
    ticker: str = Field(..., min_length=1)
    forms: list[str] | None = None
    backend: Literal["embedded", "ollama"]


class LoadTickerResponse(BaseModel):
    ticker: str
    found: int
    processed: int
    skipped: int
    failed: int
    total_chunks: int
    errors: list[str]


class KafkaStartRequest(BaseModel):
    offset: OffsetMode


class ServiceStatusOut(BaseModel):
    name: str
    ok: bool
    detail: str


class KafkaStatusResponse(BaseModel):
    state: str
    offset_mode: str | None
    topic: str
    group_id: str
    last_error: str | None


class EmbeddingConfigOut(BaseModel):
    backend: str
    backend_label: str
    model: str
    device: str
    dimensions: int
    max_seq_length: int
    batch_size: int
    similarity: str
    library: str
    query_prompt: str | None
    ollama_base_url: str | None = None


class EmbeddingBackendRequest(BaseModel):
    backend: Literal["embedded", "ollama"]


class EmbeddingBackendResponse(BaseModel):
    embedding: EmbeddingConfigOut
    message: str


class SchemaColumnOut(BaseModel):
    name: str
    type: str
    nullable: bool
    notes: str | None = None


class SchemaTableOut(BaseModel):
    name: str
    columns: list[SchemaColumnOut]
    relationships: list[str]


class PipelineStepOut(BaseModel):
    title: str
    description: str


class ParadeDbConfigOut(BaseModel):
    engine: str
    extension: str
    extension_installed: bool
    extension_version: str | None
    table: str
    index_name: str
    index_exists: bool
    index_type: str
    key_field: str
    indexed_columns: list[str]
    indexed_chunks: int
    rank_function: str
    query_operator: str
    example_query: str
    index_definition: str | None
    docker_image: str


class AdminStatusResponse(BaseModel):
    services: list[ServiceStatusOut]
    kafka: KafkaStatusResponse
    filing_count: int
    chunk_count: int
    bm25_indexed_chunks: int
    bm25_ready: bool
    embedding: EmbeddingConfigOut
    paradedb: ParadeDbConfigOut
    db_schema: list[SchemaTableOut]
    pipeline: list[PipelineStepOut]


def create_admin_app(
    settings: Settings | None = None,
    *,
    kafka_manager: KafkaConsumerManager | None = None,
) -> FastAPI:
    app_settings = settings or Settings()
    consumer_manager = kafka_manager or KafkaConsumerManager(app_settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.kafka_manager = consumer_manager
        app.state.startup_checks = check_all(app_settings)
        for status in app.state.startup_checks:
            if not status.ok:
                # Logged by caller; still serve admin UI for recovery actions.
                pass
        yield
        try:
            consumer_manager.stop(timeout=10)
        except TimeoutError:
            pass

    app = FastAPI(
        title="SEC EDGAR ETL Admin",
        description="Manage pgvector ETL: truncate, manual ticker load, Kafka consumption.",
        lifespan=lifespan,
    )

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return (_STATIC_DIR / "admin.html").read_text(encoding="utf-8")

    @app.get("/api/admin/status", response_model=AdminStatusResponse)
    def admin_status() -> AdminStatusResponse:
        services = get_connectivity(app_settings)
        kafka = consumer_manager.status()
        try:
            with psycopg.connect(app_settings.database_url) as conn:
                filing_count = conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
                chunk_count = conn.execute(
                    "SELECT COUNT(*) FROM filing_chunks"
                ).fetchone()[0]
                bm25_ready = is_bm25_ready(conn)
            schema_info = get_database_schema(app_settings.database_url)
        except psycopg.Error as exc:
            raise HTTPException(
                status_code=503,
                detail=f"Database unavailable: {exc}",
            ) from exc

        dimensions = _embedding_dimensions_from_schema(schema_info)
        embedding = get_embedding_config(app_settings, dimensions=dimensions)
        paradedb = get_paradedb_config(app_settings.database_url)

        return AdminStatusResponse(
            services=[
                ServiceStatusOut(name=s.name, ok=s.ok, detail=s.detail) for s in services
            ],
            kafka=KafkaStatusResponse(**kafka),
            filing_count=filing_count,
            chunk_count=chunk_count,
            bm25_indexed_chunks=chunk_count if bm25_ready else 0,
            bm25_ready=bm25_ready,
            embedding=EmbeddingConfigOut(**embedding.__dict__),
            paradedb=ParadeDbConfigOut(**paradedb.__dict__),
            db_schema=[
                SchemaTableOut(
                    name=table.name,
                    columns=[SchemaColumnOut(**column.__dict__) for column in table.columns],
                    relationships=table.relationships,
                )
                for table in schema_info
            ],
            pipeline=[
                PipelineStepOut(**step.__dict__) for step in get_pipeline_overview()
            ],
        )

    @app.post("/api/admin/embedding-backend", response_model=EmbeddingBackendResponse)
    def set_embedding_backend_endpoint(
        request: EmbeddingBackendRequest,
    ) -> EmbeddingBackendResponse:
        try:
            embedding = update_embedding_backend(app_settings, request.backend)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        label = embedding.backend_label
        return EmbeddingBackendResponse(
            embedding=EmbeddingConfigOut(**embedding.__dict__),
            message=f"Embedding backend set to {label}",
        )

    @app.post("/api/admin/truncate", response_model=TruncateResponse)
    def truncate(request: TruncateRequest) -> TruncateResponse:
        try:
            message = truncate_table(app_settings, request.table)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except psycopg.Error as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        return TruncateResponse(
            table=request.table,
            message=message,
        )

    @app.post("/api/admin/load-ticker", response_model=LoadTickerResponse)
    def load_ticker_endpoint(request: LoadTickerRequest) -> LoadTickerResponse:
        try:
            update_embedding_backend(app_settings, request.backend)
            result = load_ticker(app_settings, request.ticker, forms=request.forms)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

        return LoadTickerResponse(
            ticker=result.ticker,
            found=result.found,
            processed=result.processed,
            skipped=result.skipped,
            failed=result.failed,
            total_chunks=result.total_chunks,
            errors=result.errors[:20],
        )

    @app.post("/api/admin/kafka/start", response_model=KafkaStatusResponse)
    def kafka_start(request: KafkaStartRequest) -> KafkaStatusResponse:
        try:
            consumer_manager.start(request.offset)
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return KafkaStatusResponse(**consumer_manager.status())

    @app.post("/api/admin/kafka/stop", response_model=KafkaStatusResponse)
    def kafka_stop() -> KafkaStatusResponse:
        try:
            consumer_manager.stop()
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc
        return KafkaStatusResponse(**consumer_manager.status())

    return app


def _embedding_dimensions_from_schema(schema_info: list) -> int | None:
    for table in schema_info:
        if table.name != "filing_chunks":
            continue
        for column in table.columns:
            if column.name == "embedding" and column.type.startswith("vector("):
                return int(column.type.removeprefix("vector(").removesuffix(")"))
    return None
