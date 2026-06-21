"""Admin operations: truncate, ticker load, connectivity."""

from __future__ import annotations

from dataclasses import dataclass

import structlog

from edgar_etl.config import Settings
from edgar_etl.connectivity import ServiceStatus, check_all
from edgar_etl.errors import FilingNotIndexableError, NonContentProcessingError
from edgar_etl.models import FilingDownloadedEvent
from edgar_etl.mongo import MongoFilingStore
from edgar_etl.embedding_runtime import BACKEND_LABELS, get_embedding_backend, set_embedding_backend
from edgar_etl.paradedb_search import (
    BM25_INDEX_COLUMNS,
    BM25_INDEX_NAME,
    BM25_KEY_FIELD,
    bm25_index_definition,
    bm25_index_exists,
    is_bm25_ready,
    pg_search_extension_installed,
    pg_search_extension_version,
    truncate_bm25_index,
)
from edgar_etl.pipeline import process_filing_event
from edgar_etl.store import FilingStore

logger = structlog.get_logger(__name__)
SUPPORTED_FORM_SELECTIONS = frozenset({"10-K", "10-Q", "8-K"})
TRUNCATABLE_TARGETS = frozenset({"filings", "filing_chunks", "pg_search_bm25"})


def _normalize_forms(forms: list[str] | None) -> list[str] | None:
    if forms is None:
        return None
    normalized: list[str] = []
    seen: set[str] = set()
    for form in forms:
        value = form.strip().upper().replace(" ", "")
        canonical = value.replace("10Q", "10-Q").replace("10K", "10-K").replace("8K", "8-K")
        if canonical not in SUPPORTED_FORM_SELECTIONS:
            raise ValueError(f"unsupported form selection: {form}")
        if canonical not in seen:
            normalized.append(canonical)
            seen.add(canonical)
    if not normalized:
        raise ValueError("at least one filing form must be selected")
    return normalized


@dataclass
class TickerLoadResult:
    ticker: str
    found: int
    processed: int
    skipped: int
    failed: int
    total_chunks: int
    errors: list[str]


def truncate_table(settings: Settings, table: str) -> str:
    if table not in TRUNCATABLE_TARGETS:
        raise ValueError(f"unsupported table: {table}")
    if table == "pg_search_bm25":
        truncate_bm25_index(settings.database_url)
        logger.info("pg_search bm25 index data truncated")
        return (
            "Truncated pg_search BM25 index data (cleared all filing_chunks rows; "
            "pgvector embeddings removed too)"
        )
    store = FilingStore(settings.database_url)
    store.truncate_table(table)
    logger.info("table truncated", table=table)
    return f"Truncated table {table}"


def load_ticker(
    settings: Settings,
    ticker: str,
    *,
    forms: list[str] | None = None,
) -> TickerLoadResult:
    normalized = ticker.strip().upper()
    if not normalized:
        raise ValueError("ticker is required")

    mongo_store = MongoFilingStore(settings)
    filing_store = FilingStore(settings.database_url)
    selected_forms = _normalize_forms(forms)
    # If forms are explicitly selected in UI, use those; otherwise fall back to env.
    allowed_forms = selected_forms or settings.allowed_forms

    try:
        docs = mongo_store.list_filings_by_ticker(
            normalized,
            allowed_forms=allowed_forms,
        )
    finally:
        mongo_store.close()

    result = TickerLoadResult(
        ticker=normalized,
        found=len(docs),
        processed=0,
        skipped=0,
        failed=0,
        total_chunks=0,
        errors=[],
    )

    for doc in docs:
        accession_number = doc.get("accession_number", "?")
        try:
            event = FilingDownloadedEvent.model_validate(
                {
                    "event_type": "filing.downloaded",
                    "schema_version": 1,
                    **doc,
                }
            )
            chunk_count = process_filing_event(
                event,
                settings,
                store=filing_store,
                skip_if_processed=False,
            )
            if chunk_count == 0:
                result.skipped += 1
            else:
                result.processed += 1
                result.total_chunks += chunk_count
        except (FilingNotIndexableError, NonContentProcessingError) as exc:
            result.failed += 1
            result.errors.append(f"{accession_number}: {exc}")
            logger.warning(
                "ticker load skipped filing",
                ticker=normalized,
                accession_number=accession_number,
                error=str(exc),
            )
        except Exception as exc:
            result.failed += 1
            result.errors.append(f"{accession_number}: {exc}")
            logger.exception(
                "ticker load failed",
                ticker=normalized,
                accession_number=accession_number,
            )

    logger.info(
        "ticker load finished",
        ticker=normalized,
        found=result.found,
        processed=result.processed,
        skipped=result.skipped,
        failed=result.failed,
        total_chunks=result.total_chunks,
    )
    return result


def get_connectivity(settings: Settings) -> list[ServiceStatus]:
    return check_all(settings)


@dataclass
class EmbeddingConfigInfo:
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


@dataclass
class SchemaColumnInfo:
    name: str
    type: str
    nullable: bool
    notes: str | None = None


@dataclass
class SchemaTableInfo:
    name: str
    columns: list[SchemaColumnInfo]
    relationships: list[str]


@dataclass
class PipelineStepInfo:
    title: str
    description: str


def get_embedding_config(
    settings: Settings,
    *,
    dimensions: int | None = None,
) -> EmbeddingConfigInfo:
    backend = get_embedding_backend(settings)
    dims = dimensions or _infer_dimensions(settings.embedding_model)
    common = {
        "backend": backend,
        "backend_label": BACKEND_LABELS[backend],
        "dimensions": dims,
        "max_seq_length": settings.embedding_max_seq_length,
        "batch_size": settings.embedding_batch_size,
        "similarity": "cosine",
    }
    if backend == "ollama":
        return EmbeddingConfigInfo(
            **common,
            model=settings.ollama_embedding_model,
            device="host GPU via Ollama",
            library="Ollama /api/embed",
            query_prompt=None,
            ollama_base_url=settings.ollama_base_url,
        )
    return EmbeddingConfigInfo(
        **common,
        model=settings.embedding_model,
        device=settings.embedding_device,
        library="sentence-transformers (in-process)",
        query_prompt="query" if "bge-m3" in settings.embedding_model.lower() else None,
        ollama_base_url=None,
    )


def update_embedding_backend(settings: Settings, backend: str) -> EmbeddingConfigInfo:
    set_embedding_backend(settings, backend)  # type: ignore[arg-type]
    return get_embedding_config(settings)


@dataclass
class ParadeDbConfigInfo:
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


def get_paradedb_config(database_url: str) -> ParadeDbConfigInfo:
    import psycopg

    with psycopg.connect(database_url) as conn:
        extension_installed = pg_search_extension_installed(conn)
        extension_version = (
            pg_search_extension_version(conn) if extension_installed else None
        )
        index_exists = bm25_index_exists(conn)
        index_definition = bm25_index_definition(conn) if index_exists else None
        indexed_chunks = (
            int(conn.execute("SELECT COUNT(*) FROM filing_chunks").fetchone()[0])
            if is_bm25_ready(conn)
            else 0
        )

    return ParadeDbConfigInfo(
        engine="ParadeDB pg_search",
        extension="pg_search",
        extension_installed=extension_installed,
        extension_version=extension_version,
        table="public.filing_chunks",
        index_name=BM25_INDEX_NAME,
        index_exists=index_exists,
        index_type="BM25",
        key_field=BM25_KEY_FIELD,
        indexed_columns=list(BM25_INDEX_COLUMNS),
        indexed_chunks=indexed_chunks,
        rank_function="pdb.score(id)",
        query_operator="||| (match any) / &&& (match all)",
        example_query=(
            "SELECT content, pdb.score(id) FROM filing_chunks "
            "WHERE content ||| 'revenue growth' "
            "ORDER BY pdb.score(id) DESC LIMIT 10"
        ),
        index_definition=index_definition,
        docker_image="paradedb/paradedb:latest-pg17",
    )


def get_bm25_indexed_chunk_count(database_url: str) -> int:
    import psycopg

    with psycopg.connect(database_url) as conn:
        if not is_bm25_ready(conn):
            return 0
        return int(conn.execute("SELECT COUNT(*) FROM filing_chunks").fetchone()[0])


def get_database_schema(database_url: str) -> list[SchemaTableInfo]:
    import psycopg

    tables = ("filings", "filing_chunks")
    column_notes = {
        "embedding": "Dense vector produced by the embedding model; used for nearest-neighbor search.",
        "metadata": "JSONB copy of ticker, form, section, and filing identifiers.",
        "accession_number": "SEC accession number; joins filings to filing_chunks.",
        "chunk_index": "Zero-based chunk order within a filing.",
        "content": "Plain-text chunk extracted from the filing HTML.",
    }
    relationships = {
        "filings": [
            "Parent table: one row per indexed filing.",
            "Primary key accession_number is referenced by filing_chunks.",
        ],
        "filing_chunks": [
            "Child table: many chunk rows per filing.",
            "accession_number -> filings.accession_number (ON DELETE CASCADE).",
            "HNSW index on embedding supports fast cosine similarity search.",
            f"ParadeDB BM25 index ({BM25_INDEX_NAME}) on content for keyword search.",
        ],
    }

    with psycopg.connect(database_url) as conn:
        schema: list[SchemaTableInfo] = []
        for table in tables:
            rows = conn.execute(
                """
                SELECT
                    a.attname AS column_name,
                    format_type(a.atttypid, a.atttypmod) AS column_type,
                    NOT a.attnotnull AS nullable
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public'
                  AND c.relname = %s
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                ORDER BY a.attnum
                """,
                (table,),
            ).fetchall()
            schema.append(
                SchemaTableInfo(
                    name=table,
                    columns=[
                        SchemaColumnInfo(
                            name=row[0],
                            type=row[1],
                            nullable=bool(row[2]),
                            notes=column_notes.get(row[0]),
                        )
                        for row in rows
                    ],
                    relationships=relationships.get(table, []),
                )
            )
        return schema


def get_pipeline_overview() -> list[PipelineStepInfo]:
    return [
        PipelineStepInfo(
            title="1. Filing metadata",
            description=(
                "MongoDB stores filing metadata (ticker, form, accession_number, local_path). "
                "Kafka events or the Load ticker action trigger indexing."
            ),
        ),
        PipelineStepInfo(
            title="2. Read & chunk",
            description=(
                "The ETL reads the local .htm file, extracts text, and splits it into "
                "overlapping chunks (CHUNK_SIZE / CHUNK_OVERLAP)."
            ),
        ),
        PipelineStepInfo(
            title="3. Embed",
            description=(
                "Each chunk is passed through the embedding model (e.g. BGE-M3) to produce "
                "a fixed-size numeric vector. Search queries use the same model so vectors "
                "live in the same semantic space."
            ),
        ),
        PipelineStepInfo(
            title="4. Store in Postgres",
            description=(
                "Chunk text and its embedding are written to public.filing_chunks. "
                "filings holds one summary row per accession number. ParadeDB's BM25 "
                "index on content is updated automatically on insert."
            ),
        ),
        PipelineStepInfo(
            title="5. Search",
            description=(
                "Semantic search uses pgvector cosine distance; keyword search uses "
                "ParadeDB pg_search BM25 scoring (pdb.score) over chunk content."
            ),
        ),
    ]


def _infer_dimensions(model_name: str) -> int:
    lowered = model_name.lower()
    if "bge-m3" in lowered:
        return 1024
    if "bge-small" in lowered:
        return 384
    if "bge-base" in lowered:
        return 768
    if "bge-large" in lowered:
        return 1024
    return 0
