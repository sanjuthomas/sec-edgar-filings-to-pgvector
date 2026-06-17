import json
import logging

import structlog

from edgar_etl.config import Settings
from edgar_etl.embed import embed_texts
from edgar_etl.extract import (
    chunk_text,
    extract_text_from_html,
    read_filing_html,
    resolve_filing_path,
)
from edgar_etl.models import FilingDownloadedEvent
from edgar_etl.store import FilingStore

logger = structlog.get_logger(__name__)


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO))
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
    )


def parse_event(payload: bytes | str | dict) -> FilingDownloadedEvent:
    if isinstance(payload, bytes):
        data = json.loads(payload.decode("utf-8"))
    elif isinstance(payload, str):
        data = json.loads(payload)
    else:
        data = payload
    return FilingDownloadedEvent.model_validate(data)


def process_filing_event(
    event: FilingDownloadedEvent,
    settings: Settings,
    *,
    store: FilingStore | None = None,
    skip_if_processed: bool = True,
) -> int:
    filing_store = store or FilingStore(settings.database_url)

    if skip_if_processed and filing_store.is_processed(event.accession_number):
        logger.info(
            "filing already processed, skipping",
            accession_number=event.accession_number,
        )
        return 0

    allowed_forms = {form.upper() for form in settings.allowed_forms}
    if event.form.upper() not in allowed_forms:
        logger.info(
            "skipping unsupported form",
            accession_number=event.accession_number,
            form=event.form,
            allowed_forms=sorted(allowed_forms),
        )
        return 0

    filing_path = resolve_filing_path(event.local_path, settings.edgar_data_dir)

    logger.info(
        "processing filing",
        accession_number=event.accession_number,
        local_path=str(filing_path),
        form=event.form,
        ticker=event.ticker,
    )

    html_content = read_filing_html(filing_path)
    text = extract_text_from_html(html_content)
    if not text.strip():
        raise ValueError(f"no extractable text in {filing_path}")

    chunks = chunk_text(
        text,
        chunk_size=settings.chunk_size,
        chunk_overlap=settings.chunk_overlap,
    )
    if not chunks:
        raise ValueError(f"no chunks produced for {event.accession_number}")

    embeddings = embed_texts(
        [chunk.content for chunk in chunks],
        model_name=settings.embedding_model,
        batch_size=settings.embedding_batch_size,
    )

    count = filing_store.upsert_filing(event, chunks, embeddings)
    logger.info(
        "filing loaded",
        accession_number=event.accession_number,
        chunk_count=count,
    )
    return count
