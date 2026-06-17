import json
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from pgvector.psycopg import register_vector

from edgar_etl.models import FilingDownloadedEvent, TextChunk


class FilingStore:
    def __init__(self, database_url: str) -> None:
        self._database_url = database_url

    def connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self._database_url)
        register_vector(conn)
        return conn

    def init_schema(self, schema_path: str | Path) -> None:
        sql = Path(schema_path).read_text(encoding="utf-8")
        with self.connect() as conn:
            conn.execute(sql)
            conn.commit()

    def is_processed(self, accession_number: str) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM filings WHERE accession_number = %s",
                (accession_number,),
            ).fetchone()
            return row is not None

    def upsert_filing(
        self,
        event: FilingDownloadedEvent,
        chunks: list[TextChunk],
        embeddings: list[list[float]],
    ) -> int:
        if len(chunks) != len(embeddings):
            raise ValueError("chunks and embeddings length mismatch")

        processed_at = datetime.now(timezone.utc)
        with self.connect() as conn:
            with conn.transaction():
                conn.execute(
                    """
                    INSERT INTO filings (
                        accession_number, ticker, company_name, form, filing_date,
                        document_url, local_path, downloaded_at, processed_at, chunk_count
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (accession_number) DO UPDATE SET
                        ticker = EXCLUDED.ticker,
                        company_name = EXCLUDED.company_name,
                        form = EXCLUDED.form,
                        filing_date = EXCLUDED.filing_date,
                        document_url = EXCLUDED.document_url,
                        local_path = EXCLUDED.local_path,
                        downloaded_at = EXCLUDED.downloaded_at,
                        processed_at = EXCLUDED.processed_at,
                        chunk_count = EXCLUDED.chunk_count
                    """,
                    (
                        event.accession_number,
                        event.ticker,
                        event.company_name,
                        event.form,
                        event.filing_date,
                        event.document_url,
                        event.local_path,
                        event.downloaded_at,
                        processed_at,
                        len(chunks),
                    ),
                )
                conn.execute(
                    "DELETE FROM filing_chunks WHERE accession_number = %s",
                    (event.accession_number,),
                )
                for chunk, embedding in zip(chunks, embeddings, strict=True):
                    metadata = {
                        **chunk.metadata,
                        "ticker": event.ticker,
                        "company_name": event.company_name,
                        "form": event.form,
                        "filing_date": event.filing_date.isoformat(),
                        "accession_number": event.accession_number,
                        "local_path": event.local_path,
                        "document_url": event.document_url,
                    }
                    conn.execute(
                        """
                        INSERT INTO filing_chunks (
                            accession_number, chunk_index, content, embedding, metadata
                        ) VALUES (%s, %s, %s, %s, %s::jsonb)
                        """,
                        (
                            event.accession_number,
                            chunk.chunk_index,
                            chunk.content,
                            embedding,
                            json.dumps(metadata),
                        ),
                    )
        return len(chunks)
