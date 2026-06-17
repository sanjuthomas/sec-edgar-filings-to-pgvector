CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS filings (
    accession_number TEXT PRIMARY KEY,
    ticker TEXT NOT NULL,
    company_name TEXT,
    form TEXT,
    filing_date DATE,
    document_url TEXT,
    local_path TEXT NOT NULL,
    downloaded_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    chunk_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS filing_chunks (
    id BIGSERIAL PRIMARY KEY,
    accession_number TEXT NOT NULL REFERENCES filings(accession_number) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    content TEXT NOT NULL,
    embedding vector(384) NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    UNIQUE (accession_number, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_filing_chunks_accession ON filing_chunks(accession_number);
CREATE INDEX IF NOT EXISTS idx_filing_chunks_metadata ON filing_chunks USING gin(metadata);

-- HNSW index for cosine similarity search (used by future Q&A project)
CREATE INDEX IF NOT EXISTS idx_filing_chunks_embedding
    ON filing_chunks USING hnsw (embedding vector_cosine_ops);
