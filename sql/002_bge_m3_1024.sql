-- Migrate embedding column from 384 to 1024 dimensions for BGE-M3.
-- Existing 384-dim vectors are incompatible and must be cleared before re-indexing.

DROP INDEX IF EXISTS idx_filing_chunks_embedding;

TRUNCATE filing_chunks;
TRUNCATE filings;

ALTER TABLE filing_chunks
    ALTER COLUMN embedding TYPE vector(1024);

CREATE INDEX IF NOT EXISTS idx_filing_chunks_embedding
    ON filing_chunks USING hnsw (embedding vector_cosine_ops);
