-- Migration for existing pgvector-only databases: enable ParadeDB pg_search + BM25 index.
CREATE EXTENSION IF NOT EXISTS pg_search;

CREATE INDEX IF NOT EXISTS idx_filing_chunks_bm25 ON filing_chunks
USING bm25 (id, content, metadata, accession_number, chunk_index)
WITH (key_field = 'id');
