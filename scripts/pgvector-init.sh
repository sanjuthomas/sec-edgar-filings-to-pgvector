#!/bin/sh
set -eu

PGHOST="${PGHOST:-pgvector}"
PGUSER="${PGUSER:-postgres}"
PGPASSWORD="${PGPASSWORD:-postgres}"
PGDATABASE="${PGDATABASE:-edgar}"
export PGPASSWORD

echo "Waiting for Postgres at ${PGHOST}..."
until pg_isready -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" >/dev/null 2>&1; do
  sleep 1
done

echo "Applying schema (pgvector + pg_search BM25)..."
psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1 -f /sql/001_init.sql
psql -h "$PGHOST" -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1 -f /sql/003_paradedb_pgsearch.sql

echo "Postgres ready: pgvector tables and pg_search BM25 index."
