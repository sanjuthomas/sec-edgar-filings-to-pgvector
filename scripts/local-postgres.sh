#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="${PGVECTOR_DATA_DIR:-/tmp/pgvector-data}"
PORT="${PGVECTOR_PORT:-5433}"
export LC_ALL="${LC_ALL:-en_US.UTF-8}"
export LANG="${LANG:-en_US.UTF-8}"

usage() {
  cat <<EOF
Usage: $(basename "$0") {init|start|stop|status|logs}

Local Homebrew PostgreSQL + pgvector on port ${PORT}, data dir ${DATA_DIR}.
Not Docker — use this for sec-edgar-semantic-search-ui (PGUSER=sanjuthomas).
EOF
}

init_cluster() {
  if [[ -f "${DATA_DIR}/PG_VERSION" ]]; then
    echo "Data directory already initialized: ${DATA_DIR}" >&2
    exit 1
  fi
  mkdir -p "${DATA_DIR}"
  initdb -D "${DATA_DIR}" --locale=C -U postgres -A trust
  {
    echo "port = ${PORT}"
    echo "listen_addresses = 'localhost'"
  } >> "${DATA_DIR}/postgresql.conf"
  echo "Initialized ${DATA_DIR}. Run: $0 start && psql postgresql://postgres@localhost:${PORT}/postgres -c 'CREATE DATABASE edgar;' && psql postgresql://postgres@localhost:${PORT}/edgar -f sql/001_init.sql"
}

case "${1:-}" in
  init) init_cluster ;;
  start) pg_ctl -D "${DATA_DIR}" -l "${DATA_DIR}/postgres.log" -o "-p ${PORT}" start ;;
  stop) pg_ctl -D "${DATA_DIR}" stop ;;
  status) pg_ctl -D "${DATA_DIR}" status ;;
  logs) tail -f "${DATA_DIR}/postgres.log" ;;
  *) usage; exit 1 ;;
esac
