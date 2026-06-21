#!/bin/bash
set -euo pipefail

CONF="${PGDATA:-/var/lib/postgresql/data}/postgresql.conf"

if [ -f "$CONF" ] && ! grep -qE "^shared_preload_libraries.*pg_search" "$CONF"; then
  if grep -qE "^shared_preload_libraries" "$CONF"; then
    sed -i -E "s/^shared_preload_libraries = '([^']*)'/shared_preload_libraries = 'pg_search,\1'/" "$CONF"
  elif grep -qE "^#shared_preload_libraries" "$CONF"; then
    sed -i -E "s/^#shared_preload_libraries = ''/shared_preload_libraries = 'pg_search'/" "$CONF"
  else
    printf "\nshared_preload_libraries = 'pg_search'\n" >>"$CONF"
  fi
  echo "Configured pg_search in shared_preload_libraries (ParadeDB pg_search requires restart)."
fi

exec docker-entrypoint.sh "$@"
