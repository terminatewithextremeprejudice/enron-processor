#!/bin/bash
# Runs every *.up.sql in /docker-entrypoint-initdb.d/migrations in
# lexicographic order. Invoked by the official postgres image ONLY on a
# fresh data volume (its documented initdb hook).
set -euo pipefail

DIR=/docker-entrypoint-initdb.d/migrations

shopt -s nullglob
files=("$DIR"/*.up.sql)

if [ "${#files[@]}" -eq 0 ]; then
  echo "01_apply_migrations.sh: no *.up.sql in $DIR, skipping" >&2
  exit 0
fi

for f in "${files[@]}"; do
  echo "01_apply_migrations.sh: applying $f" >&2
  psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" -f "$f"
done

echo "01_apply_migrations.sh: all migrations applied" >&2
