#!/bin/sh
# Apply sql/*.sql to the project database (service db-migrate in docker-compose.yml).
#
# Runs on every `docker compose up`. A file is applied when it is new or its content changed
# since it was applied (SHA-256 kept in schema_migration), in name order (001, 002, ...). Each
# file runs in one transaction together with its schema_migration row, so a failing file leaves
# nothing half applied and stops the run. Every file in sql/ must be idempotent (IF NOT EXISTS,
# OR REPLACE, ON CONFLICT): an existing database created before this service may get a file it
# already has.
set -eu

psql_quiet() { psql -X -q -v ON_ERROR_STOP=1 "$@"; }

psql_quiet -c "CREATE TABLE IF NOT EXISTS schema_migration (
    file_name  text PRIMARY KEY,
    sha256     char(64) NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now())"

applied=0
for f in /sql/*.sql; do
    [ -e "$f" ] || continue
    name=$(basename "$f")
    sum=$(sha256sum "$f" | cut -d' ' -f1)
    done_sum=$(psql -X -tA -c "SELECT sha256 FROM schema_migration WHERE file_name = '$name'")
    [ "$done_sum" = "$sum" ] && continue
    echo "db-migrate: applying $name"
    { cat "$f"
      printf '\nINSERT INTO schema_migration (file_name, sha256) VALUES (%s, %s)\n' "'$name'" "'$sum'"
      echo "ON CONFLICT (file_name) DO UPDATE SET sha256 = EXCLUDED.sha256, applied_at = now();"
    } | psql_quiet --single-transaction -f -
    applied=$((applied + 1))
done
echo "db-migrate: $applied file(s) applied, database $PGDATABASE is up to date"
