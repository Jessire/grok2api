#!/usr/bin/env sh
set -eu

/app/scripts/init_storage.sh

if [ "${ACCOUNT_MIGRATE_POSTGRES_TO_MYSQL_ON_STARTUP:-0}" = "1" ]; then
  marker="${ACCOUNT_MIGRATION_MARKER_PATH:-/app/data/.account-postgres-to-mysql.migrated}"
  if [ -f "$marker" ]; then
    echo "Account storage migration marker exists, skipping."
  elif [ -n "${ACCOUNT_POSTGRESQL_URL:-}" ] && [ -n "${ACCOUNT_MYSQL_URL:-}" ]; then
    echo "Running account storage migration: PostgreSQL -> MySQL/TiDB."
    python /app/scripts/migrate_account_storage.py
    mkdir -p "$(dirname "$marker")"
    date -u +"%Y-%m-%dT%H:%M:%SZ" > "$marker"
  else
    echo "ACCOUNT_MIGRATE_POSTGRES_TO_MYSQL_ON_STARTUP=1 but source/target DSN is missing." >&2
    exit 1
  fi
fi

exec "$@"
