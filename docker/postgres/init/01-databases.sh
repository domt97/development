#!/usr/bin/env bash
# Creates the service databases on the local Postgres. Idempotent: an existing database is left as it is.
#
# Runs by itself on a fresh postgres_data volume (Postgres image: /docker-entrypoint-initdb.d, only when the data
# directory is empty, before TCP connections are accepted — hence the local socket, no -h). For an existing volume,
# run it once from docker/:   docker exec -i postgres bash -s < postgres/init/01-databases.sh
#
# Database per service (workspace system rule 1): store = stores-service, inventory = inventory-service,
# cart = cart-service, orders = order-service ("order" is a reserved word in SQL). Each service creates its own
# schema and tables with Flyway; its Debezium connector setup comes with the service (docs/connectors.md).
set -euo pipefail

# Fixed names, interpolated into the SQL below: keep them plain lower-case identifiers.
DATABASES=(store inventory cart orders)

for db in "${DATABASES[@]}"; do
  # CREATE DATABASE has no IF NOT EXISTS and cannot run inside a transaction: build it with \gexec, no -1.
  psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER:-postgres}" --dbname "${POSTGRES_DB:-postgres}" <<SQL
SELECT 'CREATE DATABASE $db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$db')
\gexec
SQL
  echo "✓ database $db"
done
