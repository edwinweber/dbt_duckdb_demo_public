#!/usr/bin/env bash
# Connect to the project DuckDB database with Azure credentials.
# Creates/replaces the persistent Azure secret on every connection.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "Error: .env file not found at $ENV_FILE" >&2
    exit 1
fi

# Export all variables from .env (skip comments and blank lines)
set -a
# shellcheck source=.env
source "$ENV_FILE"
set +a

# Generate a temp init file with credentials substituted as string literals
INIT_FILE="$(mktemp /tmp/duckdb_init_XXXXXX.sql)"
trap 'rm -f "$INIT_FILE"' EXIT

cat > "$INIT_FILE" <<SQL
SET azure_transport_option_type = 'curl';
CREATE OR REPLACE PERSISTENT SECRET azure_sp (
    TYPE azure,
    PROVIDER service_principal,
    TENANT_ID '${AZURE_TENANT_ID}',
    CLIENT_ID '${AZURE_CLIENT_ID}',
    CLIENT_SECRET '${AZURE_CLIENT_SECRET}',
    ACCOUNT_NAME 'onelake'
);
SQL

exec duckdb -init "$INIT_FILE" "${DUCKDB_DATABASE_LOCATION}"
