-- Initialize DuckDB with required extensions and a persistent Azure secret.
--
-- Usage:
--   duckdb <database_path> < duckdb/init_duckdb.sql
--
-- Or from Python / CLI:
--   duckdb <database_path> ".read duckdb/init_duckdb.sql"
--
-- Prerequisites:
--   The following environment variables must be set (typically via .env):
--     AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET
--
-- Idempotent: safe to run multiple times.

-- Store persistent secrets alongside the database file so they survive
-- across Docker containers (default ~/.duckdb/stored_secrets/ is ephemeral).
SET secret_directory = '/data/duckdb';


INSTALL httpfs;
INSTALL azure;
INSTALL delta;
INSTALL sqlite;

LOAD httpfs;
LOAD azure;
LOAD delta;
LOAD sqlite;

-- Set Azure transport to curl (required for OneLake / ADLS Gen2 access).
-- This is a session-level setting; dbt sets it via profiles.yml, but this
-- ensures non-dbt sessions (e.g. ad-hoc queries) also work.
SET azure_transport_option_type = 'curl';

-- Point DuckDB's bundled curl at the system CA bundle (required in Docker)
SET ca_cert_file = '/etc/ssl/certs/ca-certificates.crt';

-- Create or replace a persistent secret for Azure service principal auth.
-- Uses getenv() to read credentials from environment variables (DuckDB CLI only).
CREATE OR REPLACE PERSISTENT SECRET azure_sp (
    TYPE azure,
    PROVIDER service_principal,
    TENANT_ID getenv('AZURE_TENANT_ID'),
    CLIENT_ID getenv('AZURE_CLIENT_ID'),
    CLIENT_SECRET getenv('AZURE_CLIENT_SECRET'),
    ACCOUNT_NAME 'onelake'
);

-- Verify the secret was created
SELECT name, type, provider, scope FROM duckdb_secrets() WHERE name = 'azure_sp';
