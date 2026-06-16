-- Canonical Metabase → DuckDB connection "init script".
--
-- Paste these statements into the *init SQL* field of the DuckDB database
-- connection in the Metabase admin UI (Admin → Databases → your DuckDB DB).
-- This is the single source of truth; the README and docker-compose.yml both
-- point here so the script can't drift.
--
-- The ducklake lines attach the DuckLake catalog so the Silver layer is
-- queryable when SILVER_STORAGE_FORMAT=ducklake (Gold views read Silver through
-- ducklake_catalog).  In native mode (SILVER_STORAGE_FORMAT=duckdb) Silver lives
-- in the main DuckDB file and the ducklake lines can be omitted.
--
-- Why DETACH-then-ATTACH instead of ATTACH IF NOT EXISTS:
--   The pipeline stops/starts Metabase around runs (single-writer DuckDB).  On a
--   pooled JDBC connection that survives a restart, a stale half-attached catalog
--   otherwise raises:
--     Catalog "__ducklake_metadata_ducklake_catalog" does not exist
--   DETACH-then-ATTACH rebuilds a clean attach on every new connection (self-healing).
--   The trade-off: if several pool connections initialise concurrently, one ATTACH
--   may see the name already taken and raise "already exists" — Metabase retries and
--   the pool stabilises within one refresh.  That is less bad than a persistent
--   stale-catalog failure that never auto-recovers.
-- Why READ_ONLY:
--   Metabase never writes Silver, and read-only avoids contending for the
--   DuckLake catalog's own single-writer lock.

INSTALL icu; LOAD icu;
INSTALL httpfs; LOAD httpfs;
INSTALL delta; LOAD delta;
INSTALL sqlite; LOAD sqlite;
INSTALL ducklake; LOAD ducklake;
DETACH DATABASE IF EXISTS ducklake_catalog;
ATTACH 'ducklake:/data/duckdb/ducklake_catalog.ducklake' AS ducklake_catalog (DATA_PATH '/data/ducklake', READ_ONLY);
-- Cap threads per connection so concurrent Metabase queries share the CPU pool
-- rather than all contending for all cores simultaneously.
SET threads = 4;
