-- Metabase → DuckDB connection "init script" template (DuckDB 2.0 quack client).
--
-- This file is a TEMPLATE baked into the Metabase image.  On every container
-- start, docker-entrypoint.metabase.sh substitutes the token placeholder in the
-- ATTACH statement below with the value of the QUACK_SERVER_TOKEN environment
-- variable and writes the result to:
--     /metabase-data/duckdb-init.sql     (inside the container)
--     $METABASE_DATA_DIR/duckdb-init.sql (on the host)
--
-- One-time Metabase DuckDB connection setup (Admin → Databases → Add database):
--   Database path:  :memory:
--   Init SQL:       paste the RENDERED file ($METABASE_DATA_DIR/duckdb-init.sql),
--                   NOT this template.
--
-- Architecture: Metabase connects as a quack client to ddd-duckdb-server, which
-- is the sole read-write owner of the .duckdb file and (in DuckLake mode) the
-- DuckLake catalog.  Metabase uses :memory: + ATTACH quack server + USE —
-- it never mounts any database files directly.
--
-- Why USE and not CONNECT: after CONNECT the JDBC metadata calls Metabase's schema
-- sync relies on (getTables) fail in DuckDB 2.0, so no tables are discovered.
-- With ATTACH + USE the synced tables and views (main, main_bronze, main_silver,
-- main_gold, main_data_engineering) are addressed as schema.table, e.g.
-- main_gold.actor_cv.  DuckLake Silver tables are not synced; query them in SQL
-- as danish_democracy_data.ducklake_catalog.main_silver.<table>.
--
-- Only the TOKEN option below is substituted; keep the placeholder out of these
-- comments, because the substitution would write the real token into them too.
-- Never paste this template file into the Metabase UI directly.

INSTALL quack; LOAD quack;
ATTACH 'quack:duckdb_server:9494' AS danish_democracy_data
    (TOKEN '<QUACK_SERVER_TOKEN>', DISABLE_SSL true);
USE danish_democracy_data;
-- Cap threads per connection so concurrent Metabase queries share the CPU pool
-- rather than all contending for all cores simultaneously.
SET threads = 4;
