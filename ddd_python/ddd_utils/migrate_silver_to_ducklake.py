"""One-off migration of the Silver layer from native DuckDB into DuckLake.

When ``SILVER_STORAGE_FORMAT`` is switched from ``duckdb`` to ``ducklake`` the
Silver tables that already live inside the native DuckDB database
(``DUCKDB_DATABASE_LOCATION``) are not automatically moved.  Future dbt runs
write new Silver data into the DuckLake catalog, but the historical data stays
behind in DuckDB.  This script copies the existing Silver tables across so the
DuckLake catalog starts out with a complete Silver layer.

What it does
------------
1. Opens an **in-memory** DuckDB connection (so neither database is the
   "main" catalog and the source is never the writer).
2. Attaches the source DuckDB database (``DUCKDB_DATABASE_LOCATION``)
   **read-only** as ``src`` — the source is never modified.
3. Attaches the DuckLake catalog (``DUCKLAKE_CATALOG_LOCATION`` /
   ``DUCKLAKE_DATA_PATH``) as ``ducklake_catalog`` — created on first attach.
4. Recreates the Silver schema in DuckLake and copies every Silver table
   (``CREATE TABLE … AS SELECT *``), then verifies row counts match.

The Silver Parquet files land under ``DUCKLAKE_DATA_PATH`` and the table
metadata in the catalog database, exactly as a dbt ``ducklake`` run would
produce them.

Safety
------
* The source database is opened read-only and is never written to.
* By default an existing DuckLake Silver table is **skipped** (the migration is
  re-runnable / idempotent).  Pass ``--overwrite`` to drop and re-copy.
* Run this while the pipeline containers are **stopped** so nothing else holds
  the DuckLake catalog open or writes Silver concurrently.

Usage
-----
    python -m ddd_python.ddd_utils.migrate_silver_to_ducklake  # dry run off, skip existing
    python -m ddd_python.ddd_utils.migrate_silver_to_ducklake --dry-run  # show plan only
    python -m ddd_python.ddd_utils.migrate_silver_to_ducklake --overwrite
    python -m ddd_python.ddd_utils.migrate_silver_to_ducklake --silver-schema main_silver
"""

from __future__ import annotations

import argparse
import logging
import sys

import duckdb
from ddd_python.ddd_utils import get_variables_from_env

logger = logging.getLogger(__name__)

# dbt builds Silver tables in "<target_schema>_<custom_schema>"; with the
# default DuckDB target schema "main" and the "silver" model schema this is
# "main_silver".  Overridable via --silver-schema for non-default setups.
DEFAULT_SILVER_SCHEMA = "main_silver"

SRC_ALIAS = "src"
# Matches the alias dbt attaches the DuckLake catalog under (see dbt/profiles.yml
# local_ducklake output and dbt_project.yml silver +database).
DUCKLAKE_ALIAS = "ducklake_catalog"


def _quote_ident(name: str) -> str:
    """Quote a SQL identifier, escaping embedded double quotes."""
    return '"' + name.replace('"', '""') + '"'


def _count_rows(con: duckdb.DuckDBPyConnection, relation: str) -> int:
    """Return the row count of *relation* (a fully-qualified, quoted identifier)."""
    row = con.execute(f"SELECT count(*) FROM {relation}").fetchone()
    assert row is not None  # count(*) always returns exactly one row
    return int(row[0])


def _list_silver_tables(con: duckdb.DuckDBPyConnection, schema: str) -> list[str]:
    """Return the base-table names in *schema* of the source database."""
    rows = con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_catalog = ?
          AND table_schema = ?
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        [SRC_ALIAS, schema],
    ).fetchall()
    return [r[0] for r in rows]


def migrate(
    *,
    duckdb_database_location: str,
    ducklake_catalog_location: str,
    ducklake_data_path: str,
    silver_schema: str,
    overwrite: bool,
    dry_run: bool,
) -> int:
    """Copy every Silver table from the native DuckDB database into DuckLake.

    Returns the number of tables copied (0 on a dry run).
    """
    logger.info("Source DuckDB database : %s", duckdb_database_location)
    logger.info("DuckLake catalog       : %s", ducklake_catalog_location)
    logger.info("DuckLake data path     : %s", ducklake_data_path)
    logger.info("Silver schema          : %s", silver_schema)

    # In-memory connection: source stays read-only, DuckLake is the only writer.
    con = duckdb.connect(":memory:")
    con.execute("INSTALL ducklake; LOAD ducklake;")
    con.execute(f"ATTACH '{duckdb_database_location}' AS {SRC_ALIAS} (READ_ONLY)")
    con.execute(
        f"ATTACH 'ducklake:{ducklake_catalog_location}' AS {DUCKLAKE_ALIAS} "
        f"(DATA_PATH '{ducklake_data_path}')"
    )

    tables = _list_silver_tables(con, silver_schema)
    if not tables:
        logger.warning(
            "No base tables found in %s.%s — nothing to migrate.", SRC_ALIAS, silver_schema
        )
        con.close()
        return 0

    logger.info("Found %d Silver table(s) to migrate.", len(tables))

    schema_id = _quote_ident(silver_schema)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DUCKLAKE_ALIAS}.{schema_id}")

    existing = {
        r[0]
        for r in con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_catalog = ? AND table_schema = ?
            """,
            [DUCKLAKE_ALIAS, silver_schema],
        ).fetchall()
    }

    copied = 0
    skipped = 0
    for table in tables:
        tbl_id = _quote_ident(table)
        src_ref = f"{SRC_ALIAS}.{schema_id}.{tbl_id}"
        dst_ref = f"{DUCKLAKE_ALIAS}.{schema_id}.{tbl_id}"
        src_count = _count_rows(con, src_ref)

        if table in existing and not overwrite:
            logger.info("SKIP   %-45s already exists in DuckLake (use --overwrite)", table)
            skipped += 1
            continue

        if dry_run:
            action = "OVERWRITE" if table in existing else "COPY"
            logger.info("[dry-run] %-9s %-45s (%d rows)", action, table, src_count)
            continue

        if table in existing:
            con.execute(f"DROP TABLE {dst_ref}")

        con.execute(f"CREATE TABLE {dst_ref} AS SELECT * FROM {src_ref}")
        dst_count = _count_rows(con, dst_ref)
        if dst_count != src_count:
            con.close()
            raise RuntimeError(
                f"Row count mismatch for {table}: source={src_count}, ducklake={dst_count}"
            )
        logger.info("COPY   %-45s %d rows", table, dst_count)
        copied += 1

    con.close()

    if dry_run:
        logger.info("Dry run complete — no changes written.")
    else:
        logger.info(
            "Migration complete — %d table(s) copied, %d skipped (already present).",
            copied,
            skipped,
        )
    return copied


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="migrate_silver_to_ducklake",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--silver-schema",
        default=DEFAULT_SILVER_SCHEMA,
        help=(
            "Schema holding the Silver tables in both databases "
            f"(default: {DEFAULT_SILVER_SCHEMA})."
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Drop and re-copy Silver tables that already exist in DuckLake.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be copied without writing anything.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if get_variables_from_env.SILVER_STORAGE_FORMAT != "ducklake":
        logger.error(
            "SILVER_STORAGE_FORMAT=%s — set it to 'ducklake' before migrating.",
            get_variables_from_env.SILVER_STORAGE_FORMAT,
        )
        sys.exit(1)

    duckdb_database_location = get_variables_from_env.DUCKDB_DATABASE_LOCATION
    ducklake_catalog_location = get_variables_from_env.DUCKLAKE_CATALOG_LOCATION
    ducklake_data_path = get_variables_from_env.DUCKLAKE_DATA_PATH

    missing = [
        name
        for name, value in (
            ("DUCKDB_DATABASE_LOCATION", duckdb_database_location),
            ("DUCKLAKE_CATALOG_LOCATION", ducklake_catalog_location),
            ("DUCKLAKE_DATA_PATH", ducklake_data_path),
        )
        if not value
    ]
    if missing:
        logger.error("Required environment variable(s) not set: %s", ", ".join(missing))
        sys.exit(1)

    # Narrow Optional[str] -> str for the type checker; the check above guarantees these.
    assert duckdb_database_location is not None
    assert ducklake_catalog_location is not None
    assert ducklake_data_path is not None

    try:
        migrate(
            duckdb_database_location=duckdb_database_location,
            ducklake_catalog_location=ducklake_catalog_location,
            ducklake_data_path=ducklake_data_path,
            silver_schema=args.silver_schema,
            overwrite=args.overwrite,
            dry_run=args.dry_run,
        )
    except (duckdb.Error, RuntimeError) as exc:
        logger.error("Migration failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
