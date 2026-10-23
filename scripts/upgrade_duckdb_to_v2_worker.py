#!/usr/bin/env python3
"""
upgrade_duckdb_to_v2_worker.py — copy DuckDB 1.5.x database files to the DuckDB 2.0 format.

Called by upgrade_duckdb_to_v2.sh inside a throw-away venv that has duckdb==2.0.0.
DuckDB 2.0 opens 1.5.x files directly, so one process attaches the old file
read-only and copies it into a new file (COPY FROM DATABASE), which is written in
the 2.0 storage format. The originals are never written to.

For each file the worker then verifies the copy against the original:
  * catalog / main file: the same tables with the same row counts, the same views
  * DuckLake: the same tables with the same row counts, read through the DuckLake
    extension (old catalog vs new catalog, same Parquet data path)
  * main file views: every view binds in the copy exactly when it binds in the original

Usage:
    upgrade_duckdb_to_v2_worker.py --out-dir DIR --duckdb-file F --duckdb-alias NAME
        [--catalog-file F --ducklake-alias NAME --data-path DIR]

Exit code 0 only when every check passed.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Mapping
from pathlib import Path

import duckdb


def _quote(value: str) -> str:
    return value.replace("'", "''")


def _table_counts(con: duckdb.DuckDBPyConnection, database: str) -> dict[str, int]:
    """Row count per user table in an attached database, keyed by schema.table."""
    tables = con.execute(
        "SELECT schema_name, table_name FROM duckdb_tables() "
        "WHERE database_name = ? AND NOT internal ORDER BY 1, 2",
        [database],
    ).fetchall()
    counts: dict[str, int] = {}
    for schema, table in tables:
        row = con.execute(f'SELECT count(*) FROM "{database}"."{schema}"."{table}"').fetchone()
        counts[f"{schema}.{table}"] = row[0] if row else -1
    return counts


def _sort_exclude_lists(sql: str) -> str:
    """EXCLUDE (...) is a set: DuckDB 2.0 may serialise its columns in a different order."""
    return re.sub(
        r"EXCLUDE \(([^)]*)\)",
        lambda m: "EXCLUDE (" + ", ".join(sorted(c.strip() for c in m.group(1).split(","))) + ")",
        sql,
    )


def _view_definitions(con: duckdb.DuckDBPyConnection, database: str) -> dict[str, str]:
    views = con.execute(
        "SELECT schema_name, view_name, sql FROM duckdb_views() "
        "WHERE database_name = ? AND NOT internal ORDER BY 1, 2",
        [database],
    ).fetchall()
    return {f"{schema}.{view}": _sort_exclude_lists(sql) for schema, view, sql in views}


def _view_columns(
    con: duckdb.DuckDBPyConnection, database: str, views: dict[str, str]
) -> dict[str, str]:
    """Column list per view, or the error text when the view does not bind."""
    result: dict[str, str] = {}
    for name in views:
        schema, view = name.split(".", 1)
        try:
            rows = con.execute(
                f'DESCRIBE SELECT * FROM "{database}"."{schema}"."{view}"'
            ).fetchall()
            result[name] = ",".join(f"{r[0]}:{r[1]}" for r in rows)
        except duckdb.Error as exc:
            result[name] = f"ERROR {type(exc).__name__}"
    return result


def _compare(label: str, old: Mapping[str, object], new: Mapping[str, object]) -> list[str]:
    problems: list[str] = []
    for key in sorted(old.keys() | new.keys()):
        if key not in new:
            problems.append(f"{label}: {key} missing in the copy")
        elif key not in old:
            problems.append(f"{label}: {key} only in the copy")
        elif old[key] != new[key]:
            problems.append(f"{label}: {key} differs (old={old[key]!r}, new={new[key]!r})")
    return problems


def copy_database(src: Path, dst: Path, alias: str) -> None:
    """Copy every object of an existing database file into a new 2.0-format file."""
    if dst.exists():
        dst.unlink()
    con = duckdb.connect(":memory:")
    try:
        con.execute(f"ATTACH '{_quote(str(src))}' AS old_db (READ_ONLY)")
        con.execute(f"ATTACH '{_quote(str(dst))}' AS {alias}")
        con.execute(f"COPY FROM DATABASE old_db TO {alias}")
        con.execute("DETACH old_db")
        con.execute(f"CHECKPOINT {alias}")
    finally:
        con.close()


def verify_plain_copy(src: Path, dst: Path) -> list[str]:
    """Same tables, same row counts and same view definitions in old and new file."""
    con = duckdb.connect(":memory:")
    try:
        con.execute(f"ATTACH '{_quote(str(src))}' AS old_db (READ_ONLY)")
        con.execute(f"ATTACH '{_quote(str(dst))}' AS new_db (READ_ONLY)")
        old_counts = _table_counts(con, "old_db")
        new_counts = _table_counts(con, "new_db")
        problems = _compare(f"{src.name} rows", old_counts, new_counts)
        old_views = _view_definitions(con, "old_db")
        problems += _compare(f"{src.name} views", old_views, _view_definitions(con, "new_db"))
        print(
            f"  {src.name}: {len(old_counts)} tables, {sum(old_counts.values()):,} rows, {len(old_views)} views",
            flush=True,
        )
        return problems
    finally:
        con.close()


def _ducklake_state(
    catalog: Path, alias: str, data_path: str, with_view_check: Path | None, main_alias: str
) -> tuple[dict[str, int], dict[str, str]]:
    """Table counts through the DuckLake extension, plus optional view binding of a main file."""
    con = duckdb.connect(":memory:")
    try:
        con.execute("INSTALL ducklake")
        con.execute("LOAD ducklake")
        con.execute(
            f"ATTACH 'ducklake:{_quote(str(catalog))}' AS {alias} (DATA_PATH '{_quote(data_path)}', READ_ONLY)"
        )
        counts = _table_counts(con, alias)
        views: dict[str, str] = {}
        if with_view_check is not None:
            con.execute(f"ATTACH '{_quote(str(with_view_check))}' AS {main_alias} (READ_ONLY)")
            views = _view_columns(con, main_alias, _view_definitions(con, main_alias))
        return counts, views
    finally:
        con.close()


def verify_ducklake(
    old_catalog: Path,
    new_catalog: Path,
    alias: str,
    data_path: str,
    old_main: Path,
    new_main: Path,
    main_alias: str,
) -> list[str]:
    """DuckLake tables read the same through old and new catalog; views bind the same."""
    old_counts, old_views = _ducklake_state(old_catalog, alias, data_path, old_main, main_alias)
    new_counts, new_views = _ducklake_state(new_catalog, alias, data_path, new_main, main_alias)
    broken = sorted(v for v, cols in old_views.items() if cols.startswith("ERROR"))
    print(
        f"  DuckLake: {len(old_counts)} tables, {sum(old_counts.values()):,} rows; "
        f"{len(old_views)} views, {len(broken)} do not bind in the original either",
        flush=True,
    )
    problems = _compare("DuckLake rows", old_counts, new_counts)
    problems += _compare("view columns", old_views, new_views)
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--out-dir", required=True, type=Path, help="Directory for the 2.0-format copies"
    )
    parser.add_argument(
        "--duckdb-file", required=True, type=Path, help="Main .duckdb file (1.5.x format)"
    )
    parser.add_argument(
        "--duckdb-alias", required=True, help="Database name of the main file (= file stem)"
    )
    parser.add_argument(
        "--catalog-file", type=Path, help="DuckLake catalog file (ducklake mode only)"
    )
    parser.add_argument(
        "--ducklake-alias", default="ducklake_catalog", help="Name the catalog is attached as"
    )
    parser.add_argument(
        "--data-path", help="DuckLake Parquet data path (required with --catalog-file)"
    )
    args = parser.parse_args()

    if duckdb.__version__.split(".")[0] != "2":
        print(f"ERROR: this worker needs DuckDB 2.x, found {duckdb.__version__}", file=sys.stderr)
        return 2
    if args.catalog_file and not args.data_path:
        print("ERROR: --data-path is required with --catalog-file", file=sys.stderr)
        return 2

    args.out_dir.mkdir(parents=True, exist_ok=True)
    new_main = args.out_dir / args.duckdb_file.name
    print(f"DuckDB {duckdb.__version__}", flush=True)

    problems: list[str] = []
    new_catalog: Path | None = None

    if args.catalog_file:
        new_catalog = args.out_dir / args.catalog_file.name
        print(f"Copying {args.catalog_file.name} ...", flush=True)
        copy_database(args.catalog_file, new_catalog, "new_catalog")
        problems += verify_plain_copy(args.catalog_file, new_catalog)

    print(f"Copying {args.duckdb_file.name} ...", flush=True)
    copy_database(args.duckdb_file, new_main, args.duckdb_alias)
    problems += verify_plain_copy(args.duckdb_file, new_main)

    if args.catalog_file and new_catalog is not None and args.data_path:
        problems += verify_ducklake(
            args.catalog_file,
            new_catalog,
            args.ducklake_alias,
            args.data_path,
            args.duckdb_file,
            new_main,
            args.duckdb_alias,
        )

    if problems:
        print(f"\nVERIFICATION FAILED ({len(problems)} problems):", file=sys.stderr)
        for problem in problems[:50]:
            print(f"  - {problem}", file=sys.stderr)
        return 1
    print("\nVerification passed: the copies match the originals.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
