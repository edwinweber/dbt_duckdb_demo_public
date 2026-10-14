#!/usr/bin/env python3
"""Fail if the sample ``dbt build`` left any Silver or Gold table empty.

``dbt build`` already runs every dbt test; this guards against the quieter
failure where models build but select nothing.  Usage::

    python scripts/ci_check_sample_build.py path/to/database.duckdb
"""

import sys

import duckdb

# (schema, table) pairs that must contain rows after the sample build.
REQUIRED = [
    ("main_silver", "silver_ddd_aktoer"),
    ("main_silver", "silver_ddd_afstemning"),
    ("main_silver", "silver_ddd_stemme"),
    ("main_silver", "silver_rfam_family"),
    ("main_gold", "actor"),
    ("main_gold", "case"),
    ("main_gold", "vote"),
    ("main_gold", "individual_votes"),
    ("main_gold", "meeting"),
    ("main_gold", "date"),
]


def main(database: str) -> int:
    con = duckdb.connect(database, read_only=True)
    empty = []
    for schema, table in REQUIRED:
        (rows,) = con.execute(f'SELECT count(*) FROM {schema}."{table}"').fetchone()
        print(f"{schema}.{table}: {rows} rows")
        if rows == 0:
            empty.append(f"{schema}.{table}")
    if empty:
        print(f"ERROR: empty tables: {', '.join(empty)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sys.exit(main(sys.argv[1]))
