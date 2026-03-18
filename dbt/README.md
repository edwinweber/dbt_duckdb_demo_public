# Danish Democracy Data — dbt Project

dbt transformation layer for the Danish Parliament (Folketing) open data pipeline.

## Architecture

Medallion architecture with three layers:

- **Bronze** (`+schema: bronze`, views) — Raw JSON ingested from the Danish Parliament OData API, read directly from OneLake via `read_json_auto()`.
- **Silver** (`+schema: silver`, incremental append) — SCD Type 2 history with hash-based change detection (CDC). Each entity tracks inserts, updates, and deletes.
- **Gold** (`+schema: gold`, views) — Business-friendly English names, surrogate keys, and star-schema dimensions/facts.

## Key conventions

| Convention | Description |
| --- | --- |
| `LKHS_` prefix | Lakehouse metadata columns added by the pipeline (not from source) |
| `_cv` suffix | Current-version view (latest row per entity) |
| `*.json*` globs | Matches both `.json` (legacy) and `.jsonl` (dlt filesystem destination) |

## Running

```bash
dbt run                    # full run
dbt run --select bronze    # single layer
dbt test                   # run data tests
```

## Connection

Uses DuckDB via `dbt-duckdb`. See `profiles.yml` for configuration (requires `DUCKDB_DATABASE_LOCATION` environment variable).
