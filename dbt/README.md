# Danish Democracy Data — dbt Project

Last updated: March 2026

dbt transformation layer for the Danish Parliament (Folketing) open data pipeline.

> For full project documentation — including extraction, orchestration, Docker usage,
> and environment setup — see the [project README](../README.md).

## Architecture

Medallion architecture with three layers:

- **Bronze** (`+schema: bronze`, views) — Raw JSON ingested from the Danish Parliament OData API, read via DuckDB's `read_json_auto()` from local storage or OneLake (controlled by `STORAGE_TARGET`).
- **Silver** (`+schema: silver`, incremental append) — SCD Type 2 history with hash-based Change Data Capture (CDC). Each entity tracks inserts, updates, and deletes.
- **Gold** (`+schema: gold`, views) — Business-friendly English names, surrogate keys, and star-schema dimensions/facts.

## Key Conventions

| Convention | Description |
| --- | --- |
| `LKHS_` prefix | Lakehouse metadata columns added by the pipeline (not from source) |
| `_cv` suffix | Current-version view (latest row per entity) |
| `*.json*` globs | Matches both `.json` (legacy) and `.jsonl` (dlt filesystem destination) |

## Prerequisites

1. Generate the model SQL files (required before the first run and after entity changes):

   ```bash
   python -m ddd_python.ddd_dbt.generate_dbt_models
   ```

2. Install dbt packages:

   ```bash
   cd dbt && dbt deps --profiles-dir . && cd ..
   ```

3. Load seed data (date dimension, source system lookup):

   ```bash
   cd dbt && dbt seed --profiles-dir . && cd ..
   ```

## Running

All dbt commands must be run from the `dbt/` directory with `--profiles-dir .`:

```bash
cd dbt

dbt run --profiles-dir .                        # Full run (all layers)
dbt run --select bronze --profiles-dir .        # Bronze only
dbt run --select silver --profiles-dir .        # Silver only
dbt run --select gold   --profiles-dir .        # Gold only

# Rebuild Silver from scratch (clears all CDC history)
dbt run --select silver --full-refresh --profiles-dir .

dbt test --profiles-dir .                       # All data quality tests
dbt test --select silver --profiles-dir .       # Silver tests only

dbt docs generate --profiles-dir . && dbt docs serve   # Lineage browser on :8080
```

## Connection

Uses DuckDB via `dbt-duckdb`. See `profiles.yml` for configuration (requires `DUCKDB_DATABASE_LOCATION` environment variable).
