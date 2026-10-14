# Danish Democracy Data — dbt Project

dbt transformation layer for the Danish Parliament (Folketing) open data and
Rfam RNA family database pipeline.

> How the models and macros work, including the Silver CDC logic, is explained in
> [documentation/dbt_transformations.md](../documentation/dbt_transformations.md).
> Extraction, orchestration and setup are in the [project README](../README.md).

## Architecture

Medallion architecture with three layers, plus an observability layer:

- **Bronze** (`+schema: bronze`, views) — Raw NDJSON extracted from the Danish Parliament OData API and the Rfam public MySQL database, read via DuckDB's `read_json_auto()` from wherever `DANISH_DEMOCRACY_DATA_SOURCE` / `RFAM_DATA_SOURCE` point: the local filesystem or S3 (`RAW_STORAGE_TARGET=s3`). Bronze never lives on OneLake; only the Silver/Gold Delta exports can go there.
- **Silver** (`+schema: silver`, incremental) — SCD Type 2 history with hash-based Change Data Capture (CDC). Each entity tracks inserts, updates, and deletes. Stored in the main DuckDB file or, with `SILVER_STORAGE_FORMAT=ducklake`, in the attached `ducklake_catalog`.
- **Gold** (`+schema: gold`, views) — Business-friendly English names, surrogate keys, and handcrafted star-schema dimensions/facts with generated `_cv` views.
- **data_engineering** (`+schema: data_engineering`) — Dagster run history (SQLite via `sqlite_scan`, step failures via NDJSON) modelled as a small star schema.

## Key Conventions

| Convention | Description |
| --- | --- |
| `LKHS_` prefix | Lakehouse metadata columns added by the pipeline (not from source) |
| `_cv` suffix | Current-version view (latest row per entity) |
| `*.json*` globs | Matches the `<entity>_<timestamp>.json` files dlt writes, and the `….json.jsonl` names some dlt versions produced |

## Prerequisites

1. Generate the model SQL files (required before the first run and after entity changes):

   ```bash
   python -m ddd_python.ddd_dbt.generate_dbt_models
   ```

2. Install dbt packages:

   ```bash
   cd dbt && dbt deps --profiles-dir . && cd ..
   ```

3. Load seed data (Danish public holidays, source system lookup):

   ```bash
   cd dbt && dbt seed --profiles-dir . && cd ..
   ```

## Running

Run dbt from the `dbt/` directory with `--profiles-dir .`, with the variables
from `.env` exported (`set -a && source ../.env && set +a`); `profiles.yml` reads
them with `env_var()`.

```bash
cd dbt

dbt build --profiles-dir .                      # Models + tests, all layers
dbt run --profiles-dir .                        # Models only, all layers
dbt run --select bronze --profiles-dir .        # Bronze only
dbt run --select silver --profiles-dir .        # Silver only
dbt run --select gold   --profiles-dir .        # Gold only

# Rebuild Silver from all Bronze files. Run a full extraction first
# (--date_to_load_from 1900-01-01), or incremental entities get false deletes.
dbt run --select silver --full-refresh --profiles-dir .

dbt test --profiles-dir .                       # All data quality tests
dbt test --select silver --profiles-dir .       # Silver tests only

dbt docs generate --profiles-dir . && dbt docs serve   # Lineage browser on :8080
```

## Connection

Uses DuckDB through the adapter built into the dbt-oss binary (installed with
`scripts/install_dbt_oss.sh`). `profiles.yml` has four targets (`local`,
`local_ducklake`, `local_ducklake_s3`, `onelake`) and picks one from the storage
switches; every target uses `threads: 1`.
