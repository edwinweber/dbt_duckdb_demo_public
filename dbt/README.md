# Danish Democracy Data — dbt Project

dbt transformation layer for the Danish Parliament (Folketing) open data and
Rfam RNA family database pipeline.

> How the models and macros work, including the Silver CDC logic, is explained in
> [documentation/dbt_transformations.md](../documentation/dbt_transformations.md).
> Extraction, orchestration and setup are in the [project README](../README.md).

## Architecture

Medallion architecture with three layers, plus an observability layer:

- **Bronze** (`+schema: bronze`, views) — Raw NDJSON extracted from the Danish Parliament OData API and the Rfam public MySQL database, read via DuckDB's `read_json_auto()` from wherever `DANISH_DEMOCRACY_DATA_SOURCE` / `RFAM_DATA_SOURCE` point: the local filesystem or S3 (`RAW_STORAGE_TARGET=s3`). Bronze never lives on OneLake; only the Silver/Gold Delta exports can go there.
- **Silver** (`+schema: silver`, incremental) — SCD Type 2 history with hash-based Change Data Capture (CDC). Silver is the state: each run reads only the Bronze files newer than the model's bookmark and compares them with Silver's latest version of each key (inserts, updates, deletes). Stored in the main DuckDB file or, with `SILVER_STORAGE_FORMAT=ducklake`, in the attached `ducklake_catalog`.
- **Gold** (`+schema: gold`, views) — Business-friendly English names, surrogate keys, and handcrafted star-schema dimensions/facts with generated `_cv` views.
- **data_engineering** (`+schema: data_engineering`) — Dagster run history, step failures included, read from Dagster's SQLite files with `sqlite_scan` and modelled as a small star schema.

## Key Conventions

| Convention | Description |
| --- | --- |
| `LKHS_` prefix | Lakehouse metadata columns added by the pipeline (not from source) |
| `_cv` suffix | Current-version view (latest row per entity) |
| `*.json*` globs | Matches the `<entity>_<timestamp>.json` files dlt writes, and the `….json.jsonl` names some dlt versions produced |

## Prerequisites

1. The model SQL files are generated and committed; regenerate them after
   changing the entity lists:

   ```bash
   DBT_MODELS_DIRECTORY=dbt/models python -m ddd_python.ddd_dbt.generate_dbt_models
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
them with `env_var()`. In Docker, use `dbt-quack` instead of `dbt` (or
`python -m ddd_python.ddd_dbt.dbt_build_with_unique_logfile`).

```bash
cd dbt

dbt build --profiles-dir .                      # Models + tests, all layers
dbt run --profiles-dir .                        # Models only, all layers
dbt run --select bronze --profiles-dir .        # Bronze only
dbt run --select silver --profiles-dir .        # Silver only
dbt run --select gold   --profiles-dir .        # Gold only

# Silver is the state: every Silver model ignores --full-refresh. Repairs go
# through backups or a replay; see documentation/runbook.md §7.
# One run with real mass deletes, or with late Bronze files logged as skipped:
dbt run --select silver_ddd_sag --vars '{silver_delete_guard: false}' --profiles-dir .
dbt run --select silver --vars '{silver_skip_late_files: true}' --profiles-dir .

dbt test --profiles-dir .                       # All data quality tests
dbt test --select silver --profiles-dir .       # Silver tests only

dbt docs generate --profiles-dir . && dbt docs serve   # Lineage browser on :8080
```

## Connection

Uses DuckDB through `dbt-core` + `dbt-duckdb` (installed with
`pip install -e .`). `profiles.yml` has five targets: `quack` (chosen whenever
`QUACK_SERVER_HOST` is set, i.e. in Docker: an in-memory DuckDB that attaches
`duckdb_server`, run with the `dbt-quack` wrapper), and `local_ducklake_s3`,
`local_ducklake`, `onelake` and `local`, picked from the storage switches. Every
target uses `threads: 1`.
