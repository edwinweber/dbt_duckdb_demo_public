# Project Reference

> The canonical technical reference for this project: architecture, modules,
> entity lists, storage switches, environment variables and conventions. It is
> kept in sync with the code; where they disagree, the code wins.

## Project overview

**Danish Democracy Data (dbt + DuckDB demo)** extracts open data from the
**Danish Parliament OData API** and the **Rfam public MySQL database**,
transforms it through a **Bronze → Silver → Gold** medallion in DuckDB, and can
export Silver and Gold as Delta Lake tables to the local filesystem, S3 or
Microsoft Fabric OneLake. It is a learning/reference project on a low-cost,
open-source stack.

- **Repo:** `edwinweber/dbt_duckdb_demo`, default branch `main`
- **License:** MIT

## Tech stack

| Layer | Technology |
| --- | --- |
| Language | Python ≥ 3.13 |
| Orchestration | Dagster ≥ 1.13.26 (software-defined assets) |
| Extraction | dlt ≥ 1.31.0; `requests` for OData, SQLAlchemy ≥ 2.1.5 + PyMySQL for Rfam |
| Transformation | dbt-core ≥ 1.12.6, < 1.13 + dbt-duckdb ≥ 1.11.0 + dbt-utils 1.4.1; `dbt-quack` wrapper for Docker quack mode |
| Query engine | DuckDB ≥ 2.0.0, < 3 |
| Single-writer server | `duckdb_quack_server.py` — sole read-write DuckDB owner in Docker; all other containers connect via quack RPC |
| Bronze files, DuckLake data | local filesystem (default) or S3-compatible storage (RustFS, AWS S3) |
| Silver storage | DuckDB tables (default) or DuckLake (Parquet + catalog) |
| Delta export | DuckDB `delta_scan` for the dedup read; `deltalake` ≥ 1.6.6 + PyArrow ≥ 25 for the write; to local, S3 or OneLake |
| BI | Metabase (connects via quack client); Power BI via OneLake |
| Container | Docker + Docker Compose; non-root user `app` (UID 1000) |
| Testing | pytest ≥ 9.1.1, ruff, mypy |

The reasons for each choice are in [design_decisions.md](design_decisions.md).

## Architecture

```text
Extraction (dlt)          → NDJSON files on local disk or S3   (RAW_STORAGE_TARGET)
Bronze (dbt views)        → read_json_auto over the files
Silver (dbt incremental)  → hash-based CDC, SCD Type 2, plus _cv views
                            in the .duckdb file or DuckLake    (SILVER_STORAGE_FORMAT)
Gold (dbt views)          → star schema: dimensions and facts
Delta export (optional)   → local / S3 / OneLake               (STORAGE_TARGET)
```

Dagster orchestrates everything. Two daily schedules (06:00 full pipeline, 08:00
observability refresh, Europe/Copenhagen) ship disabled; DuckLake catalog
cleanup is a manual job.

## Directory structure

```text
├── ddd_python/                  Python package
│   ├── ddd_dagster/             assets, jobs, schedules, sensors, resources
│   ├── ddd_dlt/                 extraction pipelines + Delta Lake export
│   ├── ddd_dbt/                 dbt model generator, dbt runner, DuckDB init
│   └── ddd_utils/               configuration, env vars, paths, backups, Azure clients
├── dbt/                         dbt project
│   ├── models/bronze/           views over the raw files (+ sources, utility views)
│   ├── models/silver/           CDC tables + _cv views
│   ├── models/gold/             star-schema views
│   ├── models/data_engineering/ Dagster run history (SQLite + step-failure NDJSON)
│   ├── macros/                  model factories, hash, CDC hooks
│   ├── seeds/                   Danish public holidays, source-system registry
│   └── tests/                   singular test: Silver SCD2 grain
├── tests/                       pytest suite; fixtures/bronze_sample = CI data sample
├── docker/                      Dockerfiles (app, Metabase), entrypoints, Metabase init SQL, DOCKER_USAGE.md
├── scripts/                     deploy, backup cron, host setup, CI sample,
│                                public-repo sync, upgrade check, DuckDB 2.0 upgrade
├── duckdb/                      host-shell DuckDB init (extensions + Azure secret)
├── documentation/               guides (this file, design decisions, …), diagrams
├── .github/workflows/ci.yml     CI: pytest, S3 tests, ruff/mypy, dbt parse/build, docker build, dbt docs
├── docker-compose.yml           services: run, backup (one-off), dagster, metabase
├── docker-compose.rustfs.yml     override that adds RustFS
├── workspace.yaml               Dagster workspace (loads ddd_dagster.definitions)
└── .env.example                 environment variable template
```

Git-ignored: `/data/`, `.env`, `*.duckdb`, `dbt/target/`, `dbt/logs/`,
`dlt/pipelines_dir/`, `.dagster/`, `__pycache__/`, `.venv/`, `*.egg-info/`,
`.pytest_cache/`.

## Data sources

### Danish Parliament (DDD)

- **Base URL:** `https://oda.ft.dk/api`, no authentication; all primary keys `id`
- **Entities (18):** Afstemning, Afstemningstype, Aktør, Aktørtype, Møde,
  Mødestatus, Mødetype, Periode, Sag, Sagstrin, SagstrinAktør, Sagstrinsstatus,
  Sagstrinstype, Sagskategori, Sagsstatus, Sagstype, Stemme, Stemmetype
- **Incremental (6):** Aktør, Møde, Sag, Sagstrin, SagstrinAktør, Stemme —
  `$filter=opdateringsdato ge DateTime'<date>'`
- **Full extract:** the other 12

### Rfam

- **Connection:** `mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam` (public, read-only)
- **Tables (7):** family, genome, clan, clan_membership, author, literature_reference, dead_family
- **Incremental (2):** family (pk `rfam_acc`), genome (pk `upid`) — `WHERE updated >= :updated_from` (bound parameter)
- **Full extract:** the other 5; primary keys `clan_acc`, `author_id`, `pmid`, …
- **Queries:** `RFAM_TABLE_QUERIES` in `configuration_variables.py`, with a `{where_clause}` placeholder

**Incremental = lookback window.** Without an explicit `date_to_load_from`, the
lower bound is today minus `DANISH_DEMOCRACY_DEFAULT_DAYS_TO_LOAD` (31) or
`RFAM_DEFAULT_DAYS_TO_LOAD` (365). No cursor is stored; Silver's hash CDC
ignores the overlap.

**Full extractions of incremental entities** start on or before `1900-01-01`
(`FULL_EXTRACT_DATE`) and are written as `<entity>_<YYYYMMDD_HHMMSS>_full.json`.
Silver deletes a key of an incremental entity only when it is missing from a
`_full` file that is processed on its own
([dbt_transformations.md §3.4](dbt_transformations.md#34-deletes-of-incremental-entities-_full-files)).

## Python package (`ddd_python/`)

A guided tour with examples is in [python_code_explained.md](python_code_explained.md).

### `ddd_utils/`

- **configuration_variables.py** — the single source of truth: entity lists,
  incremental subsets, primary keys and Rfam queries. Model-name
  lists are derived from them; import-time checks raise `ValueError` on a
  missing primary key. Adding an entity means updating this
  file and regenerating the models (Gold is hand-modelled).
- **get_variables_from_env.py** — loads `.env`; optional variables are read at
  import (storage switches validated there), required ones (Azure, Fabric)
  resolve lazily through a PEP 562 `__getattr__` and raise `OSError` when
  missing. A `.pyi` stub declares the lazy names for mypy.
- **path_utils.py** — `build_bronze_destination_path` (always
  `Files/Bronze/<source>/<entity>`), `build_delta_export_path` (local / S3 /
  OneLake path + storage options), `open_export_connection` (read-only DuckDB,
  DuckLake catalog attached read-only in `ducklake` mode, S3 secret when needed),
  `silver_storage_is_ducklake`.
- **string_utils.py** — `normalize_danish_name` (ø→oe, æ→ae, å→aa, lowercase)
  and `resolve_date_to_load_from` (default window or validated `YYYY-MM-DD`).
- **get_fabric_onelake_clients.py** — cached Azure service-principal credential,
  Fabric token for the OneLake export, ADLS Gen2 client helpers. Imported lazily.
- **fabric_capacity_pause_resume.py** — CLI (`pause` / `resume`) for the Fabric
  capacity via the Azure Management API; not wired into Dagster.
- **backup_common.py**, **backup_platform.py**, **restore_platform.py** —
  backup targets (`BackupTarget` dataclass), zip archives with container
  stop/start, optional Hetzner StorageBox upload with retention, and restores
  with correct UID/GID.
- **migrate_silver_to_ducklake.py** — one-off copy of Silver history from
  native DuckDB into DuckLake.
- **mark_full_extracts.py** — one-off: adds the `_full` marker to full extractions of
  incremental entities written before the marker existed. Reads the extraction run log
  (start date `1900-01-01`), falls back to file size, dry run by default.
- **archive_bronze.py** — moves processed Bronze files (all but the newest processed
  file and files younger than `--keep-days`) to `BRONZE_ARCHIVE_DIR`; dry run by
  default, checksummed manifest, `--restore`. Also run on demand by
  `bronze_archive_job` ([dbt_transformations.md §3.6](dbt_transformations.md#36-archiving-bronze)).

### `ddd_dlt/`

- **dlt_pipeline_execution_functions.py** — the extraction engine.
  `execute_pipeline(pipeline_type, source_system_code, **kwargs)` dispatches to
  `run_api_to_file_pipeline` (paginated OData → NDJSON),
  `run_sql_to_file_pipeline` (SQL → NDJSON for Rfam; Parquet is the handler
  default) or `run_file_to_file_pipeline` (plain copy, currently unused), and
  always writes one NDJSON log record with secrets scrubbed.
  `run_extraction_pool(...)` runs a list of `PipelineTask`s on a
  `ThreadPoolExecutor` (4 workers), writes a summary record and raises after all
  tasks were attempted.
- **dlt_run_extraction_pipelines_danish_parliament_data.py**,
  **dlt_run_extraction_pipelines_rfam.py** — CLIs
  (`--date_to_load_from`, `--file_names_to_retrieve …`) that build the tasks
  and call `run_extraction_pool`.
- **export_silver.py** — Silver → Delta, incremental append: anti-join of the
  Silver table against `delta_scan('<target>')` on (pk, `LKHS_date_valid_from`),
  write with `write_deltalake`; first export overwrites. Reads Silver from
  `ducklake_catalog.main_silver` or `<DUCKDB_DATABASE>.main_silver`.
- **export_gold.py** — Gold → Delta, full overwrite, no target read.
- **duckdb_quack_server.py** — the sole read-write DuckDB owner in Docker.
  Opens the `.duckdb` file (and attaches the DuckLake catalog if enabled),
  calls `quack_serve(token, port)`, and handles `SIGTERM`/`SIGINT` for a clean
  shutdown. Validates `QUACK_SERVER_TOKEN` (≥ 4 chars) and SQL-escapes it before
  interpolation.

### `ddd_dbt/`

- **generate_dbt_models.py** — writes the Bronze and Silver model files (plus
  the Silver `_cv` views), the Gold `_cv` views and the Silver grain test from the
  configuration; picks the incremental or full-extract macro per entity.
- **dbt_build_with_unique_logfile.py** — runs `dbt build` (optionally
  `--models_to_select`) with a JSON log to `DBT_LOGS_DIRECTORY`, and fails when
  the log contains failed nodes even if dbt exited 0.
- **init_duckdb.py** — installs extensions and creates persistent secrets: Azure
  when `STORAGE_TARGET=onelake`, S3 when either storage switch is `s3`.
- **dbt_quack.py** — monkey-patches `dbt-duckdb`'s DuckDB environment to connect
  via quack (RPC) instead of opening the file directly. Used in Docker where
  `duckdb_server` is the sole owner of the database file; activated by setting
  `QUACK_SERVER_HOST`.

### `ddd_dagster/`

- **definitions.py** — the `Definitions` entry point; resources
  `DltPipelineResource` (key `dlt_onelake`, a legacy name; logs are local) and
  `DbtCliResource` (key `dbt`).
- **assets.py**, **rfam_assets.py** — extraction asset factories (keys
  `ingestion/DDD/<entity>`, `ingestion/RFAM/<table>`), retry policy 2 × with
  exponential back-off, `date_to_load_from` overridable in the Launchpad.
- **dbt_assets.py** — dagster-dbt multi-assets for seeds, Bronze, Silver
  (`full_refresh` config), Gold and data_engineering; dbt sources map to the
  extraction assets.
- **export_assets.py** — one asset per Silver and Gold table, ordered by the
  no-op barriers `barrier_dbt_gold_complete` → Silver exports →
  `barrier_all_silver_exported` → Gold exports → `barrier_all_gold_exported`.
- **bronze_archive_assets.py** — `bronze_archive_asset` (group `maintenance`), run by the
  on-demand `bronze_archive_job`; wraps `archive_bronze.run_archive`.
- **ducklake_cleanup_assets.py** — `ducklake_cleanup_asset` (group
  `maintenance`); no-op unless `SILVER_STORAGE_FORMAT=ducklake`.
- **jobs.py** — dbt, DuckLake-cleanup and Bronze-archive jobs use `in_process_executor`;
  extraction and export jobs use `multiprocess_executor(max_concurrent=4)`;
  `full_pipeline_job` uses `multiprocess_executor` with
  `FULL_PIPELINE_MAX_CONCURRENT` (2) parallel steps.
- **schedules.py** — `danish_parliament_full_pipeline_schedule` (06:00) and
  `dbt_data_engineering_schedule` (08:00), both stopped by default.
- **sensors.py** — `danish_parliament_run_success_sensor` and
  `danish_parliament_run_failure_sensor` on every job: NDJSON run summary, ntfy.sh
  alert. Step-failure details are read directly from `$DAGSTER_HOME/history/runs/*.db`
  by the `dagster_step_failures_raw` dbt model (SQLite scan).
- **_constants.py** — shared retry policy and asset keys.

## dbt project (`dbt/`)

Details, macros and the CDC logic: [dbt_transformations.md](dbt_transformations.md).

- **Profile** `danish_democracy_data`, four targets, chosen by the `target:`
  expression in `profiles.yml`:
  1. `SILVER_STORAGE_FORMAT=ducklake` and `RAW_STORAGE_TARGET=s3` → `local_ducklake_s3`
  2. `SILVER_STORAGE_FORMAT=ducklake` → `local_ducklake` (attaches the catalog as `ducklake_catalog`)
  3. `STORAGE_TARGET=onelake` → `onelake` (adds the Azure secret)
  4. otherwise → `local`
- **Threads:** every target sets `threads: 1` and it must stay 1. Concurrency
  comes from Dagster; parallel dbt threads make DuckLake commits collide
  (`Duplicate key snapshot_id`).
- **Schemas:** `bronze`, `silver`, `gold`, `data_engineering` (DuckDB prefixes
  them: `main_silver`, …). In `ducklake` mode Silver is created in the
  `ducklake_catalog` database (`+database` in `dbt_project.yml`).
- **Materializations:** Bronze, Gold and data_engineering are views (except the
  tables `dagster_asset_materialization` and `dagster_step_failures_raw`);
  Silver is incremental.
- **Variables:** `bronze_columns_to_exclude_in_silver_hash`
  (`LKHS_date_inserted`, `LKHS_filename`), `hash_null_replacement` (`<NULL>`),
  `hash_delimiter` (`]##[`), `date_spine_start_date` (`1950-01-01`), and the
  Silver switches `silver_delete_guard` (`true`), `silver_delete_guard_fraction`
  (0.1), `silver_delete_guard_min_rows` (10) and `silver_skip_late_files`
  (`false`); the Silver Dagster assets take `delete_guard` and `skip_late_files`
  in their run config, `dbt_build_with_unique_logfile` takes
  `--no_silver_delete_guard` and `--skip_late_files`.
- **`on-run-start`** recreates the S3 secret when `RAW_STORAGE_TARGET=s3`.
- **Helper tables** `_last_file` (bookmark) and `_files` (log of the Bronze
  files read) are always written as
  `{{ this.database }}.{{ this.schema }}.{{ this.name }}_…`, so in DuckLake mode
  they live in the same database as the model (DuckDB forbids writing two
  databases in one transaction).
- **Model counts:** 28 Bronze (18 + 7 entities, plus 3 utility views), 50
  Silver, 19 Gold, 8 data_engineering; 2 seeds; 314 data tests.

### `LKHS_` columns

| Column | Meaning |
| --- | --- |
| `LKHS_filename` | source file name |
| `LKHS_source_system_code` | `DDD` or `RFAM` |
| `LKHS_hash_value` | SHA-256 of the business columns |
| `LKHS_date_valid_from` | SCD Type 2 validity start (file-name timestamp) |
| `LKHS_cdc_operation` | `I`, `U` or `D` |
| `LKHS_date_inserted` | dbt run start that wrote the row |

## Storage switches

| Switch | Values | Controls |
| --- | --- | --- |
| `RAW_STORAGE_TARGET` | `local` \| `s3` | Bronze files and DuckLake Parquet data |
| `SILVER_STORAGE_FORMAT` | `duckdb` \| `ducklake` | Silver table storage |
| `STORAGE_TARGET` | `local` \| `s3` \| `onelake` | Delta Lake export destination only |

All twelve combinations, paths, example configurations and RustFS setup:
[storage_combinations.md](storage_combinations.md). Key facts:

- The `.duckdb` file and the DuckLake catalog file are always local; logs are
  always local. OneLake is only an export destination.
- With S3 raw storage, `DANISH_DEMOCRACY_DATA_SOURCE` / `RFAM_DATA_SOURCE` must
  point at `s3://<bucket>/[<prefix>/]Files/Bronze/{DDD,RFAM}`, and buckets must
  exist before the first extraction. RustFS needs `S3_URL_STYLE=path`.
- With S3 raw storage and DuckLake, `DUCKLAKE_DATA_PATH` is derived from
  `S3_BUCKET_DUCKLAKE` / `S3_PREFIX_DUCKLAKE`.
- Changing a switch moves no data. Silver is the state, so copy it: for `duckdb` →
  `ducklake`, run `python -m ddd_python.ddd_utils.migrate_silver_to_ducklake`. A first
  build in the new location instead derives the history from the Bronze files that
  are there, without the deletes of incremental entities.
- DuckLake keeps very small tables inline in the catalog; downstream tools
  (Metabase, DBeaver) need DuckDB JDBC ≥ 1.5.3 and an init SQL that loads
  `ducklake` and attaches the catalog (`docker/metabase-duckdb-init.sql`).

### DuckLake maintenance and backup

- `ducklake_cleanup_job` (manual) expires snapshots older than 31 days, deletes
  orphaned files and removes leftover `*_current_temp` directories. It never
  touches `*__dbt_tmp` directories, which hold live table data.
- The platform backup adds a `ducklake` target in DuckLake mode and runs it
  **before** `duckdb` (which contains the catalog), so the catalog never
  references a file the backup missed. With DuckLake data on S3 the local
  `ducklake` target is skipped; bucket versioning provides durability.
- `duckdb` and `ducklake` backups stop `ddd-duckdb-server`, the only process with
  the database files open; the `metabase` target stops `ddd-metabase` to copy
  Metabase's own application database.

## Naming conventions

- **Danish characters** in identifiers: ø→oe, æ→ae, å→aa (`Aktør` → `aktoer`).
- **dbt models:** `{layer}_{source}_{entity}` for Bronze and Silver
  (`bronze_ddd_aktoer`, `silver_rfam_family`); English names in Gold (`actor`,
  `vote`, `case`); `_cv` = current version.
- **Bronze files:** `<entity>_<YYYYMMDD_HHMMSS>.json` (NDJSON); a full extraction of an
  incremental entity is `<entity>_<YYYYMMDD_HHMMSS>_full.json`.
- **Dagster asset keys:** `ingestion/DDD/<entity>`, `ingestion/RFAM/<table>`,
  `ingestion/seeds/<seed>`, `export/silver/<table>`, `export/gold/<table>`;
  dbt models keep their path-based keys.
- **Dagster groups:** `ingestion_DDD_incremental`, `ingestion_DDD_full_extract`,
  `ingestion_RFAM_incremental`, `ingestion_RFAM_full_extract`,
  `ingestion_seeds`, `ddd_bronze`, `ddd_silver`, `ddd_gold`, `data_engineering`,
  `export_silver`, `export_gold`, `maintenance`.

## Key design patterns

1. One source of truth for entities (`configuration_variables.py`).
2. Asset factories and generated dbt models; Gold hand-modelled.
3. Lazy environment variables (PEP 562).
4. Hash-based CDC into SCD Type 2; `_cv` views for the current state.
5. Three independent storage switches.
6. Single-writer DuckDB: in Docker, `duckdb_server` is the sole read-write owner;
   all other containers connect via quack RPC, so parallel Dagster steps are
   separate clients of one writer. Outside Docker there is no server and only
   one process may write the `.duckdb` file at a time.
7. Stateless incremental extraction (lookback window).
8. Every pipeline call logs one NDJSON record; logging never breaks a run.

The trade-offs behind each are in [design_decisions.md](design_decisions.md).

## Defensive practices

- `resolve_date_to_load_from` rejects any date that is not `YYYY-MM-DD` before
  it reaches the OData filter; the Rfam filter is a bound SQLAlchemy parameter.
- `_scrub_secrets` redacts keys containing `secret`, `password`, `token` or
  `connection_string` before parameters are logged.
- `connect_timeout=30` on the MySQL engine; `engine.dispose()` in `finally`.
- Log-write failures surface as `warnings.warn`, never as a broken run.
- OData responses must contain `"value"`, or the extraction fails fast.
- Containers run as non-root `app` (UID 1000); Metabase as UID 2000.

## ntfy.sh alerting

Both run-status sensors send a push notification after writing the run summary.

| | Success | Failure |
| --- | --- | --- |
| Title | `Dagster run SUCCEEDED - <job_name>` | `Dagster run FAILED - <job_name>` |
| Priority | `default` | `high` |
| Tag | `white_check_mark` | `rotating_light` |
| Body | `Job: <job_name>`, `Run ID: <first 8 chars>`, `Environment: <ENVIRONMENT>` | same |

- Opt-in: nothing is sent when `NTFY_TOPIC` (topic name only, no URL) is unset.
- A failed POST is logged as a warning and never fails the sensor.
- All jobs are covered; new jobs automatically.
- HTTP headers must be ASCII, so the title uses a plain hyphen.

## Environment variables

Defined in `.env` (template: `.env.example`). In Docker, `docker-compose.yml`
overrides the path variables with container paths.

| Variable | Purpose |
| --- | --- |
| `STORAGE_TARGET` | Delta export destination: `local` (default), `s3`, `onelake` |
| `RAW_STORAGE_TARGET` | Bronze files + DuckLake data: `local` (default), `s3` |
| `SILVER_STORAGE_FORMAT` | `duckdb` (default) or `ducklake` |
| `LOCAL_STORAGE_PATH` | root for local files (default `data`) |
| `DANISH_DEMOCRACY_DATA_SOURCE`, `RFAM_DATA_SOURCE` | where dbt reads Bronze (local path or `s3://` URI) |
| `DANISH_DEMOCRACY_DATA_SOURCE_DOCKER`, `RFAM_DATA_SOURCE_DOCKER` | Docker only: override the container read paths (for S3) |
| `DANISH_DEMOCRACY_BASE_URL` | OData API base URL |
| `DANISH_DEMOCRACY_DEFAULT_DAYS_TO_LOAD` | DDD lookback window in days (default 31) |
| `RFAM_CONNECTION_STRING` | Rfam MySQL connection string |
| `RFAM_DEFAULT_DAYS_TO_LOAD` | Rfam lookback window in days (default 365) |
| `DUCKDB_DATABASE_LOCATION` | path to the `.duckdb` file (always local) |
| `DUCKDB_DATABASE` | main database name (= file stem) |
| `DUCKLAKE_CATALOG_LOCATION` | DuckLake catalog file (required in `ducklake` mode) |
| `DUCKLAKE_DATA_PATH` | DuckLake Parquet directory (local mode; derived in S3 mode) |
| `S3_ENDPOINT`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_REGION`, `S3_USE_SSL`, `S3_URL_STYLE` | shared S3 settings (when either switch is `s3`) |
| `S3_BUCKET_BRONZE`, `S3_PREFIX_BRONZE` | Bronze bucket (raw `s3`) |
| `S3_BUCKET_DUCKLAKE`, `S3_PREFIX_DUCKLAKE` | DuckLake bucket (raw `s3` + `ducklake`) |
| `S3_BUCKET_DELTA`, `S3_PREFIX_DELTA` | Delta export bucket (export `s3`) |
| `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` | service principal (export `onelake`) |
| `FABRIC_WORKSPACE`, `FABRIC_ONELAKE_STORAGE_ACCOUNT` | OneLake workspace and account (normally `onelake`) |
| `FABRIC_ONELAKE_FOLDER_SILVER`, `FABRIC_ONELAKE_FOLDER_GOLD` | OneLake export folders |
| `FABRIC_ONELAKE_MOUNT` | optional local mount point for OneLake |
| `AZURE_SUBSCRIPTION_ID`, `AZURE_RESOURCE_GROUP`, `FABRIC_CAPACITY_NAME` | optional; for the capacity pause/resume CLI |
| `DBT_PROJECT_DIRECTORY`, `DBT_MODELS_DIRECTORY` | dbt project and models paths |
| `DBT_LOGS_DIRECTORY` | dbt JSON logs |
| `DBT_EXECUTABLE` | dbt binary Dagster calls (default `dbt` on `PATH`) |
| `DBT_FRESHNESS_WARN_AFTER_DAYS`, `DBT_FRESHNESS_ERROR_AFTER_DAYS` | source freshness thresholds (2 / 7) |
| `DLT_PIPELINES_DIR` | dlt working directory |
| `DLT_PIPELINE_RUN_LOG_DIR` | NDJSON run logs (default `LOCAL_STORAGE_PATH/logs`) |
| `BRONZE_ARCHIVE_DIR` | where processed Bronze files are archived (default `LOCAL_STORAGE_PATH/archive/Bronze`) |
| `DAGSTER_HOME` | Dagster storage (SQLite) |
| `ENVIRONMENT`, `NTFY_TOPIC` | alert label and ntfy.sh topic |
| `QUACK_SERVER_TOKEN` | shared secret for the quack RPC server (required in Docker; ≥ 4 chars) |
| `QUACK_SERVER_HOST` | quack server hostname (e.g. `duckdb_server`; empty = direct file access) |
| `QUACK_SERVER_PORT` | quack server port (default `9494`) |
| `QUACK_SERVER_MEMORY_LIMIT` | DuckDB `memory_limit` of the server (default `12GB`, above the 8g container limit on purpose: DuckDB 2.0 reserves 32 MiB per file for `union_by_name` JSON reads) |

Backup settings (`*_BACKUP_DIR`, `*_BACKUP_MAX_AGE_DAYS`, `BACKUP_LOG_DIR`,
`FLUSH_WAIT_SECONDS`, `HETZNER_STORAGEBOX_*`) are documented in `.env.example`
and the docstring of `ddd_utils/backup_platform.py`.

## DuckDB initialisation

- **Docker:** with `STORAGE_TARGET=onelake`, `docker/docker-entrypoint.sh`
  installs httpfs/azure/delta and creates the persistent `azure_sp` secret.
- **Host shell:** `duckdb/init_duckdb.sh` loads `.env` and runs
  `duckdb/init_duckdb.sql` (same, plus `azure_transport_option_type = 'curl'`).
- **Python:** `python -m ddd_python.ddd_dbt.init_duckdb` does the same and also
  creates the S3 secret when either storage switch is `s3`.
- dbt runs with `RAW_STORAGE_TARGET=s3` recreate the S3 secret in `on-run-start`.

## Running the project

### Local (no Docker)

```bash
python3.13 -m venv .venv && source .venv/bin/activate
pip install -e ".[dagster,dev]"
cp .env.example .env                       # fill in the paths
set -a && source .env && set +a            # for the dbt CLI

python -m ddd_python.ddd_dbt.generate_dbt_models                         # only after entity changes
python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data
python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_rfam
python -m ddd_python.ddd_dbt.dbt_build_with_unique_logfile               # dbt build
python -m ddd_python.ddd_dlt.export_silver                               # optional Delta export
python -m ddd_python.ddd_dlt.export_gold

dagster dev -w workspace.yaml                                            # UI on :3000
```

### Docker

`QUACK_SERVER_TOKEN` must be set in `.env` before starting any service (minimum
4 characters; generate with `python3 -c "import secrets; print(secrets.token_hex(32))"`).

```bash
docker compose build
docker compose up -d duckdb_server          # start the sole DuckDB writer first
docker compose run --rm run python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data
docker compose run --rm run python -m ddd_python.ddd_dbt.dbt_build_with_unique_logfile
docker compose up -d dagster metabase       # Dagster :3000, Metabase :3001
```

Host setup, backups, RustFS and first-time Metabase quack setup: [docker/DOCKER_USAGE.md](../docker/DOCKER_USAGE.md).

### Tests

```bash
pytest tests/
ruff check . && ruff format --check . && mypy --package ddd_python
```

What each test module covers and how CI runs: [testing.md](testing.md).

## Common tasks

- **Add a DDD entity:** add it to `DANISH_DEMOCRACY_FILE_NAMES` (and
  `..._INCREMENTAL` if incremental) and `DANISH_DEMOCRACY_TABLE_PRIMARY_KEYS`,
  then `python -m ddd_python.ddd_dbt.generate_dbt_models`. Extraction, dbt and
  export assets follow automatically; Gold modelling is manual.
- **Add an Rfam table:** update `RFAM_TABLE_NAMES`, `RFAM_TABLE_PRIMARY_KEYS`,
  `RFAM_TABLE_QUERIES`, then regenerate.
- **Change CDC logic:** edit the Silver macros in `dbt/macros/`; keep helper
  tables qualified with `{{ this.database }}`.
- **Repair or rebuild Silver:** Silver is the state and ignores `--full-refresh`;
  restore a backup, or replay from Bronze and the archive. See
  [runbook.md §7](runbook.md#7-silver-needs-a-rebuild).
- **Archive Bronze:** run `bronze_archive_job` (or `archive_bronze --execute`) when the
  Bronze folders have grown.
- **Find deletes of incremental entities:** process the waiting files, run a full
  extraction (`--date_to_load_from 1900-01-01`), then run Silver with its `_full`
  file as the only new file.
- **Switch Silver storage:** set `SILVER_STORAGE_FORMAT`, then migrate or
  rebuild (see *Storage switches*).
- **Modify Gold:** edit `dbt/models/gold/` directly; only the `_cv` views are
  generated.
