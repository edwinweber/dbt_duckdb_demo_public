# CLAUDE.md — Project Context for AI Assistants

## Project Overview

**Danish Democracy Data (dbt + DuckDB Demo)** — a data engineering pipeline that
ingests open data from the **Danish Parliament OData API** (18 entities) and the
**Rfam public MySQL database** (7 tables), transforms it through a
**Bronze → Silver → Gold medallion architecture** inside DuckDB, and optionally
exports the result as Delta Lake tables to Microsoft Fabric OneLake.

- **Repo:** `edwinweber/dbt_duckdb_demo`
- **Default branch:** `main`
- **License:** see LICENSE file
- A learning/reference project demonstrating common data engineering patterns on a low-cost, open-source stack.

## Tech Stack

| Layer           | Technology                                           |
|-----------------|------------------------------------------------------|
| Language        | Python ≥3.12                                         |
| Orchestration   | Dagster ≥1.12 (software-defined assets)              |
| Extraction      | dlt ≥1.24 (Data Load Tool)                           |
| Transformation  | dbt-core ≥1.10,<1.12 + dbt-duckdb ≥1.10             |
| Query engine    | DuckDB ≥1.5.1,<1.6                                   |
| Silver storage  | DuckDB native tables (default) **or** DuckLake (Parquet + catalog) |
| Data quality    | dbt-utils 1.3.0 + dbt built-in tests                 |
| Cloud storage   | Microsoft Fabric OneLake (ADLS Gen2 / Delta Lake)    |
| Export          | DuckDB `delta_scan` (dedup read) + deltalake ≥1.5 / PyArrow ≥17 (write) |
| SQL source      | SQLAlchemy ≥2.0, PyMySQL ≥1.1                        |
| Container       | Docker + Docker Compose                               |
| Testing         | pytest ≥8.0                                          |

## Architecture

```text
Extraction (dlt)
   ↓  JSON / Parquet files
Bronze (DuckDB views over raw files)
   ↓  hash-based CDC, SCD Type 2
Silver (DuckDB native tables OR DuckLake Parquet + _cv current-version views)
   ↓  star-schema modeling
Gold (DuckDB views — facts & dimensions)
   ↓  Delta Lake export
OneLake / local filesystem
```

The **Silver storage format** is switchable via `SILVER_STORAGE_FORMAT`
(`duckdb` default | `ducklake`) — see *Silver Storage Format* below. This is
orthogonal to `STORAGE_TARGET`, which only controls the Delta Lake **export**
destination (local vs OneLake).

Orchestrated by **Dagster** (two daily schedules: 06:00 full pipeline +
08:00 data engineering, Europe/Copenhagen, disabled by default). DuckLake
catalog cleanup is a separate **manual** job (no schedule).

## Directory Structure

```text
├── ddd_python/                  Python package (the code)
│   ├── ddd_dagster/             Dagster assets, jobs, schedules, sensors, resources
│   ├── ddd_dlt/                 dlt extraction pipelines + Delta Lake export
│   ├── ddd_dbt/                 dbt model generator + dbt runner + DuckDB init
│   └── ddd_utils/               Configuration, env vars, Azure/Fabric clients
├── dbt/                         dbt project
│   ├── models/bronze/           53 views (read_json_auto over raw files)
│   ├── models/silver/           50 models (CDC tables + _cv views)
│   ├── models/gold/             19 models (star-schema views)
│   ├── models/data_engineering/ 8 observability models (Dagster SQLite)
│   ├── macros/                  9 Jinja macros (model factories, hash, CDC)
│   ├── seeds/                   Seed CSVs (Danish public holidays, source registry)
│   └── dbt_project.yml          Project config + variables
├── tests/                       pytest tests (15 modules, 132 tests)
├── duckdb/                      DuckDB init scripts (extensions + Azure secret)
├── dlt/pipelines_dir/           dlt incremental state (git-ignored)
├── data/                        Local storage root (git-ignored)
│   └── Files/{Bronze,Silver,Gold}/
├── documentation/               Handbook markdown + build scripts
├── docker-compose.yml           Services: 'run' (one-off) + 'dagster' (UI)
├── Dockerfile                   Python 3.12 + DuckDB CLI v1.5.3
├── pyproject.toml               Dependencies + build config
├── workspace.yaml               Dagster workspace (loads ddd_dagster.definitions)
└── .env.example                 Template for environment variables
```

## Data Sources

### Danish Parliament API (DDD)

- **Base URL:** `https://oda.ft.dk/api`
- **18 OData entities:** Afstemning, Afstemningstype, Aktør, Aktørtype, Møde,
  Mødestatus, Mødetype, Periode, Sag, Sagstrin, SagstrinAktør,
  Sagstrinsstatus, Sagstrinstype, Sagskategori, Sagsstatus, Sagstype,
  Stemme, Stemmetype
- **Incremental (6):** Aktør, Møde, Sag, Sagstrin, SagstrinAktør, Stemme
  — filtered by `$filter=opdateringsdato ge DateTime'...'`
- **Full-extract (12):** the remaining small lookup tables
- **All primary keys:** `id`

### Rfam MySQL Database

- **Connection:** `mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam`
  (public read-only)
- **7 tables:** family, genome, clan, clan_membership, author,
  literature_reference, dead_family
- **Incremental (2):** family (pk `rfam_acc`, date `updated`), genome
  (pk `upid`, date `updated`)
- **Full-extract (5):** remaining tables (no date column)
- **Primary keys vary:** `rfam_acc`, `upid`, `clan_acc`, `author_id`, `pmid`
- **SQL templates** with `{where_clause}` placeholder in
  `configuration_variables.py → RFAM_TABLE_QUERIES`

## Python Package (`ddd_python/`)

### `ddd_utils/configuration_variables.py` — THE canonical source of truth

All entity lists, model names, primary keys, date columns, and SQL queries are
defined here. Adding a new entity means updating _only_ this file; the rest of
the codebase (generation, macros, tests) derives from it.

Key constants:

- `DANISH_DEMOCRACY_FILE_NAMES` (18), `_INCREMENTAL` (6)
- `DANISH_DEMOCRACY_MODELS_BRONZE` (18), `_SILVER` (18), `_GOLD` (10)
- `DANISH_DEMOCRACY_TABLE_PRIMARY_KEYS` (all `id`)
- `RFAM_TABLE_NAMES` (7), `_INCREMENTAL` (2)
- `RFAM_MODELS_BRONZE` (7), `_SILVER` (7)
- `RFAM_TABLE_PRIMARY_KEYS`, `RFAM_TABLE_DATE_COLUMNS`, `RFAM_TABLE_QUERIES`
- `SILVER_TABLE_PRIMARY_KEYS` — combined DDD + Rfam Silver model → PK mapping

### `ddd_utils/string_utils.py` — String and date utilities

- `normalize_danish_name(name)` — converts Danish chars (ø→oe, æ→ae, å→aa) and
  lowercases; used by every layer that maps API entity names to file-system/schema
  identifiers. Single canonical implementation imported by all callers.
- `resolve_date_to_load_from(date, default_days, reference_time)` — validates or
  computes the `YYYY-MM-DD` lower-bound date for incremental extraction; used by
  both dlt extraction scripts and both Dagster incremental-asset factories.

### `ddd_utils/path_utils.py` — Storage path utilities

- `build_bronze_destination_path(source_system_code, entity_name)` — returns the
  Bronze directory path for dlt (relative for local, OneLake folder path for
  OneLake). Also re-exported from `ddd_dagster/_constants.py` for backward compat.
- `build_delta_export_path(layer, table)` — returns `(path, storage_options)` for
  `write_deltalake`; handles local vs OneLake switch and `os.makedirs` for local.
  Used by both Silver and Gold export scripts.
- `silver_storage_is_ducklake()` — `True` when `SILVER_STORAGE_FORMAT=ducklake`;
  the predicate that decides whether an export connection attaches the catalog.
- `open_export_connection()` — the shared read-only DuckDB connection for the
  Silver/Gold Delta exports. Opens the main DuckDB file and, in DuckLake mode,
  attaches the DuckLake catalog read-only as `ducklake_catalog`. Used by both
  export scripts and the Dagster export assets.

### `ddd_utils/get_variables_from_env.py` — Lazy environment loading

Uses `__getattr__` so that importing the module for code generation or testing
does **not** fail when Azure credentials are absent. Required vars raise only
on first access.

### `ddd_dagster/` — Dagster orchestration

- **assets.py** — DDD extraction (18× asset factory with retry policy)
- **rfam_assets.py** — Rfam extraction (7× asset factory)
- **dbt_assets.py** — dagster-dbt integration for Bronze/Silver/Gold/Data Engineering + seeds
- **export_assets.py** — Silver (incremental) + Gold (full overwrite) → Delta Lake
- **ducklake_cleanup_assets.py** — `ducklake_cleanup_asset` (group `maintenance`):
  vacuums the DuckLake catalog; no-op unless `SILVER_STORAGE_FORMAT=ducklake`
- **sensors.py** — Two `@run_status_sensor` definitions covering **all** Dagster jobs:
  - `danish_parliament_run_success_sensor` — fires on SUCCESS: writes an NDJSON run
    summary to the configured log destination and sends a ntfy.sh push notification.
  - `danish_parliament_run_failure_sensor` — fires on FAILURE: same, but with
    high-priority ntfy.sh alert. Both sensors are enabled by default
    (`DefaultSensorStatus.RUNNING`). The ntfy.sh alert is skipped when `NTFY_TOPIC`
    is not set, so notifications are opt-in.
- **jobs.py** — Pipelines (incremental, full-extract, all, full-pipeline,
  ducklake-cleanup)
  - dbt + ducklake-cleanup jobs: `in_process_executor` (DuckDB single-writer constraint)
  - Extraction/export: `multiprocess_executor` (max_concurrent=4)
- **schedules.py** — Two daily schedules (06:00 full pipeline + 08:00 data
  engineering, Europe/Copenhagen, disabled by default). The DuckLake cleanup
  job is intentionally **not** scheduled — run it manually.

### `ddd_dlt/` — Extraction & export

- **dlt_pipeline_execution_functions.py** — `api_to_file()`, `sql_to_file()`,
  `file_to_file()` — core extraction engine
- **dlt_run_extraction_pipelines_danish_parliament_data.py** — DDD orchestrator
  (CLI: `--date_to_load_from`, `--file_names`; ThreadPoolExecutor max_workers=4)
- **dlt_run_extraction_pipelines_rfam.py** — Rfam orchestrator
- **export_main_silver_to_fabric_silver.py** — Silver → Delta Lake (incremental
  append). The dedup read runs inside DuckDB via `delta_scan('<target>')`
  (anti-join on `pk + LKHS_date_valid_from`) instead of loading the target Delta
  table into PyArrow. The write still uses `deltalake.write_deltalake` —
  DuckDB's delta extension is **read-only** at the pinned version (no
  `COPY … (FORMAT delta)`). *Future:* newer DuckDB builds add a Delta writer but
  it has an **Azure/OneLake regression**, so the write can't move off
  `deltalake` yet — revisit dropping `deltalake` on the next DuckDB bump, once
  both a writer-capable version is pinned **and** the Azure regression is fixed.
  **Storage-format aware:** the shared `open_export_connection()`
  (`ddd_utils/path_utils.py`) attaches the DuckLake catalog read-only in
  `ducklake` mode; `_silver_source_database()` then reads Silver from
  `ducklake_catalog.main_silver.*` (ducklake) or `<DUCKDB_DATABASE>.main_silver.*`
  (duckdb).
- **export_main_gold_to_fabric_gold.py** — Gold → Delta Lake (full overwrite;
  no target read, so no `delta_scan` — still PyArrow + `write_deltalake`).
  **DuckLake-aware:** uses the same shared `open_export_connection()`, so in
  `ducklake` mode the catalog is attached and the Gold views (which reference
  `ducklake_catalog.main_silver`) resolve. Gold's source stays
  `<DUCKDB_DATABASE>.main_gold` in both modes.

### `ddd_dbt/` — dbt tooling

- **generate_dbt_models.py** — Reads config lists → generates Bronze/Silver/Gold SQL files
  by instantiating Jinja macros. Incremental vs full-extract macro selection
  derived from `DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL` at runtime.
- **dbt_build_with_unique_logfile.py** — Runs `dbt build`, captures JSON log,
  optionally uploads to OneLake.
- **init_duckdb.py** — Database initialization (runs `init_duckdb.sql`).

## dbt Project (`dbt/`)

### Configuration

- **Profile:** `danish_democracy_data` with three targets:
  - `local` — DuckDB on local disk (extensions: httpfs, parquet, delta, sqlite)
  - `local_ducklake` — same as `local` plus the `ducklake` extension and an
    `attach:` block that mounts the DuckLake catalog as the `ducklake_catalog`
    database (`data_path` = `DUCKLAKE_DATA_PATH`)
  - `onelake` — DuckDB with Azure secret (extensions: + azure)
- **Target selection** (`profiles.yml` `target:` expression):
  - If `SILVER_STORAGE_FORMAT=ducklake` → always `local_ducklake`
    (regardless of `STORAGE_TARGET`)
  - Otherwise → `STORAGE_TARGET` (`local` | `onelake`, default `local`)
- **Schemas:** `bronze`, `silver`, `gold`
- **Materialization:** Bronze=view, Silver=table (incremental), Gold=view
  - In `ducklake` mode the Silver tables are created in the `ducklake_catalog`
    database (`+database` in `dbt_project.yml`); Bronze and Gold stay in the
    main DuckDB file. dbt reads Bronze cross-database and Gold's `{{ ref() }}`
    resolves Silver to `ducklake_catalog.main_silver.*` automatically.

### Variables (`dbt_project.yml`)

- `bronze_columns_to_exclude_in_silver_hash`: `LKHS_date_inserted`,
  `LKHS_pipeline_execution_inserted`, `LKHS_filename`
- `hash_null_replacement`: `<NULL>`
- `hash_delimiter`: `]##[`

### dbt Macros (9 files in `dbt/macros/`)

1. `cast_hash_to_bigint.sql` — UBIGINT → BIGINT (Power BI compat)
2. `generate_base_for_hash.sql` — Build column list for SHA256 hashing
3. `generate_model_bronze.sql` — Bronze view factory (`read_json_auto`)
4. `generate_model_bronze_latest.sql` — Latest-snapshot view factory
5. `generate_model_silver_incr_extraction.sql` — CDC for incremental tables
6. `generate_model_silver_full_extraction.sql` — CDC for full-extract tables
7. `generate_pre_hook_silver.sql` — Pre-hook: `_last_file` tracking table
8. `generate_pre_hook_silver_full_refresh.sql` — Pre-hook for full refresh
9. `generate_post_hook_silver.sql` — Post-hook: rebuild `_last_file` table

> **DuckLake note:** every Silver tracking table the macros create
> (`_last_file`, `_current_temp`) is written with a fully-qualified
> `{{ this.database }}.{{ this.schema }}.{{ this.name }}_…` name. This is
> required for DuckLake mode: DuckDB forbids one transaction from writing to
> two databases, so the helper tables must live in the **same** database as
> the model (`ducklake_catalog`). In `duckdb` mode `this.database` is simply
> the main database, so the same code path works unchanged.
> `generate_pre_hook_silver.sql` also drops `_last_file` on `--full-refresh`
> so the filename watermark resets and all Bronze files are reprocessed.

### Custom Column Prefix: `LKHS_`

All data-warehouse tracking columns use the `LKHS_` prefix:

- `LKHS_hash_value` — SHA256 hash of row content
- `LKHS_date_valid_from` — SCD Type 2 validity start
- `LKHS_cdc_operation` — I (insert), U (update), D (delete)
- `LKHS_date_inserted` — extraction timestamp
- `LKHS_date_inserted_src` — source system timestamp
- `LKHS_filename` — source file name
- `LKHS_pipeline_execution_inserted` — pipeline run identifier

### Model Counts

- **Bronze:** 53 views (18 DDD + 7 Rfam = 25 entities × main + `_latest` + 3 utility)
- **Silver:** 50 models (25 CDC tables + 25 `_cv` current-version views)
- **Gold:** 19 models (10 star-schema views + 8 `_cv` views + `time` utility)

## Silver Storage Format (DuckDB vs DuckLake)

The Silver layer can be stored two ways, chosen by the `SILVER_STORAGE_FORMAT`
env var (validated eagerly in `get_variables_from_env.py`):

| `SILVER_STORAGE_FORMAT` | Silver tables live in | dbt target | Files on disk |
|-------------------------|-----------------------|------------|---------------|
| `duckdb` (default)      | main `.duckdb` file, `main_silver` schema (BASE TABLE) | `local`/`onelake` (per `STORAGE_TARGET`) | inside the DuckDB binary |
| `ducklake`              | `ducklake_catalog` (DuckLake), `main_silver` schema | `local_ducklake` | Parquet under `DUCKLAKE_DATA_PATH`, metadata in the catalog `.ducklake` file |

Key facts:

- **Independent of `STORAGE_TARGET`.** DuckLake is always local; the Delta Lake
  export (Silver/Gold → local or OneLake) is unchanged in either mode.
- **Catalog auto-creates.** The catalog `.ducklake` file and the data directory
  are created on first `ATTACH` — no manual setup.
- **Inline small tables.** DuckLake stores very small tables *inline in the
  catalog* rather than as Parquet (so not every table appears as a `.parquet`
  file on disk). Force them out with `CALL ducklake_flush_inlined_data(...)`.
- **Bronze/Gold unaffected.** Both stay in the main DuckDB file; Gold's
  `{{ ref() }}` to Silver resolves to `ducklake_catalog.main_silver.*`
  automatically.
- **Single-writer still applies.** dbt's primary connection always opens the
  main `.duckdb` file read-write (Bronze views, Gold views), so a dbt run still
  needs an exclusive lock on it — Metabase **and** any DBeaver/host connection
  to the `.duckdb` file must be closed during a run, in *both* modes.
- **Downstream tools** (Metabase, DBeaver) must load the `ducklake` extension
  and `ATTACH` the catalog to read Silver/Gold in DuckLake mode. Metabase: the
  extension is baked into `Dockerfile.metabase`, `/data/ducklake` is mounted,
  and the connection init-SQL runs `LOAD ducklake; ATTACH …`. DBeaver: needs the
  DuckDB JDBC driver **≥1.5.3** (1.5.1 lacks DuckLake) plus an `init_sql`
  driver property that runs `LOAD ducklake; ATTACH …`.

### DuckLake maintenance

`ducklake_cleanup_asset` / `ducklake_cleanup_job` (group `maintenance`) vacuums
the catalog: expires snapshots older than 31 days, deletes catalog-orphaned files, and removes
residual `*_current_temp` directories left by the Silver pre/post-hooks. It
**deliberately skips `*__dbt_tmp` directories** — DuckLake stores live table data
there, so deleting them corrupts Silver. The asset is a no-op when
`SILVER_STORAGE_FORMAT != ducklake`. It is a **manual** job (no schedule): at this
scale the catalog accumulates only trivial orphaned data, so run it on demand
after a large `--full-refresh` rather than on a daily cron.

### DuckLake backup

The platform backup (`ddd_utils/backup_platform.py`, targets defined in
`backup_common.py`) is DuckLake-aware. When `SILVER_STORAGE_FORMAT=ducklake` it
adds a **`ducklake`** target that archives the DuckLake **data files**
(`DUCKLAKE_DATA_PATH`, e.g. `/data/ducklake`). The DuckLake **catalog**
(`.ducklake` file) lives in the DuckDB directory and is already captured by the
**`duckdb`** target. Targets run **`ducklake` → `duckdb`** so the data files are
archived **before** the catalog: the catalog references the files, so files-first
ensures the catalog snapshot never points at a file the backup missed. In native
`duckdb` mode the `ducklake` target is omitted. The `backup` Docker service
mounts `/data/ducklake` and sets `DUCKLAKE_BACKUP_DIR=/data_backup/ducklake`.
Restore (`restore_platform.py`) is target-driven and handles `ducklake`
automatically.

## Naming Conventions

- **Danish characters** in table/model names: ø→oe, æ→ae, å→aa
  (e.g., `Aktør` → `bronze_ddd_aktoer`)
- **dbt models:** `{layer}_{source}_{entity}` — `bronze_ddd_aktoer`,
  `silver_rfam_family`, `gold_actor`
- **File timestamps:** `{entity}_{YYYYMMDD_HHMMSS}.json`
- **Dagster asset groups:** `ingestion/DDD`, `ingestion/Rfam`,
  `transform/dbt_*`, `export/*`

## Key Design Patterns

1. **Single source of truth** — all entity lists in `configuration_variables.py`
2. **Asset factory** — Dagster assets created via factory functions (DRY)
3. **Lazy env vars** — `__getattr__` defers credential loading
4. **Hash-based CDC** — SHA256 on all non-tracking columns detects changes
5. **SCD Type 2** — full history in Silver; `_cv` views expose current version
6. **Dual-mode export** — `STORAGE_TARGET=local|onelake` switches the Delta
   Lake export destination
7. **Switchable Silver storage** — `SILVER_STORAGE_FORMAT=duckdb|ducklake`
   stores Silver as native DuckDB tables or DuckLake Parquet (independent of
   `STORAGE_TARGET`); see *Silver Storage Format*
8. **Concurrent extraction** — ThreadPoolExecutor(max_workers=4) for I/O
9. **Single-writer dbt** — `in_process_executor` for DuckDB constraint
10. **Generated SQL** — Bronze/Silver models auto-generated from config; Gold
    mostly generated, except `individual_votes.sql` (handcrafted)

## Defensive Practices

A few defensive measures applied throughout the codebase (it remains a
single-node design, not a high-availability platform):

- **SQL injection defense** — Date parameters validated with `re.fullmatch(r"\d{4}-\d{2}-\d{2}")`
  before interpolation into SQL queries (Rfam extraction + Dagster assets)
- **Specific exception handling** — Azure `ResourceNotFoundError` caught explicitly
  in OneLake log writer instead of bare `except Exception:`
- **Database connection safety** — `connect_timeout=30` on `create_engine()` prevents
  indefinite hangs; `engine.dispose()` in `finally` prevents connection pool leaks
- **Observable failures** — Log write errors surfaced via `warnings.warn()` instead
  of silently swallowed with `except: pass`
- **API response validation** — OData responses checked for expected `"value"` key
  before processing to fail fast on malformed responses
- **Non-root Docker** — Container runs as `appuser` (UID 1000) to limit
  container-escape risk

## ntfy.sh Alerting

Both run-status sensors (`danish_parliament_run_success_sensor` and
`danish_parliament_run_failure_sensor`) send push notifications via
[ntfy.sh](https://ntfy.sh) after writing the OneLake/local log record.

### Alert configuration

| Variable      | Purpose                                                                 |
|---------------|-------------------------------------------------------------------------|
| `NTFY_TOPIC`  | Topic name only — **no** `https://ntfy.sh/` prefix (e.g. `my-topic`). Leave unset to disable alerts. |
| `ENVIRONMENT` | Label included in every alert message (e.g. `PROD`, `DEV`).            |

### Notification format

| Field     | SUCCESS                              | FAILURE                                    |
|-----------|--------------------------------------|--------------------------------------------|
| Title     | `Dagster run SUCCEEDED - <job_name>` | `Dagster run FAILED - <job_name>`          |
| Priority  | `default`                            | `high`                                     |
| Tag       | ✅ `white_check_mark`                | 🚨 `rotating_light`                        |
| Body      | `Job: <job_name>\nRun ID: <first 8 chars>\nEnvironment: <ENVIRONMENT>` | same |

### Behaviour notes

- **Opt-in** — alerts are silently skipped when `NTFY_TOPIC` is not set; no
  error is raised.
- **Non-blocking** — a failed POST (network error, bad topic, etc.) is caught
  and logged as a warning; it never blocks the sensor tick or the next run.
- **All jobs covered** — both sensors monitor every registered Dagster job with
  no explicit `monitored_jobs` list, so new jobs are covered automatically.
- **HTTP headers are ASCII-only** — the job name in the `Title` header must not
  contain non-ASCII characters; the separator used is a plain hyphen (`-`), not
  an em dash, for HTTP header compatibility.

## Environment Variables

Defined in `.env` (see `.env.example`). Key groups:

| Variable                           | Purpose                              |
|------------------------------------|--------------------------------------|
| `STORAGE_TARGET`                   | `local` or `onelake` (Delta Lake export only) |
| `SILVER_STORAGE_FORMAT`            | `duckdb` (default) or `ducklake`     |
| `DUCKLAKE_CATALOG_LOCATION`        | Path to DuckLake catalog `.ducklake` file (ducklake mode) |
| `DUCKLAKE_DATA_PATH`               | Dir for DuckLake Parquet data (ducklake mode) |
| `LOCAL_STORAGE_PATH`               | Root for local file storage          |
| `DANISH_DEMOCRACY_DATA_SOURCE`     | Path to DDD Bronze files             |
| `DANISH_DEMOCRACY_BASE_URL`        | OData API base URL                   |
| `RFAM_CONNECTION_STRING`           | MySQL connection string              |
| `RFAM_DATA_SOURCE`                 | Path to Rfam Bronze files            |
| `DUCKDB_DATABASE_LOCATION`         | Path to `.duckdb` file               |
| `DUCKDB_DATABASE`                  | Main DuckDB database name (= file stem) |
| `DBT_PROJECT_DIRECTORY`            | Path to `dbt/` folder                |
| `DLT_PIPELINES_DIR`               | dlt incremental state directory      |
| `DAGSTER_HOME`                     | Dagster SQLite storage               |
| `ENVIRONMENT`                      | Deployment label included in ntfy.sh alerts (e.g. `PROD`, `DEV`) |
| `NTFY_TOPIC`                       | ntfy.sh topic name for run alerts (topic name only, no URL prefix); leave unset to disable |
| `AZURE_TENANT_ID/CLIENT_ID/SECRET` | Service principal (OneLake mode)    |
| `FABRIC_WORKSPACE`                 | Fabric workspace name                |
| `FABRIC_ONELAKE_FOLDER_*`         | OneLake Bronze/Silver/Gold paths     |

## Running the Project

### Local (no Docker)

```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -e ".[dagster,dev]"
cp .env.example .env   # fill in paths

# Generate dbt models (only needed if entity lists changed)
python -m ddd_python.ddd_dbt.generate_dbt_models

# Extract data
python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data
python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_rfam

# Transform
python -m ddd_python.ddd_dbt.dbt_build_with_unique_logfile

# Export (OneLake mode only)
python -m ddd_python.ddd_dlt.export_main_silver_to_fabric_silver
python -m ddd_python.ddd_dlt.export_main_gold_to_fabric_gold

# Dagster UI
dagster dev -w workspace.yaml   # http://localhost:3000
```

### Docker

```bash
docker compose build
docker compose run --rm run ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data
docker compose run --rm run ddd_python.ddd_dbt.generate_dbt_models
docker compose run --rm run ddd_python.ddd_dbt.dbt_build_with_unique_logfile
docker compose up dagster       # http://localhost:3000
```

### Tests

```bash
pytest tests/                                  # all tests
pytest tests/test_configuration_variables.py   # single module
pytest -v -k "incremental"                     # keyword filter
```

## Test Structure (`tests/`)

| File                              | Tests                                              |
|-----------------------------------|----------------------------------------------------|
| `conftest.py`                     | Fixtures (mock_fabric_clients)                     |
| `test_configuration_variables.py` | Entity list consistency (counts, subsets, no dupes) |
| `test_generate_dbt_models.py`     | Macro selection (incremental vs full-extract)       |
| `test_export_silver.py`           | Silver → Delta Lake export logic (DDD + Rfam PKs)   |
| `test_export_gold.py`             | Gold → Delta Lake export logic                      |
| `test_integration_bronze.py`      | Bronze layer: JSON read, filename extraction, _latest |
| `test_integration_silver_cdc.py`  | Silver CDC: I/U/D detection, _cv view, NOT EXISTS dedup |
| `test_integration_gold.py`        | Gold star-schema: SCD2, surrogate keys, fact joins  |
| `test_integration_e2e_pipeline.py`| End-to-end: Bronze→Silver→Delta Lake round-trip     |
| `test_json_default.py`            | JSON serialization edge cases                       |
| `test_require_env.py`             | Lazy environment variable loading                   |
| `test_scrub_secrets.py`           | Sensitive data masking                              |
| `test_serialize_trace.py`         | Request tracing serialization                       |
| `test_path_utils.py`              | Bronze destination + Delta export path construction (local vs OneLake) |
| `test_string_utils.py`            | Danish name normalization + incremental load-date resolution |

**Total: 132 tests across 15 modules** (unit + integration).

## DuckDB Initialization

`duckdb/init_duckdb.sql` installs extensions (httpfs, azure, delta), sets
`azure_transport_option_type = 'curl'`, and creates a persistent Azure
service-principal secret. The Docker entrypoint runs this automatically
when `STORAGE_TARGET=onelake`.

## Files That Are Git-Ignored

`/data/`, `.env`, `*.duckdb`, `dbt/target/`, `dbt/logs/`, `dlt/pipelines_dir/`,
`.dagster/`, `__pycache__/`, `.venv/`, `*.egg-info/`, `.pytest_cache/`

## Common Tasks for AI Assistants

- **Add a new DDD entity:** Update lists in `configuration_variables.py`, then
  run `python -m ddd_python.ddd_dbt.generate_dbt_models`.
- **Add a new Rfam table:** Update `RFAM_TABLE_NAMES`, `_PRIMARY_KEYS`,
  `_DATE_COLUMNS`, `_QUERIES` in `configuration_variables.py`, then regenerate.
- **Change CDC logic:** Edit the Silver macros in `dbt/macros/`. Keep all
  helper-table writes (`_last_file`, `_current_temp`) qualified with
  `{{ this.database }}` so DuckLake mode stays within one database per transaction.
- **Switch Silver storage:** Set `SILVER_STORAGE_FORMAT=duckdb|ducklake` in
  `.env`, then `--full-refresh` the Silver layer (`dbt build --select tag:silver
  --full-refresh`). Switching format does **not** migrate existing data — rebuild
  from Bronze. See *Silver Storage Format*.
- **Modify Gold star schema:** Edit SQL in `dbt/models/gold/` directly
  (some are generated, some handcrafted).
- **Run tests after changes:** `pytest tests/` — configuration tests will catch
  inconsistencies between entity lists.
