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
- A learning/reference project demonstrating production-quality data engineering patterns.

## Tech Stack

| Layer           | Technology                                           |
|-----------------|------------------------------------------------------|
| Language        | Python ≥3.12                                         |
| Orchestration   | Dagster ≥1.12 (software-defined assets)              |
| Extraction      | dlt ≥1.24 (Data Load Tool)                           |
| Transformation  | dbt-core ≥1.10,<1.12 + dbt-duckdb ≥1.10             |
| Query engine    | DuckDB ≥1.5.1,<1.6                                   |
| Data quality    | dbt-utils 1.3.0, dbt-expectations 0.10.4             |
| Cloud storage   | Microsoft Fabric OneLake (ADLS Gen2 / Delta Lake)    |
| Export          | deltalake ≥1.5, PyArrow ≥17                          |
| SQL source      | SQLAlchemy ≥2.0, PyMySQL ≥1.1                        |
| Container       | Docker + Docker Compose                               |
| Testing         | pytest ≥8.0                                          |

## Architecture

```text
Extraction (dlt)
   ↓  JSON / Parquet files
Bronze (DuckDB views over raw files)
   ↓  hash-based CDC, SCD Type 2
Silver (DuckDB incremental tables + _cv current-version views)
   ↓  star-schema modeling
Gold (DuckDB views — facts & dimensions)
   ↓  Delta Lake export
OneLake / local filesystem
```

Orchestrated by **Dagster** (daily 06:00 UTC schedule, disabled by default).

## Directory Structure

```text
├── ddd_python/                  Python package (the code)
│   ├── ddd_dagster/             Dagster assets, jobs, schedules, sensors, resources
│   ├── ddd_dlt/                 dlt extraction pipelines + Delta Lake export
│   ├── ddd_dbt/                 dbt model generator + dbt runner + DuckDB init
│   └── ddd_utils/               Configuration, env vars, Azure/Fabric clients
├── dbt/                         dbt project
│   ├── models/bronze/           ~48 views (read_json_auto over raw files)
│   ├── models/silver/           ~56 models (CDC tables + _cv views)
│   ├── models/gold/             ~20 models (star-schema views)
│   ├── macros/                  9 Jinja macros (model factories, hash, CDC)
│   ├── seeds/                   Seed CSVs (date dimension, source registry)
│   └── dbt_project.yml          Project config + variables
├── tests/                       pytest tests (12 modules, 92 tests)
├── duckdb/                      DuckDB init scripts (extensions + Azure secret)
├── dlt/pipelines_dir/           dlt incremental state (git-ignored)
├── data/                        Local storage root (git-ignored)
│   └── Files/{Bronze,Silver,Gold}/
├── documentation/               Handbook markdown + build scripts
├── docker-compose.yml           Services: 'run' (one-off) + 'dagster' (UI)
├── Dockerfile                   Python 3.12 + DuckDB CLI v1.5.1
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

### `ddd_utils/get_variables_from_env.py` — Lazy environment loading

Uses `__getattr__` so that importing the module for code generation or testing
does **not** fail when Azure credentials are absent. Required vars raise only
on first access.

### `ddd_dagster/` — Dagster orchestration

- **assets.py** — DDD extraction (18× asset factory with retry policy)
- **rfam_assets.py** — Rfam extraction (7× asset factory)
- **dbt_assets.py** — dagster-dbt integration for Bronze/Silver/Gold
- **export_assets.py** — Silver (incremental) + Gold (full overwrite) → Delta Lake
- **jobs.py** — Pipelines (incremental, full-extract, all, full-pipeline)
  - dbt jobs: `in_process_executor` (DuckDB single-writer constraint)
  - Extraction/export: `multiprocess_executor` (max_concurrent=4)
- **schedules.py** — Daily 06:00 UTC (disabled by default)

### `ddd_dlt/` — Extraction & export

- **dlt_pipeline_execution_functions.py** — `api_to_file()`, `sql_to_file()`,
  `file_to_file()` — core extraction engine
- **dlt_run_extraction_pipelines_danish_parliament_data.py** — DDD orchestrator
  (CLI: `--date_to_load_from`, `--file_names`; ThreadPoolExecutor max_workers=4)
- **dlt_run_extraction_pipelines_rfam.py** — Rfam orchestrator
- **export_main_silver_to_fabric_silver.py** — Silver → Delta Lake (incremental)
- **export_main_gold_to_fabric_gold.py** — Gold → Delta Lake (full overwrite)

### `ddd_dbt/` — dbt tooling

- **generate_dbt_models.py** — Reads config lists → generates Bronze/Silver/Gold SQL files
  by instantiating Jinja macros. Incremental vs full-extract macro selection
  derived from `DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL` at runtime.
- **dbt_build_with_unique_logfile.py** — Runs `dbt build`, captures JSON log,
  optionally uploads to OneLake.
- **init_duckdb.py** — Database initialization (runs `init_duckdb.sql`).

## dbt Project (`dbt/`)

### Configuration

- **Profile:** `danish_democracy_data` with two targets:
  - `local` — DuckDB on local disk (extensions: httpfs, parquet, delta)
  - `onelake` — DuckDB with Azure secret (extensions: + azure)
- **Switched by:** `STORAGE_TARGET` env var (default `local`)
- **Schemas:** `bronze`, `silver`, `gold`
- **Materialization:** Bronze=view, Silver=table (incremental), Gold=view

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
7. `generate_pre_hook_silver.sql` — Pre-hook: temp table for delete detection
8. `generate_pre_hook_silver_full_refresh.sql` — Pre-hook for full refresh
9. `generate_post_hook_silver.sql` — Post-hook: drop temp table

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
- **Gold:** 18 models (10 star-schema views + `_cv` views + handcrafted)

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
6. **Dual-mode storage** — `STORAGE_TARGET=local|onelake` switches everything
7. **Concurrent extraction** — ThreadPoolExecutor(max_workers=4) for I/O
8. **Single-writer dbt** — `in_process_executor` for DuckDB constraint
9. **Generated SQL** — Bronze/Silver models auto-generated from config; Gold
   mostly generated, except `individual_votes.sql` (handcrafted)

## Production Hardening

The following measures are in place to make the pipeline production-quality:

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

## Environment Variables

Defined in `.env` (see `.env.example`). Key groups:

| Variable                           | Purpose                              |
|------------------------------------|--------------------------------------|
| `STORAGE_TARGET`                   | `local` or `onelake`                 |
| `LOCAL_STORAGE_PATH`               | Root for local file storage          |
| `DANISH_DEMOCRACY_DATA_SOURCE`     | Path to DDD Bronze files             |
| `DANISH_DEMOCRACY_BASE_URL`        | OData API base URL                   |
| `RFAM_CONNECTION_STRING`           | MySQL connection string              |
| `RFAM_DATA_SOURCE`                 | Path to Rfam Bronze files            |
| `DUCKDB_DATABASE_LOCATION`         | Path to `.duckdb` file               |
| `DBT_PROJECT_DIRECTORY`            | Path to `dbt/` folder                |
| `DLT_PIPELINES_DIR`               | dlt incremental state directory      |
| `DAGSTER_HOME`                     | Dagster SQLite storage               |
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

**Total: 92 tests across 12 modules** (unit + integration).

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
- **Change CDC logic:** Edit the Silver macros in `dbt/macros/`.
- **Modify Gold star schema:** Edit SQL in `dbt/models/gold/` directly
  (some are generated, some handcrafted).
- **Run tests after changes:** `pytest tests/` — configuration tests will catch
  inconsistencies between entity lists.
