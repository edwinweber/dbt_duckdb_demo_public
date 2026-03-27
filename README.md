# Danish Democracy Data — dbt + DuckDB Demo

A demo project that ingests, transforms, and publishes open data from the
Danish Parliament (Folketing) OData API and the Rfam public MySQL database.

> **Forked from** [bgarcevic/danish-democracy-data](https://github.com/bgarcevic/danish-democracy-data),
> which provides the initial foundation for working with Folketing open data.
> This repository extends that foundation with a full medallion pipeline,
> SCD Type 2 history, Dagster orchestration, and Microsoft Fabric / OneLake export.

The goal is to demonstrate the capabilities achievable with a **low-cost,
open-source stack** (DuckDB + dbt + dlt + Dagster) before requiring
enterprise-grade commercial tooling. This is a learning and reference project
demonstrating production-quality data engineering patterns. The pipeline runs
daily against real data, and the codebase includes production-hardening measures
(SQL injection defense, connection safety, non-root Docker, API response
validation).

The pipeline follows a **medallion architecture** (Bronze → Silver → Gold),
orchestrated by Dagster. It supports two storage backends controlled by a
single environment variable:

| Mode | Storage | Requires |
| --- | --- | --- |
| `local` | `data/` directory in the repo (Docker volume) | Nothing — runs fully offline |
| `onelake` | Microsoft Fabric OneLake (ADLS Gen2 / Delta Lake) | Azure service principal |

---

## Architecture Overview

```text
  ┌──────────────────────────────────────────────────────────────────────┐
  │  Dagster  (schedule 06:00 UTC daily · disabled by default)           │
   │  └── full_pipeline_job                                              │
  └──────────────────────────────┬───────────────────────────────────────┘
                                 │ orchestrates
  ┌──────────────────────────────▼───────────────────────────────────────┐
  │  Layer 1 — Extraction  (dlt)                                        │
  │  ┌─ DDD: 18 OData entities from Danish Parliament API                │
  │  │  ├── Incremental (6): Aktør, Møde, Sag, Sagstrin, SagstrinAktør, │
  │  │  │                    Stemme                                       │
  │  │  └── Full-extract (12): small lookup tables                      │
  │  └─ RFAM: 7 MySQL tables from Rfam public database                  │
  │     ├── Incremental (2): family, genome                             │
  │     └── Full-extract (5): clan, clan_membership, author,            │
  │                          literature_reference, dead_family          │
  └─────────────────┬──────────────────────┬────────────────────┘
                    │                                │
       STORAGE_TARGET=local             STORAGE_TARGET=onelake
                    │                                │
    ┌───────────────▼──────────────┐  ┌──────────────▼──────────────────┐
    │  data/Files/Bronze/          │  │  <Lakehouse>/Files/Bronze/     │
    │  DDD/{entity}/*.json         │  │  DDD/{entity}/*.json           │
    │  RFAM/{table}/*.json         │  │  RFAM/{table}/*.json           │
    └───────────────┬──────────────┘  └──────────────┬──────────────────┘
                    └────────────────┬─────────────┘
                          DATA_SOURCE env vars
  ┌───────────────────────────────────▼──────────────────────────────────┐
  │  Layer 2 — Bronze  (dbt views · code-generated)                      │
  │  DuckDB read_json_auto(DATA_SOURCE/{entity}/*.json)                   │
  │  Works identically for local paths and abfss:// URLs                 │
  │  25 entities (18 DDD + 7 Rfam) · no transformations · raw preserved  │
  └───────────────────────────────────┬──────────────────────────────────┘
                                      │
  ┌───────────────────────────────────▼──────────────────────────────────┐
  │  Layer 3 — Silver  (dbt incremental tables · DuckDB)                 │
  │  Hash-based CDC → SCD Type 2 history per entity                      │
  │  Companion _cv (current-version) view per entity                     │
  └──────────────────────┬────────────────────────────┬───────────────────┘
                         │                            │
    ┌────────────────────▼──────────────┐  ┌──────────▼──────────────────┐
    │  Silver export  (Delta Lake)      │  │  Layer 4 — Gold (dbt views) │
    │  Incremental append               │  │  Star schema: actor, vote,  │
    │  local:   data/Files/Silver/      │  │  case, meeting + _cv views  │
    │  onelake: <Lh>/Files/Silver/      │  └──────────┬──────────────────┘
    └───────────────────────────────────┘             │
                                           ┌──────────▼──────────────────┐
                                           │  Gold export  (Delta Lake)  │
                                           │  Full overwrite every run   │
                                           │  local:   data/Files/Gold/  │
                                           │  onelake: <Lh>/Files/Gold/  │
                                           └─────────────────────────────┘
```

### Tech Stack

| Concern | Tool |
| --- | --- |
| Orchestration | Dagster (software-defined assets, schedules, sensors) |
| Extraction | dlt (Data Load Tool) — OData API + SQL database |
| Transformation | dbt-core + dbt-duckdb |
| Query engine / local storage | DuckDB |
| SQL source connector | SQLAlchemy + PyMySQL (Rfam MySQL) |
| Cloud storage (optional) | Microsoft Fabric OneLake (ADLS Gen2 / Delta Lake) |
| Data quality | dbt built-in tests + dbt-expectations |
| Language | Python 3.12+ |

---

## Running With Docker

The recommended way to run the pipeline is via Docker — no local Python setup required.

```bash
# Build the image
docker compose build

# Run the full pipeline end-to-end via Dagster
docker compose run --rm dagster job execute -j full_pipeline_job -w workspace.yaml

# Or start the Dagster UI at http://localhost:3000
docker compose up dagster
```

See [DOCKER_USAGE.md](DOCKER_USAGE.md) for the full Docker reference including
individual pipeline steps, volume management, and troubleshooting.

---

## Project Layout

```text
.
├── data/                       Local storage (git-ignored) — mirrors Fabric OneLake layout
│   └── Files/
│       ├── Bronze/
│       │   ├── DDD/            NDJSON files per DDD entity (written by dlt)
│       │   └── RFAM/           NDJSON files per Rfam table (written by dlt)
│       ├── Silver/             Delta Lake tables per Silver entity
│       └── Gold/               Delta Lake tables per Gold model
├── dbt/                        dbt project
│   ├── models/
│   │   ├── bronze/             Views over Bronze NDJSON (read_json_auto)
│   │   ├── silver/             SCD Type 2 incremental tables + _cv views
│   │   └── gold/               Star-schema views + _cv (current-version) views
│   ├── macros/                 Code-generation macros for Bronze, Silver & Gold
│   ├── seeds/                  Date dimension + source system lookup
│   └── packages.yml            dbt-utils + dbt-expectations
├── ddd_python/
│   ├── ddd_dagster/            Dagster definitions, assets, jobs, schedules, sensors
│   ├── ddd_dlt/                dlt pipeline runners + Delta Lake export functions
│   └── ddd_utils/              Shared config, env-var helpers, Azure clients
├── tests/                      pytest unit + integration tests
├── .dagster/                   Dagster home directory (set via DAGSTER_HOME)
│   └── dagster.yaml            SQLite run/event/schedule storage config
├── .env.example                Template — copy to .env and fill in values
├── workspace.yaml              Dagster workspace entry-point
├── Dockerfile                  Container image definition
├── docker-compose.yml          Service definitions (run, dagster)
├── DOCKER_USAGE.md             Docker usage reference
└── pyproject.toml              Project metadata and dependencies
```

---

## Walkthrough

### 1. Prerequisites

**All modes:**

- Python 3.12+
- The Danish Parliament OData API (`https://oda.ft.dk/api`) is public — no API key required
- The Rfam MySQL database (`mysql-rfam-public.ebi.ac.uk:4497`) is public read-only — no credentials required

**OneLake mode only (optional):**

- Access to a Microsoft Fabric workspace with OneLake enabled
- An Azure AD service principal with **Storage Blob Data Contributor** on the
  OneLake storage account
- Fabric capacity must be **active** when running extraction or export jobs

### 2. Clone and create a virtual environment

```bash
git clone https://github.com/edwinweber/dbt_duckdb_demo.git
cd dbt_duckdb_demo
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -e ".[dagster,dev]"
```

### 3. Configure environment variables

```bash
cp .env.example .env
# Edit .env — for local mode only a handful of variables are needed
```

#### Environment Variable Reference

| Variable | Required | Example / Description |
| --- | --- | --- |
| `STORAGE_TARGET` | All | `local` or `onelake` — selects the storage backend |
| `LOCAL_STORAGE_PATH` | All | `/home/you/dbt_duckdb_demo/data` — base path for Bronze / Silver / Gold files |
| `DANISH_DEMOCRACY_DATA_SOURCE` | All | Local: `<LOCAL_STORAGE_PATH>/Files/Bronze/DDD`; OneLake: `abfss://.../<lakehouse>.Lakehouse/Files/Bronze/DDD` |
| `DAGSTER_HOME` | All | `/home/you/dbt_duckdb_demo/.dagster` — Dagster run and schedule state |
| `DUCKDB_DATABASE_LOCATION` | All | `/home/you/dbt_duckdb_demo/duckdb/danish_democracy_data.duckdb` — DuckDB file path |
| `DUCKDB_DATABASE` | All | `danish_democracy_data` — DuckDB database name |
| `DBT_PROJECT_DIRECTORY` | All | `/home/you/dbt_duckdb_demo/dbt` — path to the `dbt/` folder |
| `DBT_MODELS_DIRECTORY` | All | `/home/you/dbt_duckdb_demo/dbt/models` — path to `dbt/models/` |
| `DLT_PIPELINES_DIR` | All | `/home/you/dbt_duckdb_demo/dlt/pipelines_dir` — dlt state directory |
| `DANISH_DEMOCRACY_BASE_URL` | All | `https://oda.ft.dk/api` — Parliament OData API root |
| `RFAM_CONNECTION_STRING` | All | `mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam` — Rfam MySQL connection |
| `RFAM_DATA_SOURCE` | All | Local: `<LOCAL_STORAGE_PATH>/Files/Bronze/RFAM`; OneLake: `abfss://.../<lakehouse>.Lakehouse/Files/Bronze/RFAM` |
| `RFAM_DEFAULT_DAYS_TO_LOAD` | All | Number of days to look back for incremental Rfam loads (default: `365`) |
| `FABRIC_WORKSPACE` | OneLake | Fabric workspace name |
| `FABRIC_ONELAKE_STORAGE_ACCOUNT` | OneLake | Usually `onelake` |
| `FABRIC_ONELAKE_FOLDER_BRONZE` | OneLake | `<Lakehouse>.Lakehouse/Files/Bronze` |
| `FABRIC_ONELAKE_FOLDER_SILVER` | OneLake | `<Lakehouse>.Lakehouse/Files/Silver` |
| `FABRIC_ONELAKE_FOLDER_GOLD` | OneLake | `<Lakehouse>.Lakehouse/Files/Gold` |
| `DLT_PIPELINE_RUN_LOG_DIR` | OneLake | OneLake path for pipeline run logs |
| `AZURE_TENANT_ID` | OneLake | Azure AD tenant ID |
| `AZURE_CLIENT_ID` | OneLake | Service principal client ID |
| `AZURE_CLIENT_SECRET` | OneLake | Service principal secret |

> **Security note:** `.env` is git-ignored. Never commit credentials.

### 4. Install dbt packages

```bash
cd dbt && dbt deps && cd ..
```

### 5. Generate dbt models

Bronze, Silver, and Gold model SQL files are **code-generated** from the entity
list in `ddd_python/ddd_utils/configuration_variables.py` via dbt macros.
Run this once before the first pipeline run, and again whenever you add or
rename an entity:

```bash
python -m ddd_python.ddd_dbt.generate_dbt_models
```

This writes `.sql` files into `dbt/models/bronze/`, `dbt/models/silver/`, and
`dbt/models/gold/`.

### 6. Load dbt seeds

Seeds are static CSV reference data (date dimension, source system lookup).
Load them into DuckDB once:

```bash
cd dbt && dbt seed --profiles-dir . && cd ..
```

Or trigger the `dbt_seeds_job` from the Dagster UI.

### 7. Start Dagster

```bash
export DAGSTER_HOME="$(pwd)/.dagster"
dagster dev -w workspace.yaml
```

Open **<http://localhost:3000>** to access the Dagster UI.

> `DAGSTER_HOME` can also be set permanently in `.env` — it is loaded by
> `python-dotenv` at startup.

---

## Running the Pipeline

### First Run — End-to-End

For a first-time **full load**, run in this order:

```bash
# 1. Extract all 18 DDD entities (full + incremental)
dagster job launch -w workspace.yaml --job danish_parliament_all_job

# 2. Extract all 7 Rfam tables
dagster job launch -w workspace.yaml --job rfam_all_job

# 3. Transform: Bronze → Silver → Gold
dagster job launch -w workspace.yaml --job dbt_silver_job
dagster job launch -w workspace.yaml --job dbt_gold_job

# 4. Export Silver and Gold as Delta Lake tables
dagster job launch -w workspace.yaml --job export_silver_job
dagster job launch -w workspace.yaml --job export_gold_job
```

Or run the complete pipeline in a single command:

```bash
dagster job launch -w workspace.yaml --job full_pipeline_job
```

### Daily Incremental Runs

The schedule `danish_parliament_full_pipeline_schedule` fires at **06:00 UTC**
daily. It is **disabled by default** — enable it in the Dagster UI under
**Automation → Schedules** when you're ready to run it regularly.

For a manual incremental run (the 6 date-filterable entities only):

```bash
dagster job launch -w workspace.yaml --job danish_parliament_incremental_job
```

### Full-Extract Refresh

The 12 reference entities are always fully extracted on every run. Although they
support `opdateringsdato` filtering, a full extract is preferred because the
tables are small and it simplifies delete detection.

```bash
dagster job launch -w workspace.yaml --job danish_parliament_full_extract_job
```

This also runs as part of `full_pipeline_job`.

### Individual Layers (CLI)

```bash
dagster job launch -w workspace.yaml --job dbt_bronze_job
dagster job launch -w workspace.yaml --job dbt_silver_job
dagster job launch -w workspace.yaml --job dbt_gold_job
dagster job launch -w workspace.yaml --job export_silver_job
dagster job launch -w workspace.yaml --job export_gold_job
```

---

## Dagster Job Organisation

Jobs are organised in a **modular, composable** hierarchy. Each layer has
per-source-system jobs that can be run independently. Parent jobs compose these
building blocks via `AssetSelection` unions — adding a new source system only
requires adding its leaf jobs and including them in the parent selections.

```text
full_pipeline_job
├── Extraction
│   ├── danish_parliament_all_job
│   │   ├── danish_parliament_incremental_job   (6 DDD entities)
│   │   └── danish_parliament_full_extract_job  (12 DDD entities)
│   └── rfam_all_job
│       ├── rfam_incremental_job                (2 Rfam tables)
│       └── rfam_full_extract_job               (5 Rfam tables)
├── dbt Bronze
│   └── dbt_bronze_job                          = dbt_bronze_ddd_job | dbt_bronze_rfam_job
│       ├── dbt_bronze_ddd_job                  (18 DDD bronze models + _latest views)
│       └── dbt_bronze_rfam_job                 (7 Rfam bronze models + _latest views)
├── dbt Silver
│   └── dbt_silver_job                          = dbt_silver_ddd_job | dbt_silver_rfam_job
│       ├── dbt_silver_ddd_job                  (18 DDD silver tables + _cv views)
│       └── dbt_silver_rfam_job                 (7 Rfam silver tables + _cv views)
├── dbt Gold
│   └── dbt_gold_job                            (10 Gold models — DDD only)
└── Export
    ├── export_silver_job                       (DuckDB Silver → OneLake Delta)
    └── export_gold_job                         (DuckDB Gold → OneLake Delta)
```

**Run a single source system** through Bronze and Silver without touching the other:

```bash
# DDD only
dagster job launch -w workspace.yaml --job dbt_bronze_ddd_job
dagster job launch -w workspace.yaml --job dbt_silver_ddd_job

# Rfam only
dagster job launch -w workspace.yaml --job dbt_bronze_rfam_job
dagster job launch -w workspace.yaml --job dbt_silver_rfam_job
```

**Run all source systems** (parent jobs compose the per-source-system selections):

```bash
dagster job launch -w workspace.yaml --job dbt_bronze_job   # DDD + Rfam bronze
dagster job launch -w workspace.yaml --job dbt_silver_job   # DDD + Rfam silver
```

### Job Summary

| Job | Scope | Executor |
| --- | --- | --- |
| `full_pipeline_job` | End-to-end: extract → Bronze → Silver → Gold → export | multiprocess (max 4) |
| `danish_parliament_incremental_job` | 6 DDD incremental entities | multiprocess (max 4) |
| `danish_parliament_full_extract_job` | 12 DDD full-extract entities | multiprocess (max 4) |
| `danish_parliament_all_job` | All 18 DDD entities | multiprocess (max 4) |
| `rfam_incremental_job` | 2 Rfam incremental tables | multiprocess (max 4) |
| `rfam_full_extract_job` | 5 Rfam full-extract tables | multiprocess (max 4) |
| `rfam_all_job` | All 7 Rfam tables | multiprocess (max 4) |
| `dbt_seeds_job` | Static CSV seeds | in-process |
| `dbt_bronze_job` | All Bronze models (DDD + Rfam) | in-process |
| `dbt_bronze_ddd_job` | DDD Bronze models only | in-process |
| `dbt_bronze_rfam_job` | Rfam Bronze models only | in-process |
| `dbt_silver_job` | All Silver models (DDD + Rfam) | in-process |
| `dbt_silver_ddd_job` | DDD Silver models only | in-process |
| `dbt_silver_rfam_job` | Rfam Silver models only | in-process |
| `dbt_gold_job` | All Gold models (DDD only) | in-process |
| `export_silver_job` | Silver → OneLake Delta Lake | multiprocess (max 4) |
| `export_gold_job` | Gold → OneLake Delta Lake | multiprocess (max 4) |

Extraction and export jobs use `multiprocess_executor` (I/O bound, safe to
parallelise). dbt jobs use `in_process_executor` due to DuckDB's single-writer
constraint. The model lists for per-source-system selections are driven from
`configuration_variables.py`, so adding a new entity automatically includes it
in the correct job.

---

## Local Storage Layout

When `STORAGE_TARGET=local`, all data lands under `LOCAL_STORAGE_PATH` in a
directory structure that intentionally mirrors the Fabric OneLake layout so
that paths are directly comparable:

```text
LOCAL_STORAGE_PATH/          (e.g. /home/you/dbt_duckdb_demo/data  or  /data/local in Docker)
└── Files/
    ├── Bronze/
    │   ├── DDD/
    │   │   ├── aktoer/          aktoer_YYYYMMDD_HHMMSS.json
    │   │   ├── aktoertype/      aktoertype_YYYYMMDD_HHMMSS.json
    │   │   ├── afstemning/      …
    │   │   └── … (18 DDD entities)
    │   └── RFAM/
    │       ├── family/          family_YYYYMMDD_HHMMSS.json
    │       ├── genome/          genome_YYYYMMDD_HHMMSS.json
    │       └── … (7 Rfam tables)
    ├── Silver/
    │   ├── silver_aktoer/       Delta Lake table (incremental append)
    │   ├── silver_aktoertype/   Delta Lake table
    │   ├── silver_rfam_family/  Delta Lake table
    │   └── … (25 Silver tables: 18 DDD + 7 Rfam)
    └── Gold/
        ├── actor/               Delta Lake table (full overwrite)
        ├── vote/
        └── … (Gold models)
```

Compare with OneLake (`STORAGE_TARGET=onelake`):

```text
<Workspace>/
└── <Lakehouse>.Lakehouse/Files/
    ├── Bronze/
    │   ├── DDD/{entity}/        — NDJSON files
    │   └── RFAM/{table}/        — NDJSON files
    ├── Silver/{table}/          — Delta Lake tables
    └── Gold/{table}/            — Delta Lake tables
```

The `DANISH_DEMOCRACY_DATA_SOURCE` and `RFAM_DATA_SOURCE` variables are what
dbt’s Bronze layer uses to locate the NDJSON files via DuckDB’s
`read_json_auto()`. Set them to either `abfss://` URLs (OneLake) or absolute
local paths — the Bronze models work identically in both cases.

---

## Data Model

### DDD Entities (18)

| Category | Entities |
| --- | --- |
| **Incremental** (date-filtered) | Aktør, Møde, Sag, Sagstrin, SagstrinAktør, Stemme |
| **Full-extract** (always fully fetched — small tables, easy delete detection) | Afstemning, Afstemningstype, Aktørtype, Mødestatus, Mødetype, Periode, Sagskategori, Sagsstatus, Sagstrinsstatus, Sagstrinstype, Sagstype, Stemmetype |

### Rfam Tables (7)

| Category | Tables | Primary Key |
| --- | --- | --- |
| **Incremental** (date-filtered) | family, genome | `rfam_acc`, `upid` |
| **Full-extract** | clan, clan_membership, author, literature_reference, dead_family | `clan_acc`, (`clan_acc`, `rfam_acc`), `author_id`, `pmid`, `rfam_acc` |

### Silver Layer — SCD Type 2

One incremental DuckDB table per entity. Each row carries standard `LKHS_`
lakehouse metadata columns:

| Column | Description |
| --- | --- |
| `LKHS_source_system_code` | `DDD` or `RFAM` — identifies the source system |
| `LKHS_date_valid_from` | Point-in-time when this version was first observed |
| `LKHS_hash_value` | SHA-256 of all business columns (64 hex chars) — used for change detection |
| `LKHS_cdc_operation` | `I` insert · `U` update · `D` delete |
| `LKHS_date_inserted` | Pipeline run timestamp (when dbt loaded this row) |

A companion `_cv` (current-version) view sits alongside each table and returns
the **latest row** per entity key using `ROW_NUMBER() OVER (PARTITION BY id ORDER BY LKHS_date_valid_from DESC)`.
Note: `_cv` views include rows with `LKHS_cdc_operation = 'D'` (source-deleted
records). Downstream consumers should filter `WHERE LKHS_cdc_operation != 'D'`
if deleted records should be excluded.

### Gold Layer — Star Schema

Clean English-named views built on top of Silver `_cv` views:

| Model | Description |
| --- | --- |
| `actor` | Politicians and organisations |
| `actor_type` | Actor category lookup |
| `case` | Parliamentary cases and bills |
| `meeting` | Plenary meeting sessions |
| `meeting_status` / `meeting_type` | Meeting dimension lookups |
| `vote` | Voting results per case |
| `vote_type` | Vote category lookup |
| `date` | Date dimension (from seed) |

Surrogate keys are generated using DuckDB's built-in `hash()` function (64-bit),
mapped from unsigned to signed `BIGINT` via the `cast_hash_to_bigint` macro for
Power BI compatibility. Each Gold table also has a `_cv` view.

---

## dbt Commands Reference

All dbt commands must be run from the `dbt/` directory:

```bash
cd dbt

dbt run --profiles-dir .                        # full run (all layers)
dbt run --select bronze --profiles-dir .        # Bronze only
dbt run --select silver --profiles-dir .        # Silver only
dbt run --select gold   --profiles-dir .        # Gold only

# Rebuild Silver from scratch — clears all CDC history
dbt run --select silver --full-refresh --profiles-dir .

dbt test --profiles-dir .                       # all data quality tests
dbt test --select silver --profiles-dir .       # Silver tests only

dbt docs generate --profiles-dir . && dbt docs serve   # lineage browser on :8080
```

---

## dbt Documentation

The project includes full dbt documentation with model descriptions, column
lineage, and a dependency graph. A pre-generated copy is committed at
[`documentation/dbt-docs/`](documentation/dbt-docs/) so you can browse it
without running the pipeline first.

**Browse the committed docs** — open `documentation/dbt-docs/index.html` in
your browser. The documentation covers:

- All 121 models across Bronze, Silver, and Gold layers
- Column-level lineage and data types via the catalog
- The full DAG (directed acyclic graph) showing dependencies between
  models, seeds, sources, and tests
- 264 data quality tests defined across the project

**Regenerate after changes:**

```bash
cd dbt
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .        # interactive site at http://localhost:8080
```

After regenerating, copy the updated files back into the repository:

```bash
cp dbt/target/{index.html,catalog.json,manifest.json} documentation/dbt-docs/
```

---

## Running Tests (pytest)

No cloud credentials required — tests use in-memory DuckDB and mocked clients.

```bash
pytest tests/ -v
```

| Test file | What it covers |
| --- | --- |
| `test_configuration_variables.py` | Entity list completeness and consistency |
| `test_export_gold.py` | Gold Delta Lake export — overwrite mode, row count, target path |
| `test_export_silver.py` | Silver Delta Lake export — incremental append, first-load overwrite |
| `test_generate_dbt_models.py` | dbt model code-generation macros |
| `test_integration_bronze.py` | Bronze layer: JSON read, filename extraction, `_latest` view |
| `test_integration_silver_cdc.py` | Silver CDC: insert/update/delete detection, `_cv` view, deduplication |
| `test_integration_gold.py` | Gold star-schema: SCD2, surrogate keys, fact joins |
| `test_integration_e2e_pipeline.py` | End-to-end: Bronze→Silver→Delta Lake round-trip |
| `test_serialize_trace.py` | dlt run trace serialisation |
| `test_scrub_secrets.py` | Credential scrubbing in log output |
| `test_require_env.py` | Missing env var handling |
| `test_json_default.py` | JSON serialisation of custom types |

---

## CDC / SCD Type 2 Design

Silver models implement hash-based Change Data Capture (CDC) across Bronze snapshot files:

1. Every Bronze file is read in full on each dbt run.
2. A SHA-256 hash of all business columns is computed per row per file (for change detection).
3. Rows are compared to the previous file via `LAG()`.
4. Only inserts (`I`) and updates (`U`) are appended to the incremental table.
5. Deletes (`D`) are detected during a `--full-refresh` by comparing the
   current-version view against the latest Bronze snapshot.
6. The `_cv` view returns the latest row per entity key (including deleted rows;
   filter `WHERE LKHS_cdc_operation != 'D'` to exclude them).

See [documentation/silver_model_logic.md](documentation/silver_model_logic.md)
for a detailed walkthrough with compiled SQL examples.

---

## Executor Concurrency Model

| Job type | Executor | Reason |
| --- | --- | --- |
| Extraction | `multiprocess_executor (max 4)` | I/O bound — concurrent HTTP + file writes safe |
| Export | `multiprocess_executor (max 4)` | I/O bound — concurrent Delta Lake writes safe |
| dbt | `in_process_executor` | DuckDB single-writer constraint |

---

## Dagster Home Directory

Dagster stores run history, event logs, and schedule state under `DAGSTER_HOME`.
This project uses `.dagster/` at the repository root, configured with SQLite
backends so that history survives server restarts. Only `dagster.yaml` is
committed; runtime artefacts (`storage/`, `logs/`, `.telemetry/`) are
git-ignored.

```bash
# Set before every session (or add to .env):
export DAGSTER_HOME="$(pwd)/.dagster"
```

---

## Known Limitations

- **DuckDB pinned below 1.5** (as of March 2026) — DuckDB 1.5.0 introduced an
  internal column-binding bug that crashes `GROUP BY` queries against views
  using `QUALIFY ROW_NUMBER()`. This breaks dbt's auto-generated `unique` tests
  on all Silver `_cv` views. The project is pinned to `duckdb>=1.1,<1.5` until
  the upstream fix is released. `fetch_arrow_table()` (deprecated in 1.5) is
  used accordingly. Track the upstream issue at
  [duckdb/duckdb#16407](https://github.com/duckdb/duckdb/issues/16407).

---

## Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `CapacityNotActive` in export / Bronze models | Fabric capacity is paused | Resume the capacity in the Azure portal and re-run |
| `Parser Error: syntax error at or near "DBT_..."` in Bronze | Stale dbt partial-parse cache | Run `dbt run --no-partial-parse` or delete `dbt/target/partial_parse.msgpack` |
| Asset shows as **unsynced** (yellow) in Dagster | Upstream materialized without running downstream | Materialize the downstream asset, or run the relevant job |
| `INTERNAL Error: Failed to bind column reference "id"` in Silver `_cv` tests | DuckDB >=1.5.0 QUALIFY + GROUP BY bug | Downgrade: `pip install "duckdb>=1.1,<1.5"` (pinned in `pyproject.toml`) |
| `write_deltalake() unexpected keyword argument` | `deltalake` version mismatch | `pip install -e ".[dagster,dev]"` to restore pinned versions |
| `FileNotFoundError` on DuckDB path | `DUCKDB_DATABASE_LOCATION` not set | Check `.env` and ensure the directory exists |
| Bronze models return no rows (local mode) | `DANISH_DEMOCRACY_DATA_SOURCE` or `RFAM_DATA_SOURCE` points to empty or wrong directory | Verify files exist under `LOCAL_STORAGE_PATH/Files/Bronze/DDD/{entity}/` or `.../RFAM/{table}/` |
| Bronze models return no rows (OneLake mode) | `DANISH_DEMOCRACY_DATA_SOURCE` or `RFAM_DATA_SOURCE` missing or wrong `abfss://` path | Set the correct path pointing to the Bronze NDJSON root on OneLake |
| dbt uses wrong output profile | `STORAGE_TARGET` mismatch | `dbt/profiles.yml` selects the `local` or `onelake` output based on `STORAGE_TARGET` — ensure `.env` is set correctly |
| Azure credential errors in local mode | `STORAGE_TARGET=onelake` set accidentally | Set `STORAGE_TARGET=local` in `.env`; no Azure vars are needed |
| dbt models missing (empty `models/` dir) | Model generation not run | Run `python -m ddd_python.ddd_dbt.generate_dbt_models` |

---

## Contributing

Contributions of all sizes are welcome — whether you spotted a typo, want to add
an entity, or have ideas for improving the pipeline design.

### How to Contribute

1. **Fork** the repository and create a branch from `main`:

   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Set up your environment** by following the [Walkthrough](#walkthrough)
   section. The `local` storage mode requires no cloud credentials.

3. **Make your changes.** Keep commits focused and descriptive.

4. **Run the tests** to make sure nothing is broken:

   ```bash
   pytest tests/ -v
   ```

5. **Open a pull request** against `main`. Include a short description of what
   you changed and why.

### Guidelines

- For **bug fixes and small improvements**, open a PR directly.
- For **larger changes** (new pipeline stages, schema changes, new dependencies),
  please open an issue first so we can discuss the approach before you invest
  time in the implementation.
- Keep changes scoped — one feature or fix per PR makes review easier.
- Do not commit credentials or `.env` files (the repo's `.gitignore` already
  excludes them, but double-check before pushing).

### Reporting Issues

Use the [GitHub Issues](https://github.com/edwinweber/dbt_duckdb_demo/issues)
tab to report bugs or suggest features. Please include:

- What you expected to happen
- What actually happened (error message, traceback)
- Your OS, Python version, and `STORAGE_TARGET` setting

---

## Glossary

| Abbreviation | Definition |
| --- | --- |
| **ADLS** | Azure Data Lake Storage (Gen2) |
| **CDC** | Change Data Capture — detecting inserts, updates, and deletes between data snapshots |
| **SCD Type 2** | Slowly Changing Dimension Type 2 — preserving full history by adding new rows for each change |
| **NDJSON** | Newline-Delimited JSON — one JSON object per line |
| **OData** | Open Data Protocol — a REST-based data access standard |

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
