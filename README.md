# End-to-end Open Source data engineering project using dbt, DuckDB, dlt, Dagster and Metabase

A reference data pipeline that ingests, transforms, and publishes open data from
the Danish Parliament (Folketing) OData API and the Rfam public MySQL database.

![Reference architecture: source systems feed dlt extraction, which lands Bronze files; dbt then builds Bronze views, Silver historized tables, and Gold dimensional models in DuckDB, with a Delta Lake export to Microsoft Fabric OneLake and Power BI / Metabase consumption.](documentation/assets/architecture-overview.svg)

> **Forked from** [bgarcevic/danish-democracy-data](https://github.com/bgarcevic/danish-democracy-data),
> which provides the initial foundation for working with Folketing open data.
> This repository extends that foundation with a full medallion pipeline,
> SCD Type 2 history, Dagster orchestration, and Microsoft Fabric / OneLake export.

It explores how far a **low-cost, open-source stack** (DuckDB + dbt + dlt +
Dagster) can go before reaching for commercial tooling, and serves as a worked
example of common data engineering patterns: a medallion architecture, hash-based
CDC with SCD Type 2 history, models code-generated from a single source of truth,
and a layer that lets the pipeline observe its own runs.

It runs daily against real data on a single small server — see
[Deployment](#deployment) for the setup and its trade-offs. The code applies a
number of defensive practices (input validation on interpolated SQL, connection
timeouts and cleanup, a non-root container, secret scrubbing in logs), while
remaining a deliberately simple single-node design rather than a
high-availability platform.

## What I built on top of the fork

The fork supplied a starting point for talking to the Folketing OData API. The
data-engineering platform around it is my work:

- **Medallion pipeline (Bronze → Silver → Gold)** across two sources — 18 Danish
  Parliament OData entities and 7 Rfam MySQL tables — with the dbt models
  code-generated from a single source-of-truth entity list.
- **Hash-based CDC with SCD Type 2 history** in Silver: SHA-256 change detection,
  full row history, and `_cv` current-version views. This is the core of the
  project — see the
  [deep-dive with compiled SQL](documentation/silver_model_logic.md).
- **Dagster orchestration** — software-defined assets, jobs, schedules,
  run-status sensors, and a **ntfy.sh push-notification** layer so failures
  (and successes) reach you immediately, plus an observability layer that lets
  the pipeline report on its own runs.
- **Dual-backend export** — one environment variable (`STORAGE_TARGET`) swaps
  the Delta Lake export between local filesystem and Microsoft Fabric OneLake,
  with paths deliberately mirrored.
- **Switchable Silver storage** — a second, independent variable
  (`SILVER_STORAGE_FORMAT=duckdb|ducklake`) stores the Silver layer either as
  native DuckDB tables or as [DuckLake](https://ducklake.select)-managed Parquet
  files (open table format, local catalog). See
  [CLAUDE.md → Silver Storage Format](CLAUDE.md#silver-storage-format-duckdb-vs-ducklake).
- **Operational hardening** — input validation on interpolated SQL, connection
  timeouts and cleanup, a non-root container, secret scrubbing in logs, and a
  pytest suite.

## How it works

The pipeline follows a **medallion architecture** (Bronze → Silver → Gold),
orchestrated by Dagster. It supports two storage backends controlled by a
single environment variable:

| Mode | Storage | Requires |
| --- | --- | --- |
| `local` | `data/` directory in the repo (Docker volume) | Nothing — runs fully offline |
| `onelake` | Microsoft Fabric OneLake (ADLS Gen2 / Delta Lake) | Azure service principal |

---

## Documentation Map

This README is the entry point. Deeper topics live in focused guides:

| Topic | Document |
| --- | --- |
| One-page summary (audience: non-specialists, hiring managers) | [documentation/management-summary.md](documentation/management-summary.md) |
| Docker usage reference | [DOCKER_USAGE.md](DOCKER_USAGE.md) |
| Silver CDC / SCD Type 2 logic, with compiled SQL | [documentation/silver_model_logic.md](documentation/silver_model_logic.md) |
| dbt macro reference | [documentation/dbt_macros.md](documentation/dbt_macros.md) |
| Test strategy — what, how, why, and scope | [documentation/testing.md](documentation/testing.md) |
| Dependency choices and rationale | [documentation/python_libraries.md](documentation/python_libraries.md) |
| Production server, volumes, SSH keys, firewall | [documentation/hetzner_infrastructure.md](documentation/hetzner_infrastructure.md) |
| Generated dbt model catalogue + lineage (browsable) | [documentation/dbt-docs/](documentation/dbt-docs/) |

---

## Architecture Overview

```text
  ┌──────────────────────────────────────────────────────────────────────┐
  │  Dagster  (schedule 06:00 Europe/Copenhagen daily · disabled by default) │
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
| Data quality | dbt built-in tests + dbt-utils |
| Data visualization | Metabase (connects to DuckDB directly) |
| Push notifications | ntfy.sh (run success / failure alerts) |
| Language | Python 3.12+ |

---

## Running With Docker

The recommended way to run the pipeline is via Docker — no local Python setup required.

```bash
# Build both images (pipeline + Metabase)
docker compose build

# Run the full pipeline end-to-end via Dagster
docker compose run --rm dagster job execute -j full_pipeline_job -w workspace.yaml

# Start the Dagster UI at http://localhost:3000
docker compose up dagster

# Start Metabase at http://localhost:3001 (independently or alongside Dagster)
docker compose up metabase
```

The four Docker services are:

| Service | Port | Purpose |
| --- | --- | --- |
| `run` | — | One-off Python module runner (`docker compose run --rm run <module>`) |
| `dagster` | 3000 | Dagster webserver + daemon (mounts `/var/run/docker.sock` to control Metabase) |
| `metabase` | 3001 | Metabase BI — connects directly to the DuckDB file |
| `backup` | — | One-off backup of Dagster and Metabase state (`docker compose run --rm backup`) |

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
│   ├── seeds/                  Danish public holidays + source system lookup
│   └── packages.yml            dbt-utils
├── ddd_python/
│   ├── ddd_dagster/            Dagster definitions, assets, jobs, schedules, sensors
│   ├── ddd_dlt/                dlt pipeline runners + Delta Lake export functions
│   └── ddd_utils/              Shared config, env-var helpers, Azure clients
├── tests/                      pytest unit + integration tests
├── .dagster/                   Dagster home directory (set via DAGSTER_HOME)
│   └── dagster.yaml            SQLite run/event/schedule storage config
├── .env.example                Template — copy to .env and fill in values
├── workspace.yaml              Dagster workspace entry-point
├── Dockerfile                  Container image for the pipeline (Python 3.12 + DuckDB CLI)
├── Dockerfile.metabase         Container image for Metabase with DuckDB driver
├── docker-compose.yml          Service definitions (run, dagster, metabase, backup)
├── start_metabase_and_wait.sh  Starts the Metabase container and waits 120 s for it to initialize
├── stop_metabase_and_wait.sh   Stops the Metabase container and waits 120 s for locks to clear
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
| `STORAGE_TARGET` | All | `local` or `onelake` — selects the **Delta Lake export** backend |
| `SILVER_STORAGE_FORMAT` | All | `duckdb` (default) or `ducklake` — how Silver tables are stored (independent of `STORAGE_TARGET`) |
| `DUCKLAKE_CATALOG_LOCATION` | ducklake | `/data/duckdb/ducklake_catalog.ducklake` — DuckLake catalog file |
| `DUCKLAKE_DATA_PATH` | ducklake | `/data/ducklake` — directory for DuckLake Parquet data |
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
| `NTFY_TOPIC` | Optional | ntfy.sh topic name for run alerts — topic name only, no URL prefix (e.g. `my-alerts`). Leave unset to disable. |
| `ENVIRONMENT` | Optional | Label shown in ntfy.sh alert messages, e.g. `PROD` or `DEV`. Also routes StorageBox backups to the right subdirectory. |
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

Seeds are static CSV reference data (Danish public holidays, source system
lookup). The date dimension itself is **generated** at build time by
`bronze_dates.sql` (via `dbt_utils.date_spine`) and joined to the holiday seed
in `gold/date.sql` — it is not a seed. Load the seeds into DuckDB once:

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

Two schedules run daily (both disabled by default — enable under **Automation → Schedules**):

| Schedule | Time (Europe/Copenhagen) | Job |
| --- | --- | --- |
| `danish_parliament_full_pipeline_schedule` | 06:00 | `full_pipeline_job` — extraction → Bronze → Silver → Gold → export → data engineering |
| `dbt_data_engineering_schedule` | 08:00 | `dbt_data_engineering_job` — Dagster observability layer refresh |

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
├── Data Engineering
│   └── dbt_data_engineering_job                 (8 observability models)
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
| `full_pipeline_job` | End-to-end: extract → Bronze → Silver → Gold → export → data engineering | in-process |
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
| `dbt_data_engineering_job` | Dagster observability layer | in-process |
| `export_silver_job` | Silver → OneLake Delta Lake | multiprocess (max 4) |
| `export_gold_job` | Gold → OneLake Delta Lake | multiprocess (max 4) |

Extraction and export jobs use `multiprocess_executor` (I/O bound, safe to
parallelise). dbt jobs use `in_process_executor` due to DuckDB's single-writer
constraint. The model lists for per-source-system selections are driven from
`configuration_variables.py`, so adding a new entity automatically includes it
in the correct job.

Every job automatically stops Metabase before any work begins and restarts it
after the last asset completes (including on failure). This is implemented via
`stop_metabase_asset` and `start_metabase_asset` — two Dagster assets added to
every job selection by the `_with_metabase_control()` helper in `jobs.py`.

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
| **Full-extract** | clan, clan_membership, author, literature_reference, dead_family | `clan_acc`, `rfam_acc`, `author_id`, `pmid`, `rfam_acc` |

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
| `individual_votes` | One row per MP per vote — the fact table |
| `meeting` | Plenary meeting sessions |
| `meeting_status` / `meeting_type` | Meeting dimension lookups |
| `vote` | Voting results per case |
| `vote_type` | Vote category lookup |
| `date` | Date dimension — generated via `dbt_utils.date_spine`, enriched with public-holiday flags from the `publicholiday_dk` seed |
| `time` | Time-of-day dimension |

Surrogate keys are generated using DuckDB's built-in `hash()` function (64-bit),
mapped from unsigned to signed `BIGINT` via the `cast_hash_to_bigint` macro for
Power BI compatibility. Most Gold dimension tables also have a `_cv` (current-version) view.

---

## Data Visualization — Metabase

Metabase is included as a BI layer that connects directly to the DuckDB file,
allowing you to build dashboards and explore the Gold layer without any
additional export step.

### Setup

1. Start the Metabase container:

   ```bash
   docker compose up metabase
   ```

2. Open **<http://localhost:3001>** and complete the first-time setup wizard.

3. When prompted to add a database, choose **DuckDB** and set the database path to:

   ```text
   /data/duckdb/danish_democracy_data.duckdb
   ```

4. In the DuckDB connection init script field, enter:

   ```sql
   INSTALL icu; LOAD icu;
   INSTALL httpfs; LOAD httpfs;
   INSTALL delta; LOAD delta;
   INSTALL sqlite; LOAD sqlite;
   ```

After connecting, Metabase can query all schemas: `bronze`, `silver`, `gold`,
and `data_engineering`. The Gold layer (`actor`, `vote`, `case`, `meeting`, etc.)
is the most natural starting point for dashboards.

### Metabase Lifecycle During Pipeline Runs

DuckDB enforces a **single-writer** constraint: only one process may hold a
read-write connection at a time. Because Metabase keeps an open connection to
the DuckDB file, it must be stopped before a pipeline run writes new data and
restarted afterward.

This is handled automatically by two Dagster assets that bookend every job:

| Asset | Behaviour |
| --- | --- |
| `stop_metabase_asset` | Stops the `ddd-metabase` Docker container and waits 120 s for existing connections and WAL locks to clear. Runs as the **first** asset in every job. |
| `start_metabase_asset` | Starts the `ddd-metabase` container and waits 120 s for Metabase to initialize. Runs as the **last** asset in every job — including on failure, so Metabase is always brought back up. |

The underlying shell scripts (`stop_metabase_and_wait.sh` /
`start_metabase_and_wait.sh`) fall back to `sudo docker` automatically if the
current user does not have direct Docker socket access. The Dagster container
mounts `/var/run/docker.sock` and is added to the socket's group via
`DOCKER_GID` in `.env` so the non-root `app` user can issue Docker commands.

The `_with_metabase_control()` helper in `jobs.py` wraps every job's
`AssetSelection` with these two assets, so all 18 jobs benefit from the
lifecycle management without any per-job boilerplate.

---

## Dagster Observability Layer

One of the distinguishing features of this project is that **the pipeline observes itself**.
A dedicated dbt layer (`data_engineering` schema) reads directly from Dagster's own
SQLite databases — the same databases the Dagster UI reads — and materialises the results
into DuckDB. This means every run leaves behind a queryable, structured record of what
happened, how long it took, and whether it succeeded.

### How It Works

Dagster persists its event log and run metadata in SQLite files under `DAGSTER_HOME`:

| SQLite file | Contains |
| --- | --- |
| `$DAGSTER_HOME/history/runs/index.db` | Consolidated event log — all `ASSET_MATERIALIZATION`, `STEP_START`, `STEP_SUCCESS`, `STEP_FAILURE`, and related events across all runs |
| `$DAGSTER_HOME/history/runs/{run_id}.db` | Per-run event log — individual files, one per pipeline run |
| `$DAGSTER_HOME/history/runs.db` | Run metadata — status, job name, start/end times, duration |

DuckDB's `sqlite_scan()` function reads these files directly, so no ETL
step is needed. The dbt models query them as if they were native DuckDB tables.

### Models

| Model | Materialization | Description |
| --- | --- | --- |
| `dagster_pipeline_runs` | view | One row per Dagster run — status, job name, start/end times, duration. Reads from `runs.db`. |
| `dagster_event_logs` | view | One row per `ASSET_MATERIALIZATION` event. Flattens nested JSON metadata (records written, rows written) via a `generate_series` lateral join. Staging model used by `dagster_run` and `dagster_asset_materialization`. |
| `dagster_job` | view | Dimension — one row per unique Dagster job name ever observed. Surrogate key consistent with the Gold layer pattern. |
| `dagster_asset` | view | Dimension — one row per unique asset key ever materialized. Parses the raw asset key JSON path into `asset_key_group` / `asset_key_layer` / `asset_key_name` path segments. |
| `dagster_run` | view | One row per run with full context: surrogate keys, job/date/time foreign keys, all timestamps, duration, and aggregated measures (`rows_processed`, `assets_materialized`). The main run-level fact. |
| `dagster_asset_materialization` | **table** | One row per `ASSET_MATERIALIZATION` event with step-level timing (step start → materialization), surrogate keys, date/time FKs, row counts, and step outcome (`STEP_SUCCESS` / `STEP_FAILURE`). Materialised as a table so downstream tools (DBeaver, Power BI) can query it without access to the SQLite file path. |
| `dagster_step_failures_raw` | **table** (Python model) | One row per failed asset per run. Reads `STEP_FAILURE` events from individual per-run SQLite files (not present in the consolidated index), then expands each failure to one row per planned asset via `ASSET_MATERIALIZATION_PLANNED` events. Implemented as a dbt Python model using PyArrow. |
| `dagster_step_failure` | view | Enriched version of `dagster_step_failures_raw` — adds surrogate keys, date/time FKs, and job name by joining to `dagster_pipeline_runs`. |

### What You Can Answer

Once `dbt_data_engineering_job` has run, you can query the DuckDB `data_engineering`
schema to answer questions like:

- **How long did each asset take to materialize in yesterday's run?**
  → join `dagster_asset_materialization` on `run_id` and order by `duration_seconds`
- **Which assets fail most often?**
  → group `dagster_step_failure` by `asset_key`
- **How many rows did the Silver layer process over the last 30 runs?**
  → filter `dagster_run` by `start_date_sk` and sum `rows_processed`
- **Did any run finish without materializing all expected assets?**
  → compare `assets_materialized` in `dagster_run` against the expected count

### Scheduling

The observability layer has its own daily schedule (`dbt_data_engineering_schedule`)
that fires at **08:00 Europe/Copenhagen** — after the 06:00 full pipeline has had
time to complete. It runs `dbt_data_engineering_job` independently, with no
dependency on the Gold export step. When triggered as part of `full_pipeline_job`
(the end-to-end pipeline), the layer runs after all Gold exports have finished,
so it captures the complete picture of that run.

Both schedules default to **STOPPED** — enable them in the Dagster UI under
**Automation → Schedules**.

```bash
# Run the observability layer manually
dagster job launch -w workspace.yaml --job dbt_data_engineering_job
```

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
your browser. It covers:

- Every model across the Bronze, Silver, Gold, and Data Engineering layers
- Column-level lineage and data types via the catalog
- The full DAG (directed acyclic graph) showing dependencies between
  models, seeds, sources, and tests
- The data-quality tests defined across the project

The committed copy is a point-in-time snapshot and is the authoritative count of
models and tests; regenerate it (below) after changing models so it stays in
step with the project.

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
The suite has **133 tests across 15 modules**. For the full strategy — what is
tested, how, why, and what is intentionally out of scope (plus the dbt
data-quality test layer) — see [documentation/testing.md](documentation/testing.md).

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
| `test_path_utils.py` | Bronze destination + Delta export path construction (local vs OneLake) |
| `test_string_utils.py` | Danish name normalisation + incremental load-date resolution |

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
| Metabase lifecycle | n/a — asset bookends | DuckDB single-writer: Metabase stopped before every job, restarted after |

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

## Backup and Restore

The platform includes a built-in backup system for its stateful data:
**Dagster** (run history, event logs, schedule state), **Metabase** (dashboards,
questions, user configuration), the **DuckDB** database directory (the main
`.duckdb` file plus the DuckLake catalog), and — when `SILVER_STORAGE_FORMAT=ducklake`
— the **DuckLake** Parquet data files. Each backup produces one timestamped zip
archive per target, stored locally and optionally uploaded to a Hetzner StorageBox
for off-site retention.

Backups run inside the existing `backup` Docker Compose service — no Python or
additional tooling is needed on the host, only Docker.

### What Gets Backed Up

| Target | Container name | Source directory | Local backup directory |
| --- | --- | --- | --- |
| `dagster` | `ddd-dagster` | `/data/dagster` | `/data_backup/dagster/` |
| `metabase` | `ddd-metabase` | `/data/metabase/data` | `/data_backup/metabase/` |
| `ducklake` | `ddd-dagster`, `ddd-metabase` | `/data/ducklake` | `/data_backup/ducklake/` |
| `duckdb` | `ddd-dagster`, `ddd-metabase` | `/data/duckdb` | `/data_backup/duckdb/` |

The `ducklake` target is only included when `SILVER_STORAGE_FORMAT=ducklake`; in
that mode it is archived **before** the `duckdb` target, because the DuckLake
catalog (captured in `/data/duckdb`) references the DuckLake data files, so the
files must be captured first. The DuckLake catalog `.ducklake` file lives in
`/data/duckdb`, so it always rides along in the `duckdb` target.

Archives are named `{target}_{YYYYMMDD_HHMMSS}.zip` — for example
`dagster_20260526_020000.zip`. Each backup run also appends one NDJSON record
per target to `/data_backup/logs/backup_log_{timestamp}.ndjson`.

### Environment Variables

| Variable | Required | Description |
| --- | --- | --- |
| `DAGSTER_HOME` | All | Dagster data directory (backup source) — default `/data/dagster` |
| `METABASE_DATA_DIR` | All | Metabase data directory (backup source) — default `/data/metabase/data` |
| `DUCKDB_DATABASE_LOCATION` | All | Path to the `.duckdb` file; its parent directory is the `duckdb` backup source |
| `DUCKLAKE_DATA_PATH` | ducklake mode | DuckLake Parquet data directory (`ducklake` backup source) — default `/data/ducklake` |
| `DAGSTER_BACKUP_MAX_AGE_DAYS` | All | Local retention for `dagster` archives — default `62` |
| `METABASE_BACKUP_MAX_AGE_DAYS` | All | Local retention for `metabase` archives — default `62` |
| `DUCKDB_BACKUP_MAX_AGE_DAYS` | All | Local retention for `duckdb` archives — default `7` |
| `DUCKLAKE_BACKUP_MAX_AGE_DAYS` | ducklake mode | Local retention for `ducklake` archives — default `7` |
| `ENVIRONMENT` | All | `DEV` or `PROD` — routes StorageBox uploads to the matching subdirectory |
| `HETZNER_STORAGEBOX_HOST` | StorageBox | StorageBox hostname (upload skipped when absent) |
| `HETZNER_STORAGEBOX_USER` | StorageBox | SSH user |
| `HETZNER_STORAGEBOX_PORT` | StorageBox | SSH port — Hetzner always uses `23` |
| `HETZNER_STORAGEBOX_REMOTE_DIR` | StorageBox | Base remote path; archives go to `<base>/<env>/` |
| `HETZNER_STORAGEBOX_SSH_KEY` | StorageBox | Path to SSH private key inside the container (e.g. `/home/app/.ssh/id_ed25519`); falls back to the default key when absent. Set `HOST_SSH_DIR` in `.env` to the host directory containing the key — it is mounted at `/home/app/.ssh` |

### Running a Backup

```bash
# Back up all targets
docker compose run --rm backup

# Back up a single target
docker compose run --rm backup python -m ddd_python.ddd_utils.backup_platform --targets dagster
docker compose run --rm backup python -m ddd_python.ddd_utils.backup_platform --targets duckdb
```

The backup:

1. Checks which of the relevant containers (`ddd-dagster`, `ddd-metabase`) are running.
2. Stops only the running ones gracefully (`docker stop`, waits for exit).
3. Waits 30 seconds for databases to flush WAL to disk (`FLUSH_WAIT_SECONDS` env var overrides this; set to `0` for local/dev runs).
4. Creates a deflate-compressed zip archive and verifies every entry for CRC errors.
5. Uploads the archive to the Hetzner StorageBox via rsync/SSH (skipped when credentials are absent).
6. Purges local archives older than each target's retention (62 days for `dagster`/`metabase`, 7 days for `duckdb`/`ducklake`).
7. Restarts the containers that were stopped in step 2 — always, even on failure.

### Scheduling with Cron

Use `scripts/setup_backup_cron.sh` to install the cron entry on the host:

```bash
# Preview what will be added (dry run):
scripts/setup_backup_cron.sh

# Write to crontab:
scripts/setup_backup_cron.sh --install
```

This installs a daily job at 02:00 UTC that runs `docker compose run --rm backup`
from the repository directory and appends output to `/data_backup/logs/cron.log`.
`DOCKER_HOST` is set inline in the cron entry so Docker is reachable without
inheriting the shell environment.

To install manually instead:

```cron
# Daily backup of all targets at 02:00 UTC
0 2 * * * DOCKER_HOST=unix:///var/run/docker.sock cd "/path/to/repo" && docker compose run --rm backup >> /data_backup/logs/cron.log 2>&1
```

### Restoring From a Backup

Restores also run via the `backup` service, which has all the necessary volume
mounts (source directories, backup archives, Docker socket):

```bash
# Restore all targets from the most recent backup (interactive confirmation)
docker compose run --rm backup python -m ddd_python.ddd_utils.restore_platform

# Non-interactive (for scripted use)
docker compose run --rm backup python -m ddd_python.ddd_utils.restore_platform --yes

# Restore a specific timestamp
docker compose run --rm backup python -m ddd_python.ddd_utils.restore_platform --timestamp 20260526_020000

# Restore a single target
docker compose run --rm backup python -m ddd_python.ddd_utils.restore_platform --targets dagster --yes
docker compose run --rm backup python -m ddd_python.ddd_utils.restore_platform --targets metabase --yes
```

The restore script resolves all archive paths before touching any live data
(fail-fast), then stops the relevant containers, extracts the zip archives
in-place (overwriting existing files), and restarts the containers.

Metabase data is owned by UID 2000 inside the container. The restore script
handles this automatically by running the extraction inside a temporary Docker
container as that UID, so restored files carry the correct ownership.

### Retention

| Location | Retention |
| --- | --- |
| Local `dagster` / `metabase` | 62 days — older archives are pruned at the end of each backup run |
| Local `duckdb` / `ducklake` | 7 days — older archives are pruned at the end of each backup run |
| Hetzner StorageBox | Forever — archives are uploaded with rsync and never deleted remotely |

### Querying the Backup Log

Every backup run appends one NDJSON record per target to
`/data_backup/logs/backup_log_{timestamp}.ndjson`. Query all runs with DuckDB:

```sql
SELECT
    run_started_at,
    target,
    status,
    archive_name,
    archive_size_mb,
    archive_verified,
    uploaded_to_storagebox,
    duration_seconds,
    error_message
FROM read_json_auto('/data_backup/logs/backup_log_*.ndjson')
ORDER BY run_started_at DESC;
```

---

## Production Infrastructure

The production environment runs on a single [Hetzner Cloud](https://www.hetzner.com/cloud) server.

### Deployment

Deploys to production are performed with [`scripts/deploy.sh`](scripts/deploy.sh),
run from an operator laptop. There is no CI/CD service and no container
registry — the script pulls the latest `main` on the server and rebuilds the
images in place over SSH.

```bash
# Configure once (copy the template and fill in your values)
cp .env.deploy.example .env.deploy
#   DEPLOY_HOST — server IP or hostname
#   DEPLOY_USER — SSH user
#   DEPLOY_PATH — absolute path to the repo on the server
#   DEPLOY_KEY  — SSH private key (default: ~/.ssh/id_ed25519)
#   DEPLOY_PORT — SSH port (default: 22)

# Deploy the current main branch
./scripts/deploy.sh
```

The script SSHes into the server and runs, in order:

1. `git fetch origin main && git checkout main && git reset --hard origin/main` — sync to the latest `main`.
2. `docker compose down --remove-orphans` — stop running containers.
3. `docker compose build` — rebuild the pipeline and Metabase images.
4. `docker compose up -d dagster metabase` — start the persistent services.

The server must have read access to the GitHub repository (a deploy key) so the
`git fetch` step runs unattended. See
[documentation/hetzner_infrastructure.md](documentation/hetzner_infrastructure.md)
for the full server, SSH-key, volume, and firewall reference.

### Server

| Property | Value |
| --- | --- |
| Type | CPX42 (Hetzner Cloud) |
| Location | Nuremberg (nbg1) |
| OS image | Hetzner "Docker CE" app image (Docker pre-installed) |

### Storage Volumes

Two persistent block volumes are attached and mounted at boot:

| Mount point | Size | Purpose |
| --- | --- | --- |
| `/data` | 50 GB | Live data — DuckDB database, dlt state, dbt logs, Dagster home, Metabase state, Bronze/Silver/Gold files |
| `/data_backup` | 50 GB | Local backup archives (per-target retention: 62 days dagster/metabase, 7 days duckdb/ducklake) and backup logs |

### Firewall

A Hetzner Cloud firewall restricts inbound access to a small set of whitelisted
IP addresses. Only the following ports are open:

| Port | Service |
| --- | --- |
| 22 | SSH |
| 3000 | Dagster UI |
| 3001 | Metabase |

All other inbound traffic is blocked.

---

## Alerting (ntfy.sh)

Both Dagster run-status sensors send push notifications via [ntfy.sh](https://ntfy.sh)
— an open-source, self-hostable push notification service with free-tier cloud hosting.

### Enabling alerts

1. Pick a topic name on [ntfy.sh](https://ntfy.sh) (or self-host).
2. Subscribe to it with the ntfy app (Android / iOS / web).
3. Add to `.env`:

   ```dotenv
   NTFY_TOPIC=your-topic-name   # topic name only — no https://ntfy.sh/ prefix
   ENVIRONMENT=PROD             # included in every alert message
   ```

4. Restart the Dagster container to pick up the new variables:

   ```bash
   docker compose restart dagster
   ```

### What you receive

| Event | Priority | Tag |
| --- | --- | --- |
| Job SUCCESS | default | ✅ |
| Job FAILURE | high | 🚨 |

Every notification includes the job name, a short run ID, and the `ENVIRONMENT` label.
All registered jobs are covered — no configuration needed when new jobs are added.

### Opt-in

Alerts are **disabled by default**. When `NTFY_TOPIC` is not set, the sensors
still run and write their log summaries, but the ntfy.sh POST is silently skipped.
A failed POST (network error, wrong topic) is caught and logged as a Dagster warning
— it never blocks the sensor tick or the next job run.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `CapacityNotActive` in export / Bronze models | Fabric capacity is paused | Resume the capacity in the Azure portal and re-run |
| `Parser Error: syntax error at or near "DBT_..."` in Bronze | Stale dbt partial-parse cache | Run `dbt run --no-partial-parse` or delete `dbt/target/partial_parse.msgpack` |
| Asset shows as **unsynced** (yellow) in Dagster | Upstream materialized without running downstream | Materialize the downstream asset, or run the relevant job |
| `INTERNAL Error: Failed to bind column reference "id"` | DuckDB QUALIFY + UNION ALL bug (affects 1.5.0; fixed in tests for 1.5.1) | Ensure `duckdb>=1.5.1` is installed: `pip install -e ".[dagster,dev]"` |
| `write_deltalake() unexpected keyword argument` | `deltalake` version mismatch | `pip install -e ".[dagster,dev]"` to restore pinned versions |
| `FileNotFoundError` on DuckDB path | `DUCKDB_DATABASE_LOCATION` not set | Check `.env` and ensure the directory exists |
| Bronze models return no rows (local mode) | `DANISH_DEMOCRACY_DATA_SOURCE` or `RFAM_DATA_SOURCE` points to empty or wrong directory | Verify files exist under `LOCAL_STORAGE_PATH/Files/Bronze/DDD/{entity}/` or `.../RFAM/{table}/` |
| Bronze models return no rows (OneLake mode) | `DANISH_DEMOCRACY_DATA_SOURCE` or `RFAM_DATA_SOURCE` missing or wrong `abfss://` path | Set the correct path pointing to the Bronze NDJSON root on OneLake |
| dbt uses wrong output profile | `STORAGE_TARGET` mismatch | `dbt/profiles.yml` selects the `local` or `onelake` output based on `STORAGE_TARGET` — ensure `.env` is set correctly |
| Azure credential errors in local mode | `STORAGE_TARGET=onelake` set accidentally | Set `STORAGE_TARGET=local` in `.env`; no Azure vars are needed |
| dbt models missing (empty `models/` dir) | Model generation not run | Run `python -m ddd_python.ddd_dbt.generate_dbt_models` |
| Metabase not reachable after a pipeline run | `start_metabase_asset` failed or container not started | Run `docker compose up metabase` or check the `start_metabase_asset` log in the Dagster UI |
| DuckDB write error while Metabase is running | Metabase holds an open connection; `stop_metabase_asset` was not triggered | Stop Metabase manually (`docker stop ddd-metabase`) before writing, or run the job from the Dagster UI which manages this automatically |
| `Permission denied` on `/var/run/docker.sock` | `DOCKER_GID` in `.env` does not match the socket's group on the host | Run `stat -c '%g' /var/run/docker.sock` on the host and update `DOCKER_GID` in `.env`, then restart the affected container |

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
