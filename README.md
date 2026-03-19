# Danish Democracy Data — dbt + DuckDB Demo

A demo project that ingests, transforms, and publishes open data from the
Danish Parliament (Folketing) OData API.

> **Forked from** [bgarcevic/danish-democracy-data](https://github.com/bgarcevic/danish-democracy-data) —
> a great starting point for working with Folketing open data. This repo extends
> that foundation with a full medallion pipeline, SCD Type 2 history,
> Dagster orchestration, and Microsoft Fabric / OneLake export.

The goal is to show how far you can get with a **low-cost, open-source stack**
(DuckDB + dbt + dlt + Dagster) before needing anything heavier. This is a
learning and reference project, not a hardened production system. The pipeline
it describes does run daily against real data, but this repo is shared as a
reference for the patterns, not as a turnkey deployment.

The pipeline follows a **medallion architecture** (Bronze → Silver → Gold),
orchestrated by Dagster. It supports two storage backends controlled by a
single environment variable:

| Mode | Storage | Requires |
| --- | --- | --- |
| `local` | `data/` directory in the repo (Docker volume) | Nothing — runs fully offline |
| `onelake` | Microsoft Fabric OneLake (ADLS Gen2 / Delta Lake) | Azure service principal |

---

## Architecture overview

```text
  ┌──────────────────────────────────────────────────────────────────────┐
  │  Dagster  (schedule 06:00 UTC daily · disabled by default)           │
  │  └── danish_parliament_full_pipeline_job                             │
  └──────────────────────────────┬───────────────────────────────────────┘
                                 │ orchestrates
  ┌──────────────────────────────▼───────────────────────────────────────┐
  │  Layer 1 — Extraction  (dlt · 18 OData entities)                     │
  │  ├── Incremental (6):  Aktør, Møde, Sag, Sagstrin, SagstrinAktør,    │
  │  │                     Stemme                                         │
  │  └── Full-extract (12): small tables, always fully extracted          │
  └─────────────────┬────────────────────────────────┬────────────────────┘
                    │                                │
       STORAGE_TARGET=local             STORAGE_TARGET=onelake
                    │                                │
    ┌───────────────▼──────────────┐  ┌──────────────▼──────────────────┐
    │  data/Files/Bronze/DDD/      │  │  <Lakehouse>/Files/Bronze/DDD/  │
    │  {entity}/{entity}_TS.json   │  │  {entity}/{entity}_TS.json      │
    └───────────────┬──────────────┘  └──────────────┬──────────────────┘
                    └──────────────────┬──────────────┘
                          DANISH_DEMOCRACY_DATA_SOURCE (env var)
  ┌───────────────────────────────────▼──────────────────────────────────┐
  │  Layer 2 — Bronze  (dbt views · code-generated)                      │
  │  DuckDB read_json_auto(DANISH_DEMOCRACY_DATA_SOURCE/{entity}/*.json)  │
  │  Works identically for local paths and abfss:// URLs                 │
  │  One view per entity · no transformations · raw data preserved       │
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

### Tech stack

| Concern | Tool |
| --- | --- |
| Orchestration | Dagster (software-defined assets, schedules, sensors) |
| Extraction | dlt (Data Load Tool) |
| Transformation | dbt-core + dbt-duckdb |
| Query engine / local storage | DuckDB |
| Cloud storage (optional) | Microsoft Fabric OneLake (ADLS Gen2 / Delta Lake) |
| Data quality | dbt built-in tests + dbt-expectations |
| Language | Python 3.12+ |

---

## Running with Docker

The easiest way to try the pipeline is via Docker — no local Python setup needed.

```bash
# Build the image
docker compose build

# Run the full pipeline end-to-end via Dagster
docker compose run --rm dagster job execute -j danish_parliament_full_pipeline_job -w workspace.yaml

# Or start the Dagster UI at http://localhost:3000
docker compose up dagster
```

See [DOCKER_USAGE.md](DOCKER_USAGE.md) for the full Docker reference including
individual pipeline steps, volume management, and troubleshooting.

---

## Project layout

```text
.
├── data/                       Local storage (git-ignored) — mirrors Fabric OneLake layout
│   └── Files/
│       ├── Bronze/DDD/         NDJSON files per entity (written by dlt in local mode)
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

#### Local mode (no Fabric required)

| Variable | Example value | Description |
| --- | --- | --- |
| `STORAGE_TARGET` | `local` | Selects local filesystem storage |
| `LOCAL_STORAGE_PATH` | `/home/you/dbt_duckdb_demo/data` | Base path for Bronze / Silver / Gold files |
| `DANISH_DEMOCRACY_DATA_SOURCE` | `<LOCAL_STORAGE_PATH>/Files/Bronze/DDD` | Bronze root that dbt reads from |
| `DAGSTER_HOME` | `/home/you/dbt_duckdb_demo/.dagster` | Dagster run / schedule state |
| `DUCKDB_DATABASE_LOCATION` | `/home/you/dbt_duckdb_demo/duckdb/danish_democracy_data.duckdb` | DuckDB file path |
| `DUCKDB_DATABASE` | `danish_democracy_data` | DuckDB database name |
| `DBT_PROJECT_DIRECTORY` | `/home/you/dbt_duckdb_demo/dbt` | Path to the `dbt/` folder |
| `DBT_MODELS_DIRECTORY` | `/home/you/dbt_duckdb_demo/dbt/models` | Path to `dbt/models/` |
| `DLT_PIPELINES_DIR` | `/home/you/dbt_duckdb_demo/dlt/pipelines_dir` | dlt state directory |
| `DANISH_DEMOCRACY_BASE_URL` | `https://oda.ft.dk/api` | Parliament OData API root |

Azure / Fabric variables are **not required** in local mode and can be left unset.

#### OneLake mode (Fabric + Azure service principal)

All local mode variables above, plus:

| Variable | Description |
| --- | --- |
| `STORAGE_TARGET` | `onelake` |
| `DANISH_DEMOCRACY_DATA_SOURCE` | `abfss://onelake.dfs.fabric.microsoft.com/<workspace>/<lakehouse>.Lakehouse/Files/Bronze/DDD` |
| `FABRIC_WORKSPACE` | Fabric workspace name |
| `FABRIC_ONELAKE_STORAGE_ACCOUNT` | Usually `onelake` |
| `FABRIC_ONELAKE_FOLDER_BRONZE` | `<Lakehouse>.Lakehouse/Files/Bronze` |
| `FABRIC_ONELAKE_FOLDER_SILVER` | `<Lakehouse>.Lakehouse/Files/Silver` |
| `FABRIC_ONELAKE_FOLDER_GOLD` | `<Lakehouse>.Lakehouse/Files/Gold` |
| `DLT_PIPELINE_RUN_LOG_DIR` | OneLake path for pipeline run logs |
| `AZURE_TENANT_ID` | Azure AD tenant ID |
| `AZURE_CLIENT_ID` | Service principal client ID |
| `AZURE_CLIENT_SECRET` | Service principal secret |

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

## Running the pipeline

### First run — end-to-end

For a first-time **full load**, run in this order:

```bash
# 1. Extract all 18 entities (full + incremental)
dagster job launch -w workspace.yaml --job danish_parliament_all_job

# 2. Transform: Bronze → Silver → Gold
dagster job launch -w workspace.yaml --job dbt_silver_job
dagster job launch -w workspace.yaml --job dbt_gold_job

# 3. Export Silver and Gold as Delta Lake tables
dagster job launch -w workspace.yaml --job export_silver_job
dagster job launch -w workspace.yaml --job export_gold_job
```

Or run everything in one shot:

```bash
dagster job launch -w workspace.yaml --job danish_parliament_full_pipeline_job
```

### Daily incremental runs

The schedule `danish_parliament_full_pipeline_schedule` fires at **06:00 UTC**
daily. It is **disabled by default** — enable it in the Dagster UI under
**Automation → Schedules** when you're ready to run it regularly.

For a manual incremental run (the 6 date-filterable entities only):

```bash
dagster job launch -w workspace.yaml --job danish_parliament_incremental_job
```

### Full-extract refresh

The 12 entities that are always fully extracted on every run (they support
`opdateringsdato` filtering but are small enough that a full extract keeps
delete detection simple):

```bash
dagster job launch -w workspace.yaml --job danish_parliament_full_extract_job
```

This also runs as part of `danish_parliament_full_pipeline_job`.

### Individual layers (CLI)

```bash
dagster job launch -w workspace.yaml --job dbt_bronze_job
dagster job launch -w workspace.yaml --job dbt_silver_job
dagster job launch -w workspace.yaml --job dbt_gold_job
dagster job launch -w workspace.yaml --job export_silver_job
dagster job launch -w workspace.yaml --job export_gold_job
```

---

## Local storage layout

When `STORAGE_TARGET=local`, all data lands under `LOCAL_STORAGE_PATH` in a
directory structure that intentionally mirrors the Fabric OneLake layout so
that paths are directly comparable:

```text
LOCAL_STORAGE_PATH/          (e.g. /home/you/dbt_duckdb_demo/data  or  /data/local in Docker)
└── Files/
    ├── Bronze/
    │   └── DDD/
    │       ├── aktoer/          aktoer_YYYYMMDD_HHMMSS.json
    │       ├── aktoertype/      aktoertype_YYYYMMDD_HHMMSS.json
    │       ├── afstemning/      …
    │       └── … (18 entities total)
    ├── Silver/
    │   ├── silver_aktoer/       Delta Lake table (incremental append)
    │   ├── silver_aktoertype/   Delta Lake table
    │   └── … (18 Silver tables)
    └── Gold/
        ├── actor/               Delta Lake table (full overwrite)
        ├── vote/
        └── … (Gold models)
```

Compare with OneLake (`STORAGE_TARGET=onelake`):

```text
<Workspace>/
└── <Lakehouse>.Lakehouse/Files/
    ├── Bronze/DDD/{entity}/     — NDJSON files
    ├── Silver/{table}/          — Delta Lake tables
    └── Gold/{table}/            — Delta Lake tables
```

The `DANISH_DEMOCRACY_DATA_SOURCE` variable is what dbt's Bronze layer uses to
locate the NDJSON files via DuckDB's `read_json_auto()`. Set it to either an
`abfss://` URL (OneLake) or an absolute local path — the Bronze models work
identically in both cases.

---

## Data model

### Entities (18)

| Category | Entities |
| --- | --- |
| **Incremental** (date-filtered) | Aktør, Møde, Sag, Sagstrin, SagstrinAktør, Stemme |
| **Full-extract** (always fully fetched — small tables, easy delete detection) | Afstemning, Afstemningstype, Aktørtype, Mødestatus, Mødetype, Periode, Sagskategori, Sagsstatus, Sagstrinsstatus, Sagstrinstype, Sagstype, Stemmetype |

### Silver layer — SCD Type 2

One incremental DuckDB table per entity. Each row carries standard `LKHS_`
lakehouse metadata columns:

| Column | Description |
| --- | --- |
| `LKHS_source_system_code` | Always `DDD` |
| `LKHS_date_valid_from` | Point-in-time when this version was first observed |
| `LKHS_hash_value` | SHA-256 of all business columns (64 hex chars) |
| `LKHS_cdc_operation` | `I` insert · `U` update · `D` delete |
| `LKHS_updated_at` | Pipeline run timestamp |

A companion `_cv` (current-version) view sits alongside each table and returns
the **latest non-deleted row** per entity key using `ROW_NUMBER() OVER (PARTITION BY id ORDER BY LKHS_date_valid_from DESC)`.

### Gold layer — star schema

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

Surrogate keys are SHA-256 hashes cast to BIGINT via the `cast_hash_to_bigint`
macro. Each Gold table also has a `_cv` view.

---

## dbt commands reference

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

## Running tests (pytest)

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
| `test_serialize_trace.py` | dlt run trace serialisation |
| `test_scrub_secrets.py` | Credential scrubbing in log output |
| `test_require_env.py` | Missing env var handling |
| `test_json_default.py` | JSON serialisation of custom types |

---

## CDC / SCD Type 2 design

Silver models implement hash-based change capture across Bronze snapshot files:

1. Every Bronze file is read in full on each dbt run.
2. A SHA-256 hash of all business columns is computed per row per file.
3. Rows are compared to the previous file via `LAG()`.
4. Only inserts (`I`) and updates (`U`) are appended to the incremental table.
5. Deletes (`D`) are detected during a `--full-refresh` by comparing the
   current-version view against the latest Bronze snapshot.
6. The `_cv` view always returns the latest non-deleted row per entity key.

See [documentation/silver_model_logic.md](documentation/silver_model_logic.md)
for a detailed walkthrough with compiled SQL examples.

---

## Executor concurrency model

| Job type | Executor | Reason |
| --- | --- | --- |
| Extraction | `multiprocess_executor (max 4)` | I/O bound — concurrent HTTP + file writes safe |
| Export | `multiprocess_executor (max 4)` | I/O bound — concurrent Delta Lake writes safe |
| dbt | `in_process_executor` | DuckDB single-writer constraint |

---

## Dagster home directory

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

## Known limitations

- **DuckDB pinned below 1.5** — DuckDB 1.5.0 introduced an internal
  column-binding bug that crashes `GROUP BY` queries against views using
  `QUALIFY ROW_NUMBER()`.  This breaks dbt's auto-generated `unique` tests on
  all Silver `_cv` views.  The project is pinned to `duckdb>=1.1,<1.5` until
  the upstream fix is released.  `fetch_arrow_table()` (deprecated in 1.5) is
  used accordingly.

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
| Bronze models return no rows (local mode) | `DANISH_DEMOCRACY_DATA_SOURCE` points to empty or wrong directory | Verify files exist under `LOCAL_STORAGE_PATH/Files/Bronze/DDD/{entity}/` |
| Bronze models return no rows (OneLake mode) | `DANISH_DEMOCRACY_DATA_SOURCE` missing or wrong `abfss://` path | Set the correct path pointing to the Bronze NDJSON root on OneLake |
| dbt uses wrong output profile | `STORAGE_TARGET` mismatch | `dbt/profiles.yml` selects the `local` or `onelake` output based on `STORAGE_TARGET` — ensure `.env` is set correctly |
| Azure credential errors in local mode | `STORAGE_TARGET=onelake` set accidentally | Set `STORAGE_TARGET=local` in `.env`; no Azure vars are needed |
| dbt models missing (empty `models/` dir) | Model generation not run | Run `python -m ddd_python.ddd_dbt.generate_dbt_models` |

---

## Contributing

This is a learning and reference project. Issues, suggestions, and pull requests
are welcome. For larger changes, please open an issue first so we can discuss
the approach.

---

## Environment variable reference

See [.env.example](.env.example) for the full list with inline descriptions.
