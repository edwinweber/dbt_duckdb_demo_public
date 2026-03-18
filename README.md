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
orchestrated by Dagster, and exports the final tables to Microsoft Fabric
OneLake as Delta Lake tables for downstream BI consumption.

---

## Architecture overview

```text
┌─────────────────────────────────────────────────────────────────────┐
│  Schedule  (06:00 UTC daily, disabled by default)                   │
│  └── danish_parliament_full_pipeline_job                            │
└─────────────────────────────┬───────────────────────────────────────┘
                              │ triggers
┌─────────────────────────────▼───────────────────────────────────────┐
│  Layer 1 — Extraction  (dlt + Dagster assets)                       │
│  18 OData resources  →  NDJSON files on OneLake Bronze              │
│  ├── Incremental (6): Aktør, Møde, Sag, Sagstrin,                  │
│  │                    SagstrinAktør, Stemme                         │
│  └── Full-extract (12): reference / lookup tables                   │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────┐
│  Layer 2 — Bronze  (dbt views)                                      │
│  DuckDB read_json_auto over OneLake NDJSON                          │
│  One view per entity — no transformations, raw data preserved       │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────┐
│  Layer 3 — Silver  (dbt incremental tables)                         │
│  Hash-based CDC → SCD Type 2 history per entity                     │
│  Companion _cv (current-version) views per entity                   │
└──────────────┬──────────────────────────────────┬───────────────────┘
               │                                  │
┌──────────────▼──────────────┐   ┌───────────────▼───────────────────┐
│  Silver export (Delta Lake) │   │  Layer 4 — Gold  (dbt views)      │
│  DuckDB Silver → OneLake    │   │  Star-schema: actor, vote, case,   │
│  Incremental append         │   │  meeting, individual_votes + CVs   │
└─────────────────────────────┘   └───────────────┬───────────────────┘
                                                  │
                                  ┌───────────────▼───────────────────┐
                                  │  Gold export   (Delta Lake)       │
                                  │  DuckDB Gold → OneLake            │
                                  │  Full overwrite                   │
                                  └───────────────────────────────────┘
```

### Tech stack

| Concern | Tool |
| --- | --- |
| Orchestration | Dagster (software-defined assets, schedules, sensors) |
| Extraction | dlt (Data Load Tool) |
| Transformation | dbt-core + dbt-duckdb |
| Local storage | DuckDB |
| Cloud storage | Microsoft Fabric OneLake (ADLS Gen2 / Delta Lake) |
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
├── dbt/                        dbt project
│   ├── models/
│   │   ├── bronze/             Views over raw OneLake NDJSON (read_json_auto)
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
├── .env.example                Template — copy to .env and fill in credentials
├── workspace.yaml              Dagster workspace entry-point
├── Dockerfile                  Container image definition
├── docker-compose.yml          Service definitions (run, dagster)
├── DOCKER_USAGE.md             Docker usage reference
└── pyproject.toml              Project metadata and dependencies
```

---

## Walkthrough

### 1. Prerequisites

- Python 3.12+
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
# Edit .env and fill in all required values
```

Key variables:

| Variable | Description |
| --- | --- |
| `DAGSTER_HOME` | Path to `.dagster/` at repo root — Dagster stores run history and schedule state here |
| `DUCKDB_DATABASE_LOCATION` | Absolute path to the local DuckDB file |
| `DUCKDB_DATABASE` | DuckDB database name (without path) |
| `DBT_DANISH_DEMOCRACY_DATA_SOURCE` | `abfss://` path to Bronze NDJSON root on OneLake |
| `DBT_PROJECT_DIRECTORY` | Absolute path to the `dbt/` folder |
| `DBT_MODELS_DIRECTORY` | Absolute path to `dbt/models/` |
| `DLT_PIPELINES_DIR` | Local directory for dlt pipeline state |
| `FABRIC_WORKSPACE` | Fabric workspace name |
| `FABRIC_ONELAKE_STORAGE_ACCOUNT` | Usually `onelake` |
| `FABRIC_ONELAKE_FOLDER_BRONZE` | `<Workspace>.Lakehouse/Files/Bronze` |
| `FABRIC_ONELAKE_FOLDER_SILVER` | `<Workspace>.Lakehouse/Files/Silver` |
| `FABRIC_ONELAKE_FOLDER_GOLD` | `<Workspace>.Lakehouse/Files/Gold` |
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

# 3. Export to OneLake Delta Lake
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

### Reference table refresh

The 12 lookup entities that don't support date filtering are fetched in full by:

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

## Data model

### Entities (18)

| Category | Entities |
| --- | --- |
| **Incremental** | Aktør, Møde, Sag, Sagstrin, SagstrinAktør, Stemme |
| **Full-extract** | Afstemning, Afstemningstype, Aktørtype, Mødestatus, Mødetype, Periode, Sagskategori, Sagsstatus, Sagstrinsstatus, Sagstrinstype, Sagstype, Stemmetype |

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

- **`danish_democracy_data_source` is hardcoded in `dbt_project.yml`** — dbt
  does not recursively render Jinja inside project variable values, so
  `env_var()` cannot be nested inside `var()`.  To point Bronze sources at a
  different OneLake path, either edit the variable directly in
  `dbt_project.yml` or pass `--vars '{danish_democracy_data_source: "abfss://..."}'`
  on the CLI.

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
| Bronze models read no rows | `DBT_DANISH_DEMOCRACY_DATA_SOURCE` missing or wrong | Set the correct `abfss://` path pointing to the Bronze NDJSON root on OneLake |
| dbt models missing (empty `models/` dir) | Model generation not run | Run `python -m ddd_python.ddd_dbt.generate_dbt_models` |

---

## Contributing

This is a learning and reference project. Issues, suggestions, and pull requests
are welcome. For larger changes, please open an issue first so we can discuss
the approach.

---

## Environment variable reference

See [.env.example](.env.example) for the full list with inline descriptions.
