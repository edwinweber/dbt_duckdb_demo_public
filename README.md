# Single-Server Data Engineering: A Reference Implementation

![Architecture overview: Danish Parliament OData API and Rfam MySQL are extracted by dlt into Bronze (DuckDB views), transformed through Silver (DuckDB or DuckLake) and Gold (star-schema views) by dbt, then exported as Delta Lake to local filesystem or Microsoft Fabric OneLake. Orchestrated by Dagster on Docker/Python. Consumed via Metabase dashboards and Power BI reports.](documentation/assets/architecture-overview.svg)

**Danish Democracy Data** is a working data pipeline that runs on a €35/month virtual server (including storage and backups). It ingests open data from two sources (the Danish Parliament OData API and the Rfam public MySQL database), transforms it through a medallion architecture in DuckDB, and optionally exports Delta Lake tables to Microsoft Fabric OneLake. Built with open-source tools — Dagster, dlt, dbt — it's intended as a reference implementation for data engineers who want to understand how these pieces fit together on a single server without cloud lock-in or hidden costs. The Danish Parliament data is the demo; the patterns apply to any stack of heterogeneous sources.

This is not a tutorial. It's a working pipeline that handles real data, respects the single-writer constraint of DuckDB, manages incremental and full-extract strategies, tracks data lineage, and observes itself. It is deliberately single-node — no Kubernetes, no high availability, no distributed consensus. The trade-off is simplicity and cost. If you scale past one server, the architecture still applies; you'll just add infrastructure.

## Quick Start (Docker)

```bash
git clone https://github.com/edwinweber/dbt_duckdb_demo.git
cd dbt_duckdb_demo

# Build the pipeline and Metabase images
docker compose build

# Extract data, transform, and start the Dagster UI
docker compose run --rm run python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data
docker compose run --rm run python -m ddd_python.ddd_dbt.generate_dbt_models
docker compose run --rm run python -m ddd_python.ddd_dbt.dbt_build_with_unique_logfile
docker compose up dagster    # UI at http://localhost:3000
```

**With MinIO (local S3 development):**

To test S3-compatible storage locally using MinIO, use the Docker Compose override file:

```bash
docker compose -f docker-compose.yml -f docker-compose.minio.yml up minio    # S3-compatible storage on http://localhost:9000
# Create buckets in the MinIO web UI (http://localhost:9001) or via the mc CLI
# Update .env with RAW_STORAGE_TARGET=s3 and S3 variables (see .env.example)
docker compose run --rm run python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data
```

See [docker/DOCKER_USAGE.md](docker/DOCKER_USAGE.md#local-development-with-minio) for details.

For local (no Docker) setup, see [CLAUDE.md → Running the Project](CLAUDE.md#running-the-project).

## Documentation

Before diving into the code, read these in order:

| What | Where |
| --- | --- |
| **Project architecture, entity lists, naming, env vars, and design patterns** | [CLAUDE.md](CLAUDE.md) — the canonical source of truth for this project |
| **Why these decisions were made; what was traded off** | [documentation/design_decisions.md](documentation/design_decisions.md) |
| **Per-module walkthrough: purpose, design choices, worked examples** | [documentation/python_code_explained.md](documentation/python_code_explained.md) |
| **Silver layer CDC / SCD Type 2 logic with compiled SQL** | [documentation/silver_model_logic.md](documentation/silver_model_logic.md) |
| **dbt macro reference** | [documentation/dbt_macros.md](documentation/dbt_macros.md) |
| **Why each dependency was chosen** | [documentation/python_libraries.md](documentation/python_libraries.md) |
| **Testing strategy** | [documentation/testing.md](documentation/testing.md) |
| **Common problems and fixes** | [documentation/troubleshooting.md](documentation/troubleshooting.md) |
| **Operator runbook: failure scenarios and recovery commands** | [documentation/runbook.md](documentation/runbook.md) |
| **Server setup, volumes, SSH, firewall** | [documentation/hetzner_infrastructure.md](documentation/hetzner_infrastructure.md) |
| **Executive summary** | [documentation/management-summary.md](documentation/management-summary.md) |

## Architecture

```text
  ┌────────────────────────────────────────────────────────────────────┐
  │  Dagster (schedule 06:00 Europe/Copenhagen daily, disabled)         │
  │  └── full_pipeline_job                                             │
  └──────────────────────────────┬───────────────────────────────────┘
                                 │ orchestrates
  ┌──────────────────────────────▼───────────────────────────────────┐
  │  Layer 1 — Extraction (dlt)                                        │
  │  ├─ DDD: 18 OData entities from Danish Parliament API              │
  │  │  ├── Incremental (6): Aktør, Møde, Sag, Sagstrin,             │
  │  │  │                    SagstrinAktør, Stemme                     │
  │  │  └── Full-extract (12): small lookup tables                     │
  │  └─ Rfam: 7 MySQL tables from Rfam database                        │
  │     ├── Incremental (2): family, genome                            │
  │     └── Full-extract (5): clan, clan_membership, author,           │
  │                           literature_reference, dead_family        │
  └─────────────────┬──────────────────────────┬──────────────────────┘
                    │                          │
       STORAGE_TARGET=local        STORAGE_TARGET=onelake
                    │                          │
  ┌─────────────────▼──────────────┐  ┌────────▼───────────────────┐
  │  data/Files/Bronze/            │  │  <Lakehouse>/Files/Bronze/ │
  │  DDD/{entity}/*.json           │  │  DDD/{entity}/*.json       │
  │  Rfam/{table}/*.json           │  │  Rfam/{table}/*.json       │
  └─────────────────┬──────────────┘  └────────┬───────────────────┘
                    └────────────────┬─────────┘
                         DATA_SOURCE env vars
  ┌──────────────────────────────────▼──────────────────────────────┐
  │  Layer 2 — Bronze (dbt views · code-generated)                   │
  │  DuckDB read_json_auto(DATA_SOURCE/{entity}/*.json)              │
  └──────────────────────────────────┬──────────────────────────────┘
                                     │
  ┌──────────────────────────────────▼──────────────────────────────┐
  │  Layer 3 — Silver (dbt incremental tables · DuckDB)              │
  │  Hash-based CDC → SCD Type 2 history per entity                  │
  │  Companion _cv (current-version) view per entity                 │
  └──────────────────────┬──────────────────────────┬─────────────────┘
                         │                          │
  ┌──────────────────────▼──────────────┐  ┌────────▼───────────────┐
  │  Silver export (Delta Lake)         │  │  Layer 4 — Gold        │
  │  Incremental append                 │  │  (dbt views)            │
  │  local: data/Files/Silver/          │  │  Star schema: actor,   │
  │  onelake: <Lh>/Files/Silver/        │  │  vote, case, meeting,  │
  │                                     │  │  _cv views              │
  └─────────────────────────────────────┘  └────────┬───────────────┘
                                                    │
                                      ┌─────────────▼───────────────┐
                                      │  Gold export (Delta Lake)   │
                                      │  Full overwrite every run   │
                                      │  local: data/Files/Gold/    │
                                      │  onelake: <Lh>/Files/Gold/  │
                                      └──────────────────────────────┘
```

## Why Single-Server?

- **No cloud bill.** The entire pipeline runs on a €35/month Hetzner Cloud server (including volumes and backup storage). Annual cost is less than a week's worth of traditional warehouse licensing. No lock-in, no surprises.
- **Offline development.** Clone the repo, fill in `.env` with local paths, and run without cloud credentials or API tokens (unless exporting to OneLake, which is optional). Works on a plane, in a hotel, in your local venv.
- **DuckDB's speed.** At ~1–2GB of data, DuckDB on local SSD is faster than most warehouses. No network round-trips, no query queue, no cold starts.
- **Observability without additional cost.** The pipeline writes its own Dagster events to SQLite, then reads them back as queryable dbt models. No separate log aggregation service, no extra bill.
- **Git-friendly.** dbt models, Dagster definitions, and config are all code. Code review, version history, rollback via `git reset`.
- **Easier to reason about.** If you're learning how these tools work, a single-server pipeline is easier to trace than a distributed system. You can inspect every query, understand every decision, debug with print statements.

## Who Is This For?

- **You have a small number of heterogeneous data sources** (a REST API, a MySQL database, a SaaS CSV export) and you need a Bronze→Silver→Gold pipeline without Snowflake or Redshift pricing.
- **Your data fits on one server** (under 500GB active, under 2TB total) and you'd rather own €420/year of Hetzner infrastructure than spend €30,000/year on a managed warehouse.
- **You're feeding Microsoft Fabric or Azure Data Lake** but you want Delta Lake tables without a proprietary ETL tool or cloud-native tie-in. dbt + DuckDB → Delta Lake. No Synapse. No ADF.
- **You need CDC** without Kafka, Debezium, or replication slots on your source databases. Hash the row, compare it, detect changes. Works with read-only connections and SaaS APIs.
- **You're using Dagster** (or learning it) and you need a concrete example of asset factories, sensors, multi-executor orchestration, and observability. This is one.

### This pattern is NOT for you if

- You need sub-second ad-hoc queries on multi-terabyte datasets — use Snowflake or BigQuery
- You need real-time or near-real-time ingestion — this is daily batch only
- You need incremental hard-delete detection between batch runs — the OData API has no tombstones; deletes are captured only at `--full-refresh`
- Your team needs multi-writer concurrency on the transformation layer
- You need compliance row-level security at the data layer — this is file-system access control only

## What Makes This Non-Trivial

Despite the simplicity, the pipeline demonstrates several patterns that scale:

- **Hash-based CDC with SCD Type 2** — Every Silver table tracks full history. Rows carry a SHA-256 hash of business columns; changes are detected by comparing hashes across files. Deletes are found during a full-refresh by anti-joining against Bronze.
- **Single-writer DuckDB constraint** — Dagster must serialize all dbt jobs (via `in_process_executor`) while extraction/export can run in parallel (via `multiprocess_executor`). Metabase is automatically stopped before writes and restarted afterward.
- **Switchable Silver storage** — `SILVER_STORAGE_FORMAT=duckdb` (default) stores Silver tables in the main DuckDB file, or `=ducklake` stores them as Parquet in a local DuckLake catalog. Both modes export identically to Delta Lake.
- **Incremental extraction strategies** — DDD entities use OData date filtering; Rfam tables use SQL date filters. Both track their high-water marks via dlt state. Full-extract fallback when date filtering isn't available.
- **Delta Lake export with dedup read** — Silver export detects new rows by reading the target Delta table via DuckDB's `delta_scan` (in-place, no Python materialization) and anti-joining. Only the delta appends.
- **Asset factory pattern** — Extraction assets are code-generated by a single factory function. Adding an entity updates `configuration_variables.py` and regenerates.
- **Observability layer** — dbt reads Dagster's SQLite event logs directly (`sqlite_scan`) and materializes run summaries, asset timings, and failure details into queryable DuckDB tables.

## Tech Stack

| Layer | Tool | Version |
| --- | --- | --- |
| Language | Python | ≥3.12 |
| Orchestration | Dagster | ≥1.13.11 |
| Extraction | dlt | ≥1.28.1 |
| Transformation | dbt-core + dbt-duckdb | ≥1.11 / ≥1.10 |
| Query engine | DuckDB | ≥1.5.4, <1.6 |
| Cloud storage (optional) | Microsoft Fabric OneLake | ADLS Gen2 / Delta Lake |
| Data quality | dbt-utils 1.3.0 + dbt tests | — |
| BI (optional) | Metabase | with DuckDB driver |
| Alerting (optional) | ntfy.sh | open-source push notifications |
| Container | Docker + Compose | — |
| Testing | pytest | ≥9.1.1 |

## Lessons Learned

What I'd tell someone starting a similar project:

- **DuckDB single-writer is a real constraint.** You'll discover it at 2am when dbt hangs mysteriously and Metabase is still running. Don't try to work around it; respect it and serialize writes.
- **DuckLake inline tables.** Small tables are stored inside the catalog `.ducklake` file, not as Parquet on disk. The `CALL ducklake_flush_inlined_data(...)` API exists, but in practice you can leave them inline. It doesn't matter after a few runs.
- **Incremental extraction: when to use OData filters vs. date windows.** OData `$filter=opdateringsdato ge` works for date columns with high-water-mark semantics. SQL `WHERE updated > ?` is equivalent. Full extraction is the fallback for small tables.
- **Delta export split reads and writes because of DuckDB limits.** The delta extension is read-only (v1.5.x). Writes use `deltalake` + PyArrow. This changes when newer DuckDB versions land and the Azure regression is fixed.
- **Hash-based CDC doesn't catch deletes in incremental mode.** On each run, only new Bronze files are scanned. Deletes surface during a `--full-refresh`, which replays all Bronze and diffs against Silver. Document this — it's not obvious.
- **INSTALL ducklake has a race condition under multiprocess subprocesses.** Each worker tries to install simultaneously. Wrap in `contextlib.suppress(Exception)` — duplicate installs are harmless.
- **Primary-key dict in config_variables.py: keep it explicit.** 18 entries of `"name": "id"` is more readable than a comprehension. The config is the contract; readability wins.
- **Lazy environment loading via `__getattr__` unlocks offline development.** You can run code generation and tests without Azure credentials. Just be clear about which vars are required vs. optional.

## Project Structure

| Directory | Purpose |
| --- | --- |
| `ddd_python/` | Python package — Dagster assets, dlt extraction, dbt runner, utilities |
| `dbt/` | dbt project — Bronze/Silver/Gold models, macros, seeds, tests |
| `tests/` | pytest tests — unit, integration, and end-to-end |
| `documentation/` | Markdown guides and dbt-docs |
| `data/` | Local storage root (git-ignored) — mirrors OneLake layout |
| `duckdb/` | DuckDB init scripts |
| `docker/` | Dockerfiles, entrypoints, seed script, Metabase init SQL, usage guide |
| `docker-compose.yml` | Services: run, dagster, metabase, backup |

## Data Sources

### Danish Parliament (DDD)

18 OData entities from the Folketing API (`https://oda.ft.dk/api`). No authentication required; all data is public.

- **Incremental (6):** Aktør, Møde, Sag, Sagstrin, SagstrinAktør, Stemme — filtered by `opdateringsdato ge DateTime('...')`
- **Full-extract (12):** AfstemningType, Afstemning, Aktørtype, Mødestatus, Mødetype, Periode, Sagskategori, Sagsstatus, Sagstrinsstatus, Sagstrinstype, Sagstype, Stemmetype — always fully loaded for clean delete detection

### Rfam

7 tables from the public Rfam MySQL database at `mysql-rfam-public.ebi.ac.uk:4497`. No credentials; read-only access.

- **Incremental (2):** family (pk `rfam_acc`, date `updated`), genome (pk `upid`, date `updated`)
- **Full-extract (5):** clan, clan_membership, author, literature_reference, dead_family

## Setting Up

### Prerequisites

- Python 3.12+ or Docker
- No cloud credentials needed for local mode (set `STORAGE_TARGET=local`)
- Optional: Microsoft Fabric workspace with OneLake for cloud export

### Local Setup (5 minutes)

```bash
git clone https://github.com/edwinweber/dbt_duckdb_demo.git
cd dbt_duckdb_demo

python -m venv .venv
source .venv/bin/activate
pip install -e ".[dagster,dev]"

cp .env.example .env
# Edit .env — set LOCAL_STORAGE_PATH, DUCKDB_DATABASE_LOCATION, etc.

cd dbt && dbt deps && cd ..
python -m ddd_python.ddd_dbt.generate_dbt_models
cd dbt && dbt seed --profiles-dir . && cd ..

export DAGSTER_HOME="$(pwd)/.dagster"
dagster dev -w workspace.yaml    # UI at http://localhost:3000
```

For full instructions, see [CLAUDE.md → Running the Project](CLAUDE.md#running-the-project).

## Running the Pipeline

### First Run (End-to-End)

```bash
# Extract all data
dagster job launch -w workspace.yaml --job danish_parliament_all_job
dagster job launch -w workspace.yaml --job rfam_all_job

# Transform and export
dagster job launch -w workspace.yaml --job full_pipeline_job
```

### Daily Incremental (Automated)

Two schedules are built in (disabled by default; enable in the Dagster UI):

- **06:00 Europe/Copenhagen** — Full pipeline (extraction → Bronze → Silver → Gold → export)
- **08:00 Europe/Copenhagen** — Observability layer (run summaries, asset timings, failure details)


Both use `dlt` to track incremental state, so unchanged rows are skipped. Full-extract tables are still fully loaded (better delete detection).

## Data Model

### Bronze Layer

Raw data read via `read_json_auto()` over NDJSON files. No transformations. Includes a companion `_latest` view per entity.

### Silver Layer

Hash-based CDC with SCD Type 2. Every row has:
- `LKHS_hash_value` — SHA256 of business columns (change detection)
- `LKHS_date_valid_from` — When this version was first observed
- `LKHS_cdc_operation` — I (insert), U (update), D (delete)
- `LKHS_date_inserted` — When dbt loaded it

Companion `_cv` (current-version) view returns the latest row per entity key.

### Gold Layer

Star schema for analytical queries. Includes entity dimensions and fact tables, with mostly code-generated models and a few handcrafted (e.g., individual_votes).

See [documentation/silver_model_logic.md](documentation/silver_model_logic.md) for compiled SQL examples.

## Tests

The suite includes unit, integration, and end-to-end tests. No cloud credentials required — all tests use in-memory DuckDB and mocked clients.

```bash
pytest tests/                        # all tests
pytest -v -k "silver"               # keyword filter
pytest tests/test_integration_*.py   # integration tests only
```

See [documentation/testing.md](documentation/testing.md) for the full strategy.

## Contributing

Contributions of all sizes are welcome. Open an issue first for larger changes; for bug fixes and small improvements, open a PR directly. See [CLAUDE.md → Common Tasks for AI Assistants](CLAUDE.md#common-tasks-for-ai-assistants) for how to add entities or change CDC logic.

## License

MIT. See [LICENSE](LICENSE) for details.

---

**Troubleshooting?** See [documentation/troubleshooting.md](documentation/troubleshooting.md) for common issues and fixes.

**Want deeper context?** Start with [CLAUDE.md](CLAUDE.md) (canonical reference), then [documentation/design_decisions.md](documentation/design_decisions.md) (the why behind each choice).

## About

Built and maintained by [Edwin Weber](https://github.com/edwinweber). If you have a similar use case — ingesting messy APIs and databases, building a clean medallion stack without cloud lock-in, or feeding an existing Azure/Fabric environment from a single server — feel free to open an issue or reach out via GitHub.
