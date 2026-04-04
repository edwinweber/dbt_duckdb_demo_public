# Docker Usage

Last updated: April 2026

## Prerequisites

1. **Docker Engine 24+** and **Docker Compose v2+** installed.
2. Copy `.env.example` to `.env` and fill in your values. See the
   [environment variable reference](README.md#environment-variable-reference)
   for required values per storage mode.

## Build the Image

```bash
docker compose build
```

## Seed Volumes With Local State (Optional, One-Time)

Copies your existing dlt pipeline state, Dagster run history, and dbt logs
into the Docker volumes so containers start with your current state:

```bash
./docker-seed-volumes.sh
```

## Running Services

Two services cover all use cases:

| Service | Purpose |
| --- | --- |
| `run` | Generic Python runner for one-off pipeline steps |
| `dagster` | Dagster webserver + daemon, or one-off job execution |

### Pipeline Steps (via `run`)

All Python modules are executed with the same `run` service — pass the module name:

```bash
# 1. Extract data from Danish Parliament API to Bronze storage
docker compose run --rm run ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data

# 1b. Extract data from Rfam MySQL database to Bronze storage
docker compose run --rm run ddd_python.ddd_dlt.dlt_run_extraction_pipelines_rfam

# 2. Generate dbt model SQL files (only needed once or after entity changes)
docker compose run --rm run ddd_python.ddd_dbt.generate_dbt_models

# 3. Run dbt build (Bronze → Silver → Gold transformations)
docker compose run --rm run ddd_python.ddd_dbt.dbt_build_with_unique_logfile

# 4. Export Silver tables to Delta Lake
docker compose run --rm run ddd_python.ddd_dlt.export_main_silver_to_fabric_silver

# 5. Export Gold tables to Delta Lake
docker compose run --rm run ddd_python.ddd_dlt.export_main_gold_to_fabric_gold
```

### Initialize DuckDB (Automatic)

When `STORAGE_TARGET=onelake`, the Docker entrypoint automatically installs
DuckDB extensions (httpfs, azure, delta) and creates a persistent Azure service
principal secret. No separate command is needed — the first `dbt build` or
Dagster run handles initialization transparently via `docker-entrypoint.sh`.

### Full Pipeline via Dagster (Single Command)

Runs the complete end-to-end pipeline as a single Dagster job:

```bash
docker compose run --rm dagster job execute -j full_pipeline_job -w workspace.yaml
```

### Dagster Orchestration UI

```bash
# Start Dagster webserver on http://localhost:3000
docker compose up dagster

# Run in background
docker compose up -d dagster

# View logs
docker compose logs -f dagster

# Stop
docker compose down
```

## Environment Variables in Docker

Environment variables are handled in two layers:

1. **`env_file: .env`** — loads all variables from your `.env` file.
2. **`environment:` overrides** — the `docker-compose.yml` remaps path variables
   (e.g., `DUCKDB_DATABASE_LOCATION`, `DAGSTER_HOME`) to container volume mount
   paths, taking precedence over `.env` values.

Variables like `STORAGE_TARGET`, `AZURE_TENANT_ID`, and `AZURE_CLIENT_SECRET` are
passed through from `.env` without modification.

## Persistent Volumes

Five named volumes keep state across container runs:

| Volume | Contents |
| --- | --- |
| `dlt_pipelines` | dlt pipeline state (incremental load tracking) |
| `duckdb_data` | DuckDB database file |
| `dbt_logs` | dbt build log files |
| `dagster_data` | Dagster run history, schedules, storage |
| `local_storage` | Bronze / Silver / Gold data files (local storage mode) |

### Inspect Volume Contents

```bash
docker volume ls | grep dbt_duckdb_demo
docker run --rm -v dbt_duckdb_demo_duckdb_data:/data alpine ls -la /data
```

### Reset All Data (Start Fresh)

```bash
docker compose down -v
```

### Reset a Single Volume

```bash
docker compose down
docker volume rm dbt_duckdb_demo_dlt_pipelines
```

## Resource Requirements

A full pipeline run (18 entities, Bronze → Silver → Gold → export) typically
requires approximately 2 GB of RAM and 1 GB of disk space for the DuckDB
database and Delta Lake tables.

## Rebuilding After Code Changes

```bash
docker compose build
```

## Troubleshooting

### Run a Shell Inside the Container

```bash
docker compose run --rm --entrypoint bash run
```
