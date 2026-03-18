# Docker Usage

## Prerequisites

1. Copy `.env.example` to `.env` and fill in your values.
2. Docker and Docker Compose installed.

## Build the image

```bash
docker compose build
```

## Seed volumes with local state (optional, one-time)

Copies your existing dlt pipeline state, Dagster run history, and dbt logs
into the Docker volumes so containers start with your current state:

```bash
./docker-seed-volumes.sh
```

## Running services

Three services cover all use cases: `run` (generic Python runner), `duckdb-init`, and `dagster`.

### Pipeline steps (via `run`)

All Python modules are executed with the same `run` service — just pass the module name:

```bash
# 1. Extract data from Danish Parliament API to OneLake Bronze
docker compose run --rm run ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data

# 2. Generate dbt model SQL files (only needed once or after entity changes)
docker compose run --rm run ddd_python.ddd_dbt.generate_dbt_models

# 3. Run dbt build (Bronze -> Silver -> Gold transformations)
docker compose run --rm run ddd_python.ddd_dbt.dbt_build_with_unique_logfile

# 4. Export Silver tables to Fabric Delta Lake
docker compose run --rm run ddd_python.ddd_dlt.export_main_silver_to_fabric_silver

# 5. Export Gold tables to Fabric Delta Lake
docker compose run --rm run ddd_python.ddd_dlt.export_main_gold_to_fabric_gold
```

### Initialize DuckDB (optional)

Installs DuckDB extensions (httpfs, azure, delta) and creates a persistent Azure
service principal secret via the DuckDB CLI. Use this **only** if you need the
secret before running dbt (e.g., for ad-hoc CLI queries). Otherwise, the first
`dbt build` or Dagster run creates the persistent secret automatically via
`profiles.yml`.

```bash
docker compose run --rm duckdb-init
```

### Full pipeline via Dagster (single command)

Runs the complete end-to-end pipeline as a single Dagster job:

```bash
docker compose run --rm dagster job execute -j danish_parliament_full_pipeline_job -w workspace.yaml
```

### Dagster orchestration UI

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

## Persistent volumes

Four named volumes keep state across container runs:

| Volume | Contents |
|--------|----------|
| `dlt_pipelines` | dlt pipeline state (incremental load tracking) |
| `duckdb_data` | DuckDB database file |
| `dbt_logs` | dbt build log files |
| `dagster_data` | Dagster run history, schedules, storage |

### Inspect volume contents

```bash
docker volume ls | grep dbt_duckdb_demo
docker run --rm -v dbt_duckdb_demo_duckdb_data:/data alpine ls -la /data
```

### Reset all data (start fresh)

```bash
docker compose down -v
```

### Reset a single volume

```bash
docker compose down
docker volume rm dbt_duckdb_demo_dlt_pipelines
```

## Rebuilding after code changes

```bash
docker compose build
```

## Troubleshooting

### Run a shell inside the container

```bash
docker compose run --rm --entrypoint bash run
```
