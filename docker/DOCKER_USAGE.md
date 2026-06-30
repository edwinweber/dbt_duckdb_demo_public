# Docker Usage

Last updated: May 2026

## Prerequisites

1. **Docker Engine 24+** and **Docker Compose v2+** installed.
2. Copy `.env.example` to `.env` and fill in your values. See
   [`../.env.example`](../.env.example) for the environment variable reference and descriptions.

## One-Time Host Setup

Run these commands once on the host **before** starting any container.
They create the bind-mount directories with the correct ownership so the
non-root container user (`app`, UID 1000) can write to them.

```bash
# Create all data and backup directories
sudo mkdir -p /data/{dlt_pipelines,duckdb,dbt_logs,dagster,local,metabase/data,metabase/duckdb-extensions}
sudo mkdir -p /data_backup/{dagster,metabase,logs}

# app user (UID 1000) owns the pipeline and backup directories
sudo chown -R 1000:1000 /data/dlt_pipelines /data/dbt_logs /data/dagster /data/local
sudo chown -R 1000:1000 /data/duckdb /data_backup

# Metabase (UID 2000) owns its own state directories
sudo chown -R 2000:2000 /data/metabase/data /data/metabase/duckdb-extensions

# DuckDB creates WAL/lock files even during read-only queries.
# o+rwx lets Metabase (UID 2000) write into the UID-1000-owned /data/duckdb directory.
chmod -R o+rwx /data/duckdb

# backup (UID 1000) reads Metabase data (owned by UID 2000) to create archives
chmod -R o+rX /data/metabase/data

# Add the Docker socket GID to .env so the dagster and backup containers can
# reach the socket as non-root.
echo "DOCKER_GID=$(stat -c '%g' /var/run/docker.sock)" >> .env
```

## Build the Image

```bash
docker compose build
```

## Seed Volumes With Local State (Optional, One-Time)

Copies your existing dlt pipeline state, Dagster run history, and dbt logs
into the Docker volumes so containers start with your current state:

```bash
docker/docker-seed-volumes.sh
```

## Running Services

Four services cover all use cases:

| Service | Purpose |
| --- | --- |
| `run` | Generic Python runner for one-off pipeline steps |
| `dagster` | Dagster webserver + daemon, or one-off job execution |
| `metabase` | Metabase BI — connects directly to the DuckDB file |
| `backup` | One-off backup (and restore) of Dagster and Metabase state |

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
Dagster run handles initialization transparently via `docker/docker-entrypoint.sh`.

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

## Backup and Restore

The `backup` service runs the backup and restore Python modules inside the
pipeline container. No Python or extra tooling is needed on the host — only
Docker.

```bash
# Back up all targets (Dagster + Metabase)
docker compose run --rm backup

# Back up a single target
docker compose run --rm backup python -m ddd_python.ddd_utils.backup_platform --targets dagster
docker compose run --rm backup python -m ddd_python.ddd_utils.backup_platform --targets metabase

# Restore from the most recent backup (interactive — prompts for confirmation)
docker compose run --rm backup python -m ddd_python.ddd_utils.restore_platform

# Restore non-interactively (for scripted use)
docker compose run --rm backup python -m ddd_python.ddd_utils.restore_platform --yes

# Restore a specific timestamp
docker compose run --rm backup python -m ddd_python.ddd_utils.restore_platform --timestamp 20260526_020000

# Restore a single target
docker compose run --rm backup python -m ddd_python.ddd_utils.restore_platform --targets dagster
```

### Cron Setup

Install the nightly cron entry (02:00 UTC) on the host with the helper script:

```bash
scripts/setup_backup_cron.sh            # preview
scripts/setup_backup_cron.sh --install  # write to crontab
```

Backup archives and logs are written to `/data_backup/` on the host:

| Path | Contents |
| --- | --- |
| `/data_backup/dagster/` | Timestamped zip archives of the Dagster home directory |
| `/data_backup/metabase/` | Timestamped zip archives of the Metabase data directory |
| `/data_backup/logs/cron.log` | Stdout/stderr from cron-triggered runs |
| `/data_backup/logs/backup_log_*.ndjson` | Structured per-run backup records (queryable with DuckDB) |
| `/data_backup/logs/restore_log_*.ndjson` | Structured per-run restore records (queryable with DuckDB) |

Query all backup runs with DuckDB:

```sql
SELECT * FROM read_json_auto('/data_backup/logs/backup_log_*.ndjson')
ORDER BY run_started_at DESC;
```

---

## Troubleshooting

### Run a Shell Inside the Container

```bash
docker compose run --rm --entrypoint bash run
```

### Verify the Container Runs as Non-Root

```bash
docker compose run --rm --entrypoint id run
# Expected: uid=1000(app) gid=1000(app) groups=1000(app),<DOCKER_GID>(docker)
```

### Docker Socket Permission Denied

If backup or Dagster jobs fail with `permission denied` on `/var/run/docker.sock`:

```bash
# Check the socket GID on the host
stat -c '%g' /var/run/docker.sock

# Make sure .env contains the right value
grep DOCKER_GID .env
```

Update `DOCKER_GID` in `.env` to match, then restart the affected containers.
