# Docker Usage

How to run the pipeline with Docker Compose: host setup, the four services,
pipeline commands, RustFS, and backups. The production server itself is described
in [hetzner_infrastructure.md](../documentation/hetzner_infrastructure.md).

## Prerequisites

1. Docker Engine 24+ with Docker Compose v2.
2. `cp .env.example .env`. The defaults run fully locally; `docker-compose.yml`
   replaces all path variables with container paths, so the `/path/to/repo`
   placeholders in `.env` do not matter inside Docker.

## One-time host setup

All containers use **host bind mounts under `/data` and `/data_backup`** (no
named Docker volumes) and run as non-root users: the pipeline as `app`
(UID 1000), Metabase as UID 2000. Create the directories with the right owners
before the first start:

```bash
sudo scripts/setup_host_permissions.sh            # --dry-run to preview
```

The script creates the directories, sets ownership (UID 1000 for pipeline and
backup data, UID 2000 for Metabase), gives `duckdb_server` (UID 1000) write
access to `/data/duckdb`, gives Metabase read access to `/data/ducklake`, the
backup user read access to `/data/metabase/data`, and writes `DOCKER_GID` to
`.env` so the `dagster` and `backup` containers can use the Docker socket.

It also creates `/data/ducklake` (DuckLake Parquet data, readable by Metabase)
in every mode, because all services mount it. The backup creates its own
archive directories under `/data_backup`.

| Host path | Contents |
| --- | --- |
| `/data/local` | `LOCAL_STORAGE_PATH`: `Files/{Bronze,Silver,Gold}/` and `logs/` |
| `/data/duckdb` | the `.duckdb` file (and the DuckLake catalog) |
| `/data/ducklake` | DuckLake Parquet data (DuckLake mode) |
| `/data/dagster` | `DAGSTER_HOME`: run history, schedules |
| `/data/dbt_logs` | dbt JSON logs |
| `/data/dlt_pipelines` | dlt working directory |
| `/data/metabase/data`, `/data/metabase/duckdb-extensions` | Metabase state and its DuckDB extensions |
| `/data_backup` | backup archives and logs |

Because everything is a bind mount, `docker compose down -v` deletes no data;
removing files under `/data` on the host does.

## Build

```bash
docker compose build
```

## One-time token setup

`QUACK_SERVER_TOKEN` authenticates all connections to the quack RPC server.
Generate it once and add it to `.env`:

```bash
python3 -c "import secrets; print(secrets.token_hex(32))"
# copy the output, then add to .env:
# QUACK_SERVER_TOKEN=<the generated value>
```

The token must be at least 4 characters. All services that connect to the server
(`duckdb_server`, `run`, `dagster`, `metabase`) read it from the environment.

## Services

| Service | Container | Purpose |
| --- | --- | --- |
| `duckdb_server` | `ddd-duckdb-server` | Sole read-write DuckDB owner; all others connect via quack RPC; restarts unless stopped |
| `run` | `ddd-run` | One-off runner: `docker compose run --rm run python -m <module>` |
| `dagster` | `ddd-dagster` | Dagster webserver + daemon on port 3000; restarts unless stopped; 6 GB memory limit |
| `metabase` | `ddd-metabase` | Metabase on port 3001, connects via quack; restarts unless stopped; 4 GB memory limit |
| `backup` | `ddd-backup` | One-off backup (default command) and restore |

## Pipeline steps

Start `duckdb_server` before running any pipeline step or bringing up `dagster`/`metabase`:

```bash
docker compose up -d duckdb_server
docker compose run --rm run python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data
docker compose run --rm run python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_rfam
docker compose run --rm run python -m ddd_python.ddd_dbt.generate_dbt_models   # only after entity changes
docker compose run --rm run python -m ddd_python.ddd_dbt.dbt_build_with_unique_logfile
docker compose run --rm run python -m ddd_python.ddd_dlt.export_silver         # optional Delta export
docker compose run --rm run python -m ddd_python.ddd_dlt.export_gold
```

No Metabase stop/start is needed: `duckdb_server` is the sole writer; `run`,
`dagster` and `metabase` all connect via quack and never hold the file directly.

### Remote access to the DuckDB server

`duckdb_server` publishes port 9494 on the Docker host, so other machines can
connect as quack clients (DuckDB 2.0 CLI/Python, DBeaver with the 2.0 JDBC driver):

```sql
INSTALL quack; LOAD quack;
ATTACH 'quack:<docker-host>:9494' AS qk (TOKEN '<QUACK_SERVER_TOKEN>', DISABLE_SSL true);
USE qk;
```

Use `ATTACH` followed by `USE` for BI and SQL tools (DBeaver, Metabase). With `CONNECT` the
JDBC metadata calls that list tables fail in DuckDB 2.0, and Arrow output is blocked. After
`USE qk` the views and tables are `main_gold.<view>`, `main_silver.<table>` and so on. The
DuckLake Silver tables are `qk.ducklake_catalog.main_silver.<table>`.

- The client must run the same DuckDB line as the server (2.x); a 1.5 client fails
  with a deserialization error.
- The server serves plain HTTP (`disable_ssl`), so the token and query results are not
  encrypted. On untrusted networks use a VPN or an SSH tunnel
  (`ssh -L 9494:localhost:9494 <docker-host>`), or restrict the listener with
  `QUACK_SERVER_BIND=127.0.0.1` in `.env`.
- `QUACK_SERVER_HOST_PORT` changes the host port. Open the port in the host firewall only
  for the machines that need it. Anyone with the token has read-write access to the
  database, including DuckLake Silver.

The whole chain as one Dagster job, without the UI:

```bash
docker compose run --rm dagster dagster job execute -m ddd_python.ddd_dagster.definitions -j full_pipeline_job
```

### Dagster UI

```bash
docker compose up -d duckdb_server dagster metabase   # Dagster :3000, Metabase :3001
docker compose logs -f dagster
docker compose down
```

### Metabase first-time setup (one-time per install)

When Metabase starts, its entrypoint renders `docker/metabase-duckdb-init.sql`
with the real `QUACK_SERVER_TOKEN` value and writes the result to
`/data/metabase/duckdb-init.sql` (visible on the host). The admin must paste
the contents of that rendered file into the Metabase UI once:

1. Open Metabase at `http://localhost:3001`.
2. Go to **Admin → Databases → Add database → DuckDB**.
3. In the **Initialization SQL** field paste the contents of
   `/data/metabase/duckdb-init.sql`.
4. Save. Metabase will connect via quack and the database will be available.

The rendered file is regenerated on every `metabase` container restart (e.g.
after a token rotation), so repeat this step if the token changes. Do not paste
the source template (`docker/metabase-duckdb-init.sql`) — it contains the
placeholder `<QUACK_SERVER_TOKEN>`, not the real value.

## Environment variables in Docker

Every service loads `.env` (`env_file`), then `docker-compose.yml` overrides
the path variables (`LOCAL_STORAGE_PATH=/data/local`, `DUCKDB_DATABASE_LOCATION`,
`DAGSTER_HOME`, `DBT_*`, …) with container paths.

The Bronze read paths default to the container mount
(`/data/local/Files/Bronze/{DDD,RFAM}`). With `RAW_STORAGE_TARGET=s3`, set
`DANISH_DEMOCRACY_DATA_SOURCE_DOCKER` and `RFAM_DATA_SOURCE_DOCKER` in `.env` to
the `s3://` URIs.

With `STORAGE_TARGET=onelake`, the entrypoint (`docker/docker-entrypoint.sh`)
installs the DuckDB `httpfs`, `azure` and `delta` extensions and creates the
persistent Azure service-principal secret on start; no separate step is needed.

## RustFS for local S3

```bash
docker compose -f docker-compose.yml -f docker-compose.rustfs.yml up -d rustfs
# S3 API on http://localhost:9000, console on http://localhost:9001
```

Create the buckets, set the S3 variables in `.env` (RustFS needs
`S3_URL_STYLE=path`) and run the pipeline as usual. The bucket commands and an
example `.env` are in
[storage_combinations.md](../documentation/storage_combinations.md#rustfs-for-local-development).

## Backup and restore

The `backup` service runs the backup and restore modules inside the pipeline
image; the host needs only Docker. Targets: `dagster`, `metabase`, `duckdb`, plus
`ducklake` when `SILVER_STORAGE_FORMAT=ducklake` (archived before `duckdb`, which
holds the catalog). Each target stops the containers that hold its files and
restarts them afterwards.

```bash
docker compose run --rm backup                                   # all targets
docker compose run --rm backup python -m ddd_python.ddd_utils.backup_platform --targets dagster

docker compose run --rm backup python -m ddd_python.ddd_utils.restore_platform               # latest, asks for confirmation
docker compose run --rm backup python -m ddd_python.ddd_utils.restore_platform --yes
docker compose run --rm backup python -m ddd_python.ddd_utils.restore_platform --timestamp 20260526_020000
docker compose run --rm backup python -m ddd_python.ddd_utils.restore_platform --targets dagster
```

Nightly backups (02:00 in the server's time zone) are installed with:

```bash
scripts/setup_backup_cron.sh            # preview
scripts/setup_backup_cron.sh --install  # write to crontab
```

| Path | Contents |
| --- | --- |
| `/data_backup/<target>/` | timestamped zip archives per target |
| `/data_backup/logs/cron.log` | output of cron-triggered runs |
| `/data_backup/logs/backup_log_*.ndjson`, `restore_log_*.ndjson` | one structured record per run |

```sql
SELECT * FROM read_json_auto('/data_backup/logs/backup_log_*.ndjson')
ORDER BY run_started_at DESC;
```

Retention and the off-site Hetzner StorageBox upload are described in
[hetzner_infrastructure.md](../documentation/hetzner_infrastructure.md#backup-and-nightly-cron).

## Troubleshooting

```bash
docker compose run --rm --entrypoint bash run      # shell in a throw-away container
docker compose run --rm --entrypoint id run        # expect uid=1000(app) … <DOCKER_GID>(docker)
```

**`permission denied` on `/var/run/docker.sock`:** `DOCKER_GID` in `.env` must
match `stat -c '%g' /var/run/docker.sock` on the host; fix it and restart the
`dagster` container.

More failure scenarios: [runbook.md](../documentation/runbook.md).
