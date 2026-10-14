# Operator Runbook

What to check when the pipeline fails, the common failures and their fixes, and
the commands for recovery. For what a normal run does step by step, see
[daily_pipeline_execution.md](daily_pipeline_execution.md).

Commands assume the Docker deployment (`/data/...` paths). For a host install,
drop the `docker compose run --rm run` prefix and use your `.env` paths.

## First checks

1. **Dagster UI** (<http://localhost:3000>): open the failed run; the failing
   step and its stack trace are there. With `NTFY_TOPIC` set you also got a push
   alert.
2. **Extraction logs:** one NDJSON file per pipeline under
   `DLT_PIPELINE_RUN_LOG_DIR` (default `LOCAL_STORAGE_PATH/logs`), in
   `DDD/<pipeline>/` or `RFAM/<pipeline>/`. The `error` field holds the traceback:

   ```sql
   SELECT start_time, parameters.pipeline_name, duration_seconds, error
   FROM read_json_auto('/data/local/logs/DDD/*/*_log.ndjson')
   ORDER BY start_time DESC LIMIT 10;
   ```

3. **dbt logs:** under Dagster, in the step log. Runs started with
   `dbt_build_with_unique_logfile` write `dbt_build_log_<timestamp>.json` to
   `DBT_LOGS_DIRECTORY`, one JSON event per line:

   ```bash
   jq -c 'select(.info.level == "error") | .info.msg' "$(ls -t /data/dbt_logs/dbt_build_log_*.json | head -1)"
   ```

4. **Containers:** `docker compose ps`; look for `Exited` or `unhealthy`.

## Quick reference

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| dbt hangs or fails with a lock error on the `.duckdb` file | Another process has the file open (Metabase, DBeaver, a DuckDB shell) | [§1](#1-duckdb-file-is-locked) |
| Metabase down after a run | The failure sensor's restart did not run or failed | [§2](#2-metabase-left-stopped) |
| `NoSuchBucket` / 404 on extraction | S3 bucket not created | [§3](#3-s3-bucket-missing-or-credentials-wrong) |
| `InvalidAccessKeyId`, `SignatureDoesNotMatch`, S3 400 errors | Wrong S3 credentials, or `S3_URL_STYLE=vhost` on MinIO | [§3](#3-s3-bucket-missing-or-credentials-wrong) |
| `CapacityNotActive` in the Delta export | Fabric capacity paused (`STORAGE_TARGET=onelake`) | `python -m ddd_python.ddd_utils.fabric_capacity_pause_resume resume`, then re-run the export |
| Bronze models return no rows | `DANISH_DEMOCRACY_DATA_SOURCE` / `RFAM_DATA_SOURCE` wrong, or nothing extracted yet | [§4](#4-bronze-is-empty) |
| dbt cannot find a `bronze_ddd_…` / `silver_ddd_…` model | Models not regenerated after a config change | [§5](#5-models-missing-after-adding-an-entity) |
| DuckLake attach errors | `DUCKLAKE_CATALOG_LOCATION` / `DUCKLAKE_DATA_PATH` missing or wrong | [§6](#6-ducklake-catalog-misconfigured) |
| Many unexpected `D` rows in Silver | `--full-refresh` ran while the newest Bronze file was an incremental extract | [§7](#7-silver-needs-a-rebuild) |
| Extra `I` rows, or a `U` with unchanged content, in Silver | Old Bronze files were deleted, or a Bronze file arrived out of timestamp order | [dbt_transformations.md §3.6 and §3.8](dbt_transformations.md#36-never-delete-bronze-files-arbitrarily); repair with [§7](#7-silver-needs-a-rebuild) after restoring the files |
| dbt picks the wrong target | Storage switches not what you expect | `profiles.yml` picks `local_ducklake_s3`, `local_ducklake`, `onelake` or `local`, in that order; check `SILVER_STORAGE_FORMAT`, `RAW_STORAGE_TARGET`, `STORAGE_TARGET` |
| Source freshness warning | No extraction in `DBT_FRESHNESS_WARN_AFTER_DAYS` (default 2) | [§9](#9-source-freshness-warning) |
| No ntfy.sh alert | `NTFY_TOPIC` unset or ntfy.sh unreachable | [§8](#8-no-ntfysh-alerts) |
| `FileNotFoundError` on the DuckDB path | `DUCKDB_DATABASE_LOCATION` unset or its directory missing | Set it in `.env` and create the directory |
| Odd errors after a dependency change | Package versions drifted from the pins | `pip install -e ".[dagster,dev]"`; for dbt, `bash scripts/install_dbt_oss.sh` |
| Asset shows as unsynced in Dagster | An upstream asset materialised without its downstream | Materialise the downstream asset or run the job |

---

## 1. DuckDB file is locked

DuckDB allows one writer, and a process holding the file open blocks dbt.

```bash
docker compose stop metabase            # or: scripts/stop_metabase_and_wait.sh
lsof /data/duckdb/danish_democracy_data.duckdb   # anything left? close it
docker compose run --rm run python -m ddd_python.ddd_dbt.dbt_build_with_unique_logfile
docker compose start metabase
```

Dagster jobs do the stop/start themselves. When you run dbt by hand, stop
Metabase and close DBeaver or `scripts/duckdb_connect.sh` sessions first. In
DuckLake mode the catalog file has the same rule.

## 2. Metabase left stopped

`start_metabase_asset` runs only when every step succeeded; for a failed run the
failure sensor restarts Metabase. It stays down when that restart failed (Docker
socket permissions, sensor daemon not running) or the run was cancelled.

```bash
docker compose start metabase
docker compose logs dagster | grep "Could not restart Metabase"
curl -s http://localhost:3001/api/health      # Metabase listens on 3001
```

## 3. S3 bucket missing or credentials wrong

Buckets must exist before the first extraction (`ddd-bronze`, plus
`ddd-ducklake` for DuckLake and `ddd-delta` for an S3 export). Create them in the
MinIO console (<http://localhost:9001>) or with `mc mb`; see
[storage_combinations.md](storage_combinations.md#minio-for-local-development).

For credential errors, check that the container sees the values and that MinIO
uses path-style URLs:

```bash
docker compose run --rm run bash -c 'echo "$S3_ENDPOINT $S3_URL_STYLE $S3_ACCESS_KEY_ID"'
mc alias set test http://localhost:9000 "$S3_ACCESS_KEY_ID" "$S3_SECRET_ACCESS_KEY" && mc ls test
```

## 4. Bronze is empty

Bronze views read whatever files are at `DANISH_DEMOCRACY_DATA_SOURCE` /
`RFAM_DATA_SOURCE`. Check that files exist there, and run an extraction if not:

```bash
ls /data/local/Files/Bronze/DDD/ /data/local/Files/Bronze/RFAM/
docker compose run --rm run python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data
docker compose run --rm run python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_rfam
```

In Docker the read paths come from `docker-compose.yml` (the container mount),
not from `.env`; for S3 set `DANISH_DEMOCRACY_DATA_SOURCE_DOCKER` and
`RFAM_DATA_SOURCE_DOCKER`.

## 5. Models missing after adding an entity

Bronze and Silver model files are generated from `configuration_variables.py`:

```bash
docker compose run --rm run python -m ddd_python.ddd_dbt.generate_dbt_models
ls dbt/models/bronze/bronze_ddd_<entity>*.sql dbt/models/silver/silver_ddd_<entity>*.sql
```

Gold is not generated per entity; add a dimension or fact by hand if needed.

## 6. DuckLake catalog misconfigured

With `SILVER_STORAGE_FORMAT=ducklake`, `DUCKLAKE_CATALOG_LOCATION` must point to
a writable path (by convention next to the `.duckdb` file) and, for local data,
`DUCKLAKE_DATA_PATH` to an existing directory:

```bash
SILVER_STORAGE_FORMAT=ducklake
DUCKLAKE_CATALOG_LOCATION=/data/duckdb/ducklake_catalog.ducklake
DUCKLAKE_DATA_PATH=/data/ducklake
```

The catalog file itself is created on the first attach.

## 7. Silver needs a rebuild

A `--full-refresh` drops each model's `_last_file` bookmark and rebuilds Silver
from all Bronze files. For **incremental** entities it also marks as deleted
every key that is missing from the newest Bronze file, so **run a full
extraction first** or the 31-day incremental file turns most records into `D`
rows:

```bash
# 1. complete snapshot of the incremental entities
docker compose run --rm run python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data --date_to_load_from 1900-01-01
docker compose run --rm run python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_rfam --date_to_load_from 1900-01-01
# 2. rebuild Silver (Metabase stopped)
docker compose run --rm run bash -c "cd /app/dbt && dbt build --select tag:silver --full-refresh --profiles-dir ."
```

In Dagster: run the extraction jobs with `date_to_load_from` set, then
`dbt_silver_job` with `full_refresh: true`.

**If a refresh already produced false deletes,** run the full extraction (step 1)
and then a normal Silver run. Every falsely deleted key that is in the complete
new file gets an `I` row, so `_cv` and Gold are correct again. The false `D` and
the `I` after it stay in the history; repeating the full refresh keeps them,
because `D` rows are carried forward.

To remove them from the history as well, restore the `duckdb` (or `ducklake`)
backup from before the refresh, or delete the `D` rows that run wrote (they all
carry its start time in `LKHS_date_inserted`) and run the full refresh again;
the rebuild then drops the `I` rows that followed them:

```sql
DELETE FROM main_silver.silver_ddd_aktoer
WHERE LKHS_cdc_operation = 'D' AND LKHS_date_inserted = TIMESTAMP '<run start>';
```

To inspect a bookmark:

```sql
SELECT * FROM main_silver.silver_ddd_aktoer_last_file;   -- ducklake_catalog.main_silver… in DuckLake mode
```

## 8. No ntfy.sh alerts

Alerts are opt-in: set `NTFY_TOPIC` to a topic name (no URL prefix). Test the
topic, then check the sensor log:

```bash
curl -d "test from DDD" https://ntfy.sh/<your-topic>
docker compose logs dagster | grep -i ntfy
```

## 9. Source freshness warning

The `freshness:` blocks in `dbt/models/bronze/__sources*.yml` warn after
`DBT_FRESHNESS_WARN_AFTER_DAYS` (default 2) and error after
`DBT_FRESHNESS_ERROR_AFTER_DAYS` (default 7) without new Bronze files. On a
development machine this is expected; in production check that
`danish_parliament_full_pipeline_schedule` is enabled (schedules ship stopped).

---

## Recovery commands

### Metabase

```bash
scripts/stop_metabase_and_wait.sh [seconds]    # docker stop + wait (default 120 s)
scripts/start_metabase_and_wait.sh [seconds]   # docker start + wait (default 120 s)
docker compose logs -f metabase | grep -i error
```

### dbt

```bash
docker compose run --rm run bash -c "cd /app/dbt && dbt build --select silver_ddd_aktoer --profiles-dir ."
docker compose run --rm run bash -c "cd /app/dbt && dbt test --profiles-dir ."
jq -c '.info | {ts, level, msg}' "$(ls -t /data/dbt_logs/dbt_build_log_*.json | head -1)" | tail -20
```

### DuckDB

```bash
duckdb -readonly /data/duckdb/danish_democracy_data.duckdb   # or scripts/duckdb_connect.sh
```

```sql
SELECT LKHS_cdc_operation, COUNT(*) FROM main_silver.silver_ddd_aktoer GROUP BY ALL;
SELECT COUNT(*) FROM main_silver.silver_ddd_aktoer_cv;
SELECT * FROM delta_scan('/data/local/Files/Silver/silver_ddd_aktoer') LIMIT 5;  -- local export
```

### Extraction and export

```bash
# one DDD entity from a given date
docker compose run --rm run python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data \
    --date_to_load_from 2026-01-01 --file_names_to_retrieve Stemme
# Delta export: Silver appends new rows, Gold overwrites
docker compose run --rm run python -m ddd_python.ddd_dlt.export_silver
docker compose run --rm run python -m ddd_python.ddd_dlt.export_gold
```

### Backups

```sql
SELECT * FROM read_json_auto('/data_backup/logs/backup_log_*.ndjson')
ORDER BY run_started_at DESC LIMIT 5;
```

Backup and restore commands are in
[docker/DOCKER_USAGE.md](../docker/DOCKER_USAGE.md#backup-and-restore).

### Docker

```bash
docker compose exec dagster bash        # shell in the running Dagster container
docker compose run --rm run bash        # throw-away shell
docker compose down                     # stop everything; data is on host bind mounts under /data
stat -c '%a %u:%g %n' /data/duckdb /data/dlt_pipelines /data/dagster /data/local
```
