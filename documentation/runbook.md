# Operator Runbook

A quick reference for troubleshooting the Danish Democracy Data pipeline. This document covers the most common failure scenarios, what causes them, and how to fix them.

Last updated: July 2026

## First Things to Check

When a Dagster job fails or the pipeline misbehaves, start here (in order):

1. **Check the Dagster run status.** Go to http://localhost:3000 (Dagster UI), find the failed job, and read the step-by-step output. Look for the exact error message in the logs.
2. **Check for an ntfy.sh alert.** If `NTFY_TOPIC` is set in `.env`, you should have received a push notification with the job name and run ID. This confirms the sensor fired.
3. **Read the dbt log file.** If the failure is in `dbt build`, check `DBT_LOGS_DIRECTORY` (default: `data/dbt_logs/`) for a timestamped `dbt_build_log_*.json`. Use `jq` to pretty-print: `cat dbt_build_log_YYYYMMDD_HHMMSS.json | jq '.message'` to see the error.
4. **Check dlt extraction logs.** If extraction (Danish Parliament or Rfam) failed, look in `DLT_PIPELINE_RUN_LOG_DIR` (default: `data/logs/DDD/` or `data/logs/RFAM/`) for NDJSON run summaries. Query with DuckDB: `SELECT * FROM read_json_auto('data/logs/DDD/*.ndjson') ORDER BY timestamp DESC LIMIT 5`.
5. **Verify Docker container status.** Run `docker compose ps` to check if the `dagster`, `metabase`, or `run` container is stuck. Look for unhealthy or exited states.

## Failure Scenarios

### 1. DuckDB file is locked

**Symptom:** `dbt build` fails with `Cannot acquire exclusive lock on database file` or similar. The pipeline hangs during the dbt job.

**Cause:** Metabase, DBeaver, or another DuckDB connection holds a read lock on the `.duckdb` file. DuckDB is single-writer — only one process may hold a write lock at a time. Any reader blocks writers.

**Fix:**

1. Stop Metabase and close all external connections:
   ```bash
   docker compose stop metabase
   # Or manually:
   scripts/stop_metabase_and_wait.sh
   ```

2. If you're connected via DBeaver or `duckdb_connect.sh`, close those sessions.

3. Verify the lock is released:
   ```bash
   lsof | grep danish_democracy_data.duckdb
   # If any process appears, kill it: kill -9 <PID>
   ```

4. Retry the dbt job:
   ```bash
   docker compose run --rm run ddd_python.ddd_dbt.dbt_build_with_unique_logfile
   ```

5. Restart Metabase when dbt finishes:
   ```bash
   docker compose start metabase
   # Or:
   scripts/start_metabase_and_wait.sh
   ```

**Prevention:**

- The Dagster `full_pipeline_job` includes `stop_metabase_asset` before dbt runs and `start_metabase_asset` after. If you run dbt manually, stop Metabase first.
- In DuckLake mode (`SILVER_STORAGE_FORMAT=ducklake`), the `.ducklake` catalog file is also single-writer. Close all connections before running dbt.

---

### 2. Metabase left stopped after dbt failure

**Symptom:** Metabase is offline after a pipeline run. The `start_metabase_asset` step did not execute because the dbt job crashed.

**Cause:** If dbt fails mid-run, the Dagster job terminates early. The `stop_metabase_asset` ran, but `start_metabase_asset` (which comes after dbt) never ran. Metabase is stuck offline.

**Fix:**

1. Restart Metabase manually:
   ```bash
   docker compose start metabase
   # Or wait longer:
   scripts/start_metabase_and_wait.sh 180
   ```

2. Verify Metabase is healthy:
   ```bash
   docker compose logs metabase | tail -20
   # Check for "Metabase initialization complete" message.
   ```

3. Confirm the UI is reachable at http://localhost:3000.

**Prevention:**

- Always monitor Dagster job status. If a job fails, check that Metabase comes back online.
- Run `docker compose ps` to see the state: if `metabase` shows `Exited`, restart it immediately.

---

### 3. S3 bucket does not exist

**Symptom:** Extraction or dbt fails with `NoSuchBucket`, `404 Not Found`, or `Access Denied` when writing Bronze files or DuckLake Parquet data. The error appears in `dlt` or `dbt` logs.

**Cause:** When using `RAW_STORAGE_TARGET=s3` (MinIO or AWS S3), the bucket must exist before extraction runs. If you forgot to create it, writes fail.

**Fix:**

**For MinIO (local dev):**

1. Open the MinIO web UI at http://localhost:9001 (default credentials: minioadmin / minioadmin).
2. Click **Object Browser** → **+** button.
3. Enter the bucket name (e.g., `ddd-bronze`, `ddd-ducklake`) and click **Create Bucket**.
4. Retry the extraction:
   ```bash
   docker compose run --rm run ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data
   ```

**For AWS S3:**

1. Use the AWS CLI or AWS console to create the bucket:
   ```bash
   aws s3 mb s3://ddd-bronze --region us-east-1
   aws s3 mb s3://ddd-ducklake --region us-east-1  # if using DuckLake
   ```

2. Verify the bucket exists:
   ```bash
   aws s3 ls | grep ddd-bronze
   ```

3. Retry extraction.

**Prevention:**

- Before switching to S3 storage, create the required buckets.
- For MinIO, write a setup script that creates buckets automatically when the container starts.
- Document the bucket names in your deployment runbook.

---

### 4. `generate_dbt_models.py` not run after adding an entity

**Symptom:** `dbt build` fails with `"model not found"` or `"Compilation Error in file dbt/models/bronze/ddd/…"`. You added a new entity to `DANISH_DEMOCRACY_FILE_NAMES` in `configuration_variables.py` but dbt cannot find the model.

**Cause:** dbt models are auto-generated by reading `configuration_variables.py` and instantiating Jinja macros. If you add a new entity without regenerating the models, the SQL files don't exist.

**Fix:**

1. Run the model generator:
   ```bash
   docker compose run --rm run ddd_python.ddd_dbt.generate_dbt_models
   ```

2. Verify the SQL files were created:
   ```bash
   ls dbt/models/bronze/ddd/{new_entity_name}*.sql
   ls dbt/models/silver/ddd/{new_entity_name}*.sql
   ls dbt/models/gold/ddd_{new_entity_name}*.sql
   ```

3. Retry dbt build:
   ```bash
   docker compose run --rm run ddd_python.ddd_dbt.dbt_build_with_unique_logfile
   ```

**Prevention:**

- Always run `generate_dbt_models` after updating `configuration_variables.py`.
- Add a pre-commit hook or CI check that detects changes to `configuration_variables.py` and warns if `dbt/models/` is not updated.

---

### 5. Bronze files not found

**Symptom:** Bronze views return no rows or fail with `"File not found"`. You run `dbt build` and the Bronze layer has zero data.

**Cause:** The extraction never ran (you skipped that step), or the `DANISH_DEMOCRACY_DATA_SOURCE` / `RFAM_DATA_SOURCE` path is wrong and doesn't point to the actual extracted files.

**Fix:**

1. Verify the data source path in `.env`:
   ```bash
   grep "DANISH_DEMOCRACY_DATA_SOURCE\|RFAM_DATA_SOURCE" .env
   ```

2. Check if extracted files exist at that location:
   ```bash
   # Local mode:
   ls data/Files/Bronze/DDD/
   ls data/Files/Bronze/RFAM/
   
   # S3 mode (MinIO web UI or AWS CLI):
   aws s3 ls s3://ddd-bronze/Files/Bronze/DDD/
   ```

3. If files don't exist, run extraction:
   ```bash
   docker compose run --rm run ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data
   docker compose run --rm run ddd_python.ddd_dlt.dlt_run_extraction_pipelines_rfam
   ```

4. Re-run dbt:
   ```bash
   docker compose run --rm run ddd_python.ddd_dbt.dbt_build_with_unique_logfile
   ```

**Prevention:**

- Always extract before transforming. The standard pipeline order is: extraction → model generation → dbt build.
- Test that Bronze views are non-empty as part of your health checks: `SELECT COUNT(*) FROM bronze_ddd_afstemning LIMIT 1`.

---

### 6. DuckLake catalog path misconfigured

**Symptom:** `dbt build` fails with `"Extension 'ducklake' not found"` or `"Catalog Error: Cannot find catalog file"` when `SILVER_STORAGE_FORMAT=ducklake`. The error is cryptic and happens inside the DuckDB extension.

**Cause:** The `DUCKLAKE_CATALOG_LOCATION` environment variable is unset or points to a non-existent directory. DuckDB cannot attach the DuckLake catalog database.

**Fix:**

1. Check the `.env` file:
   ```bash
   grep DUCKLAKE_CATALOG_LOCATION .env
   ```

2. If it's missing or empty, set it. The catalog file should live in the same directory as the `.duckdb` file:
   ```bash
   DUCKLAKE_CATALOG_LOCATION=/path/to/duckdb/.ducklake
   ```
   Example:
   ```bash
   DUCKDB_DATABASE_LOCATION=duckdb/danish_democracy_data.duckdb
   DUCKLAKE_CATALOG_LOCATION=duckdb/.ducklake
   ```

3. Ensure the directory exists:
   ```bash
   mkdir -p duckdb
   ```

4. Retry dbt:
   ```bash
   docker compose run --rm run ddd_python.ddd_dbt.dbt_build_with_unique_logfile
   ```

**Prevention:**

- When switching to DuckLake mode, add these to `.env` before running any dbt:
  ```
  SILVER_STORAGE_FORMAT=ducklake
  DUCKLAKE_CATALOG_LOCATION=duckdb/.ducklake
  ```
- Check that both paths are absolute or relative to the same working directory.

---

### 7. Incremental watermark is wrong

**Symptom:** Silver picks up the wrong date range or misses recent data. The `_last_file` tracking table has an old watermark. You suspect incremental extraction is loading stale data.

**Cause:** The `_last_file` table stores the last-processed filename (which encodes a timestamp). If it's corrupted, incremental Silver models will re-process data from the wrong starting point.

**Fix:**

1. Inspect the watermark:
   ```bash
   duckdb duckdb/danish_democracy_data.duckdb << SQL
   SELECT table_name, last_file_timestamp FROM main_silver.*_last_file
   ORDER BY table_name;
   SQL
   ```

2. If the date is clearly wrong, reset it with a full-refresh:
   ```bash
   docker compose run --rm run ddd_python.ddd_dbt.dbt_build_with_unique_logfile --models_to_select tag:silver
   # Add --full-refresh to rebuild from scratch:
   docker compose run --rm run bash -c "cd /dbt && dbt build --select tag:silver --full-refresh"
   ```

3. After the refresh, verify:
   ```bash
   duckdb duckdb/danish_democracy_data.duckdb << SQL
   SELECT table_name, COUNT(*) as rows FROM main_silver.silver_ddd_aktoer_cv
   GROUP BY table_name;
   SQL
   ```

**Prevention:**

- Monitor the `_last_file` watermarks as part of your health checks (daily queries).
- If you suspect data loss, run a periodic full-refresh of Silver (e.g., monthly) to reset the watermark and re-process all Bronze files.

---

### 8. Delta export fails with S3 credentials error

**Symptom:** `export_silver` or `export_gold` fails with `InvalidAccessKeyId`, `SignatureDoesNotMatch`, or `credentials missing` when writing Delta tables to S3.

**Cause:** The S3 credentials in `.env` are wrong, missing, or the environment variables are not being passed to the export container.

**Fix:**

1. Check the `.env` file for S3 credentials:
   ```bash
   grep "S3_ACCESS_KEY_ID\|S3_SECRET_ACCESS_KEY\|S3_ENDPOINT" .env
   ```

2. Verify they match your MinIO or AWS account:
   - **MinIO:** credentials are usually `minioadmin` / `minioadmin` by default, endpoint is `http://minio:9000`.
   - **AWS:** access key and secret key from your AWS IAM user, endpoint is empty (AWS default).

3. Test the credentials manually:
   ```bash
   # For MinIO:
   mc alias set test http://minio:9000 minioadmin minioadmin
   mc ls test
   
   # For AWS:
   aws s3 ls --profile your-profile
   ```

4. If credentials are correct, ensure they're in `.env` (not commented out) and the `.env` file is loaded:
   ```bash
   docker compose run --rm run bash -c "echo \$S3_ACCESS_KEY_ID"
   # Should print your key, not empty
   ```

5. Retry the export:
   ```bash
   docker compose run --rm run ddd_python.ddd_dlt.export_silver
   docker compose run --rm run ddd_python.ddd_dlt.export_gold
   ```

**Prevention:**

- Test S3 connectivity before running the full pipeline: `mc ls` or `aws s3 ls`.
- Store credentials securely (not in git). Use `.env` and add `.env` to `.gitignore`.

---

### 9. ntfy.sh alerts not firing

**Symptom:** A Dagster job fails but you don't receive a push notification. The ntfy.sh endpoint is not being called.

**Cause:** Either `NTFY_TOPIC` is not set in `.env`, or the ntfy.sh service is unreachable. Alerts are intentionally opt-in.

**Fix:**

1. Check if `NTFY_TOPIC` is set:
   ```bash
   grep NTFY_TOPIC .env
   ```

2. If empty or missing, set it to a topic name (no `https://ntfy.sh/` prefix):
   ```bash
   echo "NTFY_TOPIC=my-ddd-alerts" >> .env
   ```

3. Test the ntfy.sh endpoint manually:
   ```bash
   curl -X POST -d "Test alert from DDD pipeline" \
     -H "Title: DDD Test" \
     https://ntfy.sh/my-ddd-alerts
   ```
   You should receive a notification on your phone within seconds.

4. If the curl test works but Dagster alerts still don't fire, check the Dagster sensor logs:
   ```bash
   docker compose logs -f dagster | grep ntfy
   ```

5. Re-run the job that failed, or trigger a new one to test the sensor.

**Prevention:**

- Set `NTFY_TOPIC` and `ENVIRONMENT` in `.env` before deploying.
- Test the notification flow manually after setup: trigger a small job and verify you receive an alert.

---

### 10. dbt source freshness warning

**Symptom:** `dbt build` or `dbt freshness` prints a warning: `"Source DDD has not been updated in N days"`. The job continues, but a warning is issued.

**Cause:** The source freshness check (`dbt_freshness_*` in `dbt_project.yml`) detected that Bronze files haven't been updated beyond the threshold. This is a diagnostic warning, not a failure — intentionally non-blocking.

**Fix:**

1. Check the freshness thresholds in `dbt_project.yml`:
   ```bash
   grep "DBT_FRESHNESS" dbt/dbt_project.yml
   # Or check .env:
   grep "DBT_FRESHNESS" .env
   ```
   Default: warn at 2 days, error at 7 days.

2. Verify Bronze is actually recent:
   ```bash
   ls -ltr data/Files/Bronze/DDD/*/  # Check file timestamps
   ```

3. If Bronze is stale, trigger extraction:
   ```bash
   docker compose run --rm run ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data
   ```

4. Re-run dbt. If the warning persists, it means Bronze data is genuinely old — this is expected in non-prod environments or if the schedule is disabled.

**When to ignore:**

- In development, freshness warnings are expected if you haven't run extraction recently.
- On production systems, freshness warnings should trigger an alert (integrate with your monitoring).

**Prevention:**

- Ensure the extraction schedule is enabled: check `SCHEDULE_ENABLED_FULL_PIPELINE_JOB` in `.env`.
- Periodically run `dbt source freshness` to verify extraction is keeping up with your SLA.

---

## Manual Recovery Commands

These are the most useful one-liners for quick recovery or debugging.

### Metabase

```bash
# Start Metabase (waits 120 seconds for initialization)
scripts/start_metabase_and_wait.sh 120

# Stop Metabase (waits 120 seconds for locks to clear)
scripts/stop_metabase_and_wait.sh 120

# View Metabase logs
docker compose logs -f metabase | grep -i error

# Connect to Metabase HTTP API (list running instances)
curl -s http://localhost:3000/api/session | jq .
```

### dbt

```bash
# Run a single dbt model
docker compose run --rm run bash -c "cd /dbt && dbt build --select silver_ddd_aktoer"

# Run a full Silver refresh (resets incremental watermarks)
docker compose run --rm run bash -c "cd /dbt && dbt build --select tag:silver --full-refresh"

# Run tests only (no model builds)
docker compose run --rm run bash -c "cd /dbt && dbt test"

# Inspect a dbt JSON log file
cat data/dbt_logs/dbt_build_log_YYYYMMDD_HHMMSS.json | jq '.[] | select(.message_type == "run_status")' | head -20

# Parse all errors from the latest dbt run
jq '.[] | select(.level == "error") | .message' data/dbt_logs/dbt_build_log_*.json | tail -1
```

### DuckDB

```bash
# Connect to DuckDB CLI (local mode, no Azure credentials)
duckdb duckdb/danish_democracy_data.duckdb

# Within DuckDB, query Bronze row counts by entity
SELECT table_name, COUNT(*) as rows
FROM (
  SELECT 'bronze_ddd_aktoer' as table_name FROM bronze_ddd_aktoer
  UNION ALL
  SELECT 'bronze_ddd_moede' FROM bronze_ddd_moede
  LIMIT 1  -- just check existence
)
GROUP BY table_name;

# Check Silver CDC operation counts (insert/update/delete)
SELECT LKHS_cdc_operation, COUNT(*) as count
FROM silver_ddd_aktoer
GROUP BY LKHS_cdc_operation;

# View the incremental watermark for an entity
SELECT * FROM main_silver.silver_ddd_aktoer_last_file;

# Check DuckLake catalog status (if SILVER_STORAGE_FORMAT=ducklake)
LOAD ducklake;
ATTACH 'duckdb/.ducklake' AS ducklake_catalog;
SELECT table_name, COUNT(*) as rows FROM ducklake_catalog.main_silver.silver_ddd_aktoer;
```

### Bronze / Extraction

```bash
# List extracted Bronze files by entity (local mode)
find data/Files/Bronze/DDD -name "*.json" -o -name "*.parquet" | sort -r | head -20

# Check Bronze file timestamps
ls -lhtr data/Files/Bronze/DDD/{entity_name}/

# Count rows in a single Bronze file
duckdb duckdb/danish_democracy_data.duckdb << SQL
SELECT COUNT(*) FROM read_json_auto('data/Files/Bronze/DDD/{entity}/*.json');
SQL

# Re-run extraction for a single entity (DDD only)
docker compose run --rm run bash -c "cd /dbt && python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data --file_names {entity_name}"
```

### Export

```bash
# Inspect Delta Lake tables (local mode)
duckdb duckdb/danish_democracy_data.duckdb << SQL
SELECT * FROM delta_scan('data/Files/Silver/{entity_name}') LIMIT 5;
SQL

# Re-export Silver (incremental append)
docker compose run --rm run ddd_python.ddd_dlt.export_silver

# Re-export Gold (full overwrite)
docker compose run --rm run ddd_python.ddd_dlt.export_gold
```

### Logs and Diagnostics

```bash
# Query dlt extraction logs (NDJSON format)
duckdb << SQL
SELECT * FROM read_json_auto('data/logs/DDD/*.ndjson')
ORDER BY timestamp DESC LIMIT 10;
SQL

# Query backup logs (if backups have been run)
duckdb << SQL
SELECT * FROM read_json_auto('/data_backup/logs/backup_log_*.ndjson')
ORDER BY run_started_at DESC LIMIT 5;
SQL

# Follow Dagster logs
docker compose logs -f dagster | tail -100

# Check if Metabase has any database connection errors
docker compose logs metabase 2>&1 | grep -i "connection\|error" | tail -20

# View the `.env` file (mask secrets with grep)
cat .env | grep -v "AZURE_CLIENT_SECRET\|S3_SECRET" | sort
```

### Docker Troubleshooting

```bash
# Force-stop all containers
docker compose down

# Start fresh (delete all data)
docker compose down -v

# Rebuild the application image
docker compose build

# Run a shell inside a running container
docker compose exec run bash

# Check permissions on bind-mounts
ls -la data/ /data_backup/
stat -c '%a %u:%g %n' data/duckdb data/dlt_pipelines
```
