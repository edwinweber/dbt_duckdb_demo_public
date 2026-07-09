# Daily Pipeline Execution: A Walkthrough

This document describes what happens during a typical 06:00 scheduled run of the Danish Democracy Data pipeline. Understanding the sequence helps you predict what will happen, interpret log output, and debug failures.

## The Two Daily Schedules

Two Dagster schedules are configured (both disabled by default; enable in the Dagster UI):

1. **Full Pipeline Job — 06:00 Europe/Copenhagen**
   - Extraction (all sources, both incremental and full-extract)
   - Bronze layer ingestion
   - Silver layer CDC transformation
   - Gold layer modeling
   - Delta Lake export (Silver and Gold)

2. **Data Engineering Job — 08:00 Europe/Copenhagen**
   - Reads Dagster's SQLite event logs from the 06:00 run
   - Materializes run summaries, asset timings, and failure details as dbt models
   - Exposes the pipeline's observability data for Metabase dashboards

The first run generates the operational data; the second consumes it. Both are wrapped in the same orchestration framework but represent different concerns.

## The 06:00 Full Pipeline Execution — Minute by Minute

The `full_pipeline_job` (defined in [ddd_dagster/jobs.py](../ddd_python/ddd_dagster/jobs.py)) orchestrates five stages. Here's the sequence:

### Stage 1: Metabase Stop (0:00–0:30)

**Asset:** `stop_metabase_asset`

The pipeline cannot proceed if Metabase holds a read connection to the DuckDB file. DuckDB's single-writer constraint prevents dbt from acquiring a write lock.

- Sends a `curl` request to the Metabase container's `/api/session` endpoint to stop the service gracefully.
- Waits up to 60 seconds for the container to stop.
- If Metabase is already stopped, proceeds immediately.
- If the stop times out, the asset fails and the entire pipeline stops (failure is loud, not silent).

**Why this matters:** This is not optional. If dbt runs while Metabase holds the lock, the run hangs indefinitely and times out. Stopping Metabase first prevents the hang.

**Log output:** Look for:
```
stop_metabase_asset completed successfully
```

### Stage 2: Extraction — Danish Parliament & Rfam (0:30–4:00 typical)

**Assets:** `danish_parliament_*_asset` (one per entity, 18 total) + `rfam_*_asset` (one per table, 7 total)

Both extraction jobs run in parallel (multiprocess executor, max 4 concurrent workers per job). dlt downloads JSON/Parquet files from the OData API and MySQL database.

#### Danish Parliament (DDD):

- **Incremental entities (6):** Aktør, Møde, Sag, Sagstrin, SagstrinAktør, Stemme
  - Filtered by `$filter=opdateringsdato ge DateTime('YYYY-MM-DD')`
  - Fetches only records modified since the last run
  - Typically 5–50 KB per run

- **Full-extract entities (12):** AfstemningType, Afstemning, Aktørtype, etc.
  - No date filter; always fully loaded
  - Enables clean delete detection (records absent from the full snapshot are treated as deletes)
  - Typically 1–10 KB each

#### Rfam:

- **Incremental tables (2):** family (pk `rfam_acc`, date `updated`), genome (pk `upid`, date `updated`)
  - SQL query with `WHERE updated >= CAST(? AS DATE)` clause
  - Fetches only rows modified since the last run

- **Full-extract tables (5):** clan, clan_membership, author, literature_reference, dead_family
  - No date filter; always fully loaded

**Parallelism:** The `dlt_run_extraction_pipelines_danish_parliament_data` and `dlt_run_extraction_pipelines_rfam` scripts each spawn `ThreadPoolExecutor(max_workers=4)`. Four Danish Parliament entities download simultaneously; four Rfam rows stream simultaneously. If one extraction fails, the orchestration collects all task outcomes, reports which succeeded and which failed, and raises a `RuntimeError` only after all tasks have attempted.

**Failure semantics:** If any extraction fails (network timeout, API error, malformed response), the entire extraction stage fails. dbt never runs. Silver tables remain in their previous state — no partial data is committed.

**Log output:** Look for NDJSON lines in `DLT_PIPELINE_RUN_LOG_DIR`:
```json
{"pipeline":"DDD","entity":"Aktør","rows_loaded":1234,"duration_sec":2.5,"status":"success"}
{"pipeline":"Rfam","table":"family","rows_loaded":567,"duration_sec":3.1,"status":"success"}
```

**Source code:** [ddd_dlt/dlt_pipeline_execution_functions.py](../ddd_python/ddd_dlt/dlt_pipeline_execution_functions.py), [ddd_dlt/dlt_run_extraction_pipelines_danish_parliament_data.py](../ddd_python/ddd_dlt/dlt_run_extraction_pipelines_danish_parliament_data.py), [ddd_dlt/dlt_run_extraction_pipelines_rfam.py](../ddd_python/ddd_dlt/dlt_run_extraction_pipelines_rfam.py)

### Stage 3: Transformation — Bronze, Silver, Gold (4:00–6:30 typical)

**Asset:** `dbt_full_pipeline_asset` (wraps `dbt build` on all three layers + seeds)

Now that all Bronze files are written, dbt begins the transformation. This stage is fully serialized (no parallel tasks) because of the single-writer constraint.

- **Pre-phase:** dbt seeds (load reference data from CSVs).
- **Bronze models (views, <1 min):** Compile and materialize `bronze_ddd_*` and `bronze_rfam_*` views. These are read-only views over the JSON/Parquet files with `read_json_auto` / `read_parquet`.
- **Silver models (tables, CDC, 1–3 min typical):** For each entity:
  - Pre-hook: Track which Bronze files were already processed (`_last_file` table).
  - Model: Detect inserts/updates/deletes via SHA256 hash comparison. For incremental runs, only new Bronze files are scanned. For `--full-refresh`, all Bronze files are re-processed.
  - Post-hook: Update the `_last_file` table with the new watermark.
  - Result: One immutable Silver table per entity, with SCD Type 2 history (every version of every row, with validity timestamps).
  - Companion `_cv` view: Exposes only the current-version rows per key.
- **Gold models (views, <1 min):** Compile and materialize star-schema views (facts and dimensions) that join Silver `_cv` views.

**Failure semantics:** If dbt fails (CDC macro exception, data-quality test failure), the entire stage fails. Silver tables may be partially updated (depending on which models completed). Exports don't run.

**Log output:** dbt writes a JSON logfile (timestamped, uploaded to OneLake if configured). Look for:
```
Running 18 Bronze models ✓
Running 25 Silver models (CDC) ✓
Running 8 Gold models ✓
22 tests passed ✓
Completed in 2m 45s
```

**Source code:** [ddd_dbt/dbt_build_with_unique_logfile.py](../ddd_python/ddd_dbt/dbt_build_with_unique_logfile.py), [dbt/models/bronze](../dbt/models/bronze), [dbt/models/silver](../dbt/models/silver), [dbt/models/gold](../dbt/models/gold)

### Stage 4: Delta Lake Export (6:30–7:00 typical)

**Assets:** `export_silver_asset`, `export_gold_asset`

Silver and Gold assets are exported as Delta Lake tables. These run sequentially (export_silver completes before export_gold starts) but are still independent of dbt's output — they read from the transformed DuckDB tables.

#### Silver Export (Incremental Append):

- Reads the target Delta table (if it exists) via `delta_scan('<target>')` inside DuckDB — no Python materialization.
- Anti-joins to find Silver rows not yet exported: `SELECT * FROM silver_ddd_aktoer WHERE (id, LKHS_date_valid_from) NOT EXISTS IN delta_scan(...)`.
- Appends only the new rows to the Delta table.
- **Storage target:** Local filesystem (`data/Files/Silver/`) or Microsoft Fabric OneLake (`abfss://...`), selected by `STORAGE_TARGET` env var.
- **Idempotency:** If the export runs twice with the same data, the second run appends no rows (the anti-join returns empty). Safe to retry.

#### Gold Export (Full Overwrite):

- Reads all Gold view data into PyArrow as a table.
- Overwrites the target Delta table completely.
- **Rationale:** Gold is a derived view, not incremental. The safest semantics are full overwrite. If Gold logic changes, the next export reflects the new logic.
- **Storage target:** Same as Silver (local or OneLake).

**Failure semantics:** If Silver export fails (Delta write error, bad credentials), export stops. Gold doesn't export. Silver stays read-only (the delta_scan read succeeds, but append fails). Delta Lake tables stay unchanged.

**Log output:** Look for NDJSON:
```json
{"export":"silver","source":"DuckDB","rows_appended":1234,"duration_sec":5.2,"status":"success"}
{"export":"gold","source":"DuckDB","rows_written":567,"duration_sec":2.1,"status":"success"}
```

**Source code:** [ddd_dlt/export_silver.py](../ddd_python/ddd_dlt/export_silver.py), [ddd_dlt/export_gold.py](../ddd_python/ddd_dlt/export_gold.py)

### Stage 5: Metabase Start (7:00–7:30)

**Asset:** `start_metabase_asset`

Metabase is restarted so BI users can query the updated data.

- Sends a Docker `start` signal to the Metabase container.
- Waits up to 60 seconds for the container to be ready (polls the HTTP health endpoint).
- If Metabase is already running, proceeds immediately.

**Log output:** Look for:
```
start_metabase_asset completed successfully
```

---

## Run-Status Sensors (Observability)

After the `full_pipeline_job` completes (success or failure), two **run-status sensors** fire:

1. **`danish_parliament_run_success_sensor`** (if the full job succeeded)
2. **`danish_parliament_run_failure_sensor`** (if the full job failed)

Both sensors:
- Write an NDJSON summary to the log directory (locally or OneLake): job name, run ID, start/end time, asset counts, status.
- Send a push notification via [ntfy.sh](https://ntfy.sh) if `NTFY_TOPIC` is set.

**Notification format:**

| Field | SUCCESS | FAILURE |
|-------|---------|---------|
| Title | `Dagster run SUCCEEDED - full_pipeline_job` | `Dagster run FAILED - full_pipeline_job` |
| Priority | `default` | `high` |
| Tag | ✅ | 🚨 |
| Body | `Job: full_pipeline_job\nRun ID: abc12345...\nEnvironment: PROD` | Same |

**Opt-in:** Alerts are silently skipped if `NTFY_TOPIC` is not set. No error is raised; the notification failure never blocks the pipeline.

**Source code:** [ddd_dagster/sensors.py](../ddd_python/ddd_dagster/sensors.py)

---

## The 08:00 Data Engineering Job

Two hours after the full pipeline, the `data_engineering_job` runs. It reads the Dagster SQLite logs from the 06:00 run and materializes observability models:

- **`asset_runs`** — When did each asset start/end? How long did it take?
- **`job_runs`** — What was the overall job status? Did any steps fail?
- **`asset_lineage`** — Which assets depend on which? (Derived from `dbt_dependencies` seed)
- **Failure summaries** — If any asset failed, what was the error?

These tables are then read by Metabase dashboards, giving data engineers visibility into pipeline health without a separate observability stack.

**Source code:** [ddd_dagster/definitions.py](../ddd_python/ddd_dagster/definitions.py) (schedule definition), [dbt/models/data_engineering](../dbt/models/data_engineering) (observability models)

---

## Troubleshooting a Failed Run

If the 06:00 run fails, the sequence stops at the failing stage. Here's how to diagnose:

1. **Check Metabase stop:** Did `stop_metabase_asset` complete? If not, Metabase may still be running; stop it manually (`docker stop metabase`).

2. **Check extraction:** Look at NDJSON logs in `DLT_PIPELINE_RUN_LOG_DIR`. Which entities succeeded? Which failed? Common causes:
   - Network timeouts (API rate limiting, temporary outage)
   - Malformed response (API change, encoding error)
   - Storage credentials invalid (OneLake mode)

3. **Check dbt:** Look at the dbt JSON logfile. Which models failed? Common causes:
   - Data-quality test failure (unexpected NULL, cardinality change)
   - Schema change (new column at source, not inferred by `read_json_auto`)
   - CDC macro error (hash collision, surrogate key conflict — rare)

4. **Check export:** If dbt passed but export failed, look at the Delta export logs. Common causes:
   - OneLake credentials expired
   - Partition format mismatch
   - Delta table schema incompatible (rare; deltalake + PyArrow handle evolution)

5. **Retry:** Most failures are transient (network timeout, API rate limit). Rerun the job. Extraction is idempotent (dlt deduplicates by file name); dbt is idempotent (re-running CDC macros produces identical results); export is idempotent (Silver anti-join deduplicates, Gold full-overwrites).

For persistent failures, open an issue with the log details.

---

## Related Documentation

- [python_code_explained.md](python_code_explained.md) — how each Python module works
- [silver_model_logic.md](silver_model_logic.md) — the CDC/SCD2 SQL in detail
- [design_decisions.md](design_decisions.md) — why we chose this execution strategy (see ADR-5)
- [CLAUDE.md](../CLAUDE.md#running-the-project) — how to manually trigger jobs outside the schedule
