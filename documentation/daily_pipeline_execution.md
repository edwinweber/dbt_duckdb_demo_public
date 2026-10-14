# Daily Pipeline Execution: A Walkthrough

This document describes what happens during the scheduled 06:00 run of the
Danish Democracy Data pipeline: which assets run in which order, what each step
writes, and where to look when something fails. Everything here is derived from
[`ddd_dagster/jobs.py`](../ddd_python/ddd_dagster/jobs.py),
[`schedules.py`](../ddd_python/ddd_dagster/schedules.py) and the asset modules.

## The Two Daily Schedules

Both schedules are defined in `schedules.py`, run in the `Europe/Copenhagen`
timezone and ship **stopped** (`DefaultScheduleStatus.STOPPED`); enable them in
the Dagster UI.

| Schedule | Time | Job | What it does |
| --- | --- | --- | --- |
| `danish_parliament_full_pipeline_schedule` | 06:00 | `full_pipeline_job` | Extraction → Bronze → Silver → Gold → Delta export → observability layer |
| `dbt_data_engineering_schedule` | 08:00 | `dbt_data_engineering_job` | Rebuilds only the observability layer, so the 06:00 run's own outcome (written to Dagster's run storage after it ends) is included |

`full_pipeline_job` already refreshes the observability layer as its last dbt
step, but that step runs *inside* the run it is describing. The 08:00 job picks
up the completed 06:00 run, including its final status and any step failures.

## The 06:00 Full Pipeline

`full_pipeline_job` selects every ingestion group, the Bronze/Silver/Gold dbt
assets, both export groups and the data-engineering models, wrapped by
`_with_metabase_control()` in `stop_metabase_asset` / `start_metabase_asset`.
It uses `in_process_executor`, so **every step runs sequentially in one
process**, because DuckDB allows only one writer: no two assets in this job
compete for the `.duckdb` file.

dbt seeds are **not** part of this job; they change rarely and are loaded with
`dbt_seeds_job`.

### Step 1 — Stop Metabase

**Asset:** `stop_metabase_asset`
([`metabase_control_assets.py`](../ddd_python/ddd_dagster/metabase_control_assets.py))

Metabase keeps a read connection open on the `.duckdb` file, which blocks dbt's
write lock. The asset runs `scripts/stop_metabase_and_wait.sh`, which does
`docker stop ddd-metabase` (container name overridable via
`METABASE_CONTAINER_NAME`) and then sleeps **120 seconds** so file locks are
released. A non-zero exit fails the asset and the run. Outside Docker, where the
script is absent, the asset logs a warning and does nothing.

### Step 2 — Extraction (25 assets)

**Assets:** `ingestion/DDD/<entity>` (18) and `ingestion/RFAM/<table>` (7), in
the groups `ingestion_DDD_incremental`, `ingestion_DDD_full_extract`,
`ingestion_RFAM_incremental` and `ingestion_RFAM_full_extract`.

Each asset calls `DltPipelineResource.execute_pipeline()`, the same engine the
CLI scripts use, and writes one timestamped NDJSON file to
`Files/Bronze/<SOURCE>/<entity>/<entity>_<YYYYMMDD_HHMMSS>.json` on the local
filesystem or S3 (`RAW_STORAGE_TARGET`).

| Source | Incremental | Full extract |
| --- | --- | --- |
| Danish Parliament OData | Aktør, Møde, Sag, Sagstrin, SagstrinAktør, Stemme — `$filter=opdateringsdato ge DateTime'<date>'` | the 12 lookup entities, no filter |
| Rfam MySQL | `family`, `genome` — `WHERE updated >= :updated_from` (bound parameter) | `clan`, `clan_membership`, `author`, `literature_reference`, `dead_family` |

**Incremental means a lookback window, not a cursor.** The `<date>` is
`today − DANISH_DEMOCRACY_DEFAULT_DAYS_TO_LOAD` (default 31 days) for DDD and
`today − RFAM_DEFAULT_DAYS_TO_LOAD` (default 365) for Rfam, unless a
`date_to_load_from` is supplied in the run config. Consecutive runs therefore
overlap; Silver's hash comparison discards the rows that did not change.

**Retries:** every extraction asset has `RetryPolicy(max_retries=2, delay=60,
backoff=EXPONENTIAL)`. A malformed `date_to_load_from` is rejected before any
request is made.

**Concurrency:** inside this job the 25 assets run one after another. Parallel
extraction happens in the standalone jobs (`danish_parliament_*_job`,
`rfam_*_job`, `multiprocess_executor` with `max_concurrent=4`) and in the CLI
scripts (`ThreadPoolExecutor(max_workers=4)`).

**Log output:** every pipeline run, successful or not, appends one record to
`<DLT_PIPELINE_RUN_LOG_DIR>/<SOURCE>/<pipeline>/<pipeline>_log.ndjson`
(default root `LOCAL_STORAGE_PATH/logs`). A real record, shortened:

```json
{"level": "INFO", "message": "Pipeline execution completed successfully",
 "pipeline_type": "api_to_file", "start_time": "2026-06-24T06:47:18+00:00",
 "end_time": "2026-06-24T06:47:24+00:00", "duration_seconds": 6.6,
 "parameters": {"source_system_code": "DDD", "pipeline_name": "afstemning", "...": "..."},
 "result": {"status": "success", "...": "..."}, "error": null}
```

Secrets in `parameters` are redacted by `_scrub_secrets` before the record is written.

### Step 3 — dbt: Bronze, Silver, Gold

**Assets:** the dbt multi-assets `dbt_bronze_assets`, `dbt_silver_assets` and
`dbt_gold_assets` ([`dbt_assets.py`](../ddd_python/ddd_dagster/dbt_assets.py)).
Each declares `stop_metabase_asset` as an input, so no dbt model can start while
Metabase is up. Dagster runs `dbt build` (models **and** their tests) through
`DbtCliResource`, with dbt's own logs under `DBT_LOGS_DIRECTORY`.

- **Bronze (views).** `read_json_auto()` over the NDJSON files of each entity,
  plus a `_latest` view over the newest file.
- **Silver (incremental tables).** Per entity:
  - the pre-hook records which Bronze files are already processed (`<model>_last_file`);
  - the model hashes every business column (SHA-256) and compares consecutive
    files: new key → `I`, changed hash → `U`, a key that comes back after a
    `D` → `I`, and for full-extract tables a key missing from the next snapshot
    → `D`;
  - the post-hook rebuilds `<model>_last_file` as the new watermark;
  - a `NOT EXISTS (pk, LKHS_date_valid_from)` guard makes reruns idempotent;
  - a `_cv` view exposes the current version of every key.

  The job passes `full_refresh: false`. Deletes in date-filtered incremental
  entities are only detected by a `--full-refresh`
  ([dbt_transformations.md](dbt_transformations.md#34-deletes-of-incremental-entities---full-refresh-only)), and only after a full extraction.
- **Gold (views).** Handcrafted star schema (`actor`, `case`, `meeting`, `vote`,
  `individual_votes`, the `date`/`time` dimensions, …) plus generated `_cv` views.

With `SILVER_STORAGE_FORMAT=ducklake` the Silver tables are written to the
attached `ducklake_catalog` instead of the main file; everything else is the same.

### Step 4 — Delta Lake export

**Assets:** `export/silver/<table>` and `export/gold/<table>`
([`export_assets.py`](../ddd_python/ddd_dagster/export_assets.py)), ordered by
two no-op barrier assets: `barrier_dbt_gold_complete` gates the Silver exports,
and `barrier_all_silver_exported` gates the Gold exports. The destination is set
by `STORAGE_TARGET` (`local` → `LOCAL_STORAGE_PATH/Files/{Silver,Gold}/`, `s3`,
or `onelake` → `abfss://…`).

- **Silver — incremental append.** DuckDB reads the existing Delta table with
  `delta_scan()` and anti-joins on `(primary key, LKHS_date_valid_from)`; only
  rows not yet exported are appended with `deltalake.write_deltalake`. A rerun
  with no new Silver rows appends nothing.
- **Gold — full overwrite.** Each Gold view is read into Arrow and written with
  `mode="overwrite"`; there is no target read.

The export opens its own read-only DuckDB connection. Each table is its own
asset, so a failed table shows up as one failed step rather than hiding the others.

### Step 5 — Observability layer

**Asset:** `dbt_data_engineering_assets` (depends on
`export/barrier_all_gold_exported`). Builds the `data_engineering` models from
Dagster's SQLite run storage (`$DAGSTER_HOME/history/runs.db` and
`history/runs/index.db`) and from the step-failure NDJSON exported by the
failure sensor:

| Model | Grain |
| --- | --- |
| `dagster_pipeline_runs`, `dagster_event_logs` | staging: runs, `ASSET_MATERIALIZATION` events |
| `dagster_job`, `dagster_asset` | dimensions: one row per job / per asset key |
| `dagster_run` | fact: one row per run, with rows processed and assets materialized |
| `dagster_asset_materialization` | fact: one row per materialization, with step timing |
| `dagster_step_failures_raw`, `dagster_step_failure` | fact: one row per failed asset per run |

### Step 6 — Start Metabase

**Asset:** `start_metabase_asset` runs `scripts/start_metabase_and_wait.sh`:
`docker start ddd-metabase`, then a fixed **120-second** wait for Metabase to
initialise. It only runs when every upstream step succeeded; see the failure
sensor below for the other case.

---

## Run-Status Sensors

Two sensors in [`sensors.py`](../ddd_python/ddd_dagster/sensors.py) watch
**every** job and are enabled by default.

| | `danish_parliament_run_success_sensor` | `danish_parliament_run_failure_sensor` |
| --- | --- | --- |
| Run summary | appends to `<DLT_PIPELINE_RUN_LOG_DIR>/DDD/<job_name>_run_log.ndjson`: times, duration, per-step status, tags | same |
| Metabase | — | restarts it (`docker start`, no wait) if the failed run had stopped it |
| Step failures | — | writes the run's `STEP_FAILURE` events to `<DLT_PIPELINE_RUN_LOG_DIR>/dagster/step_failures/<run_id>.ndjson` |
| ntfy.sh | `Dagster run SUCCEEDED - <job>`, priority `default` | `Dagster run FAILED - <job>`, priority `high` |

Alerts are opt-in: nothing is sent when `NTFY_TOPIC` is unset. A failed log
write or alert is logged as a warning and never fails the sensor tick.

---

## Troubleshooting a Failed Run

1. **Open the run in the Dagster UI** (<http://localhost:3000>). The failing step
   and its stack trace are there; with `NTFY_TOPIC` set you also got a push alert.
2. **Extraction failed?** Read the newest record in
   `<DLT_PIPELINE_RUN_LOG_DIR>/<SOURCE>/<pipeline>/<pipeline>_log.ndjson`; the
   `error` field holds the traceback. Typical causes: API timeouts, a MySQL
   connection error (30 s connect timeout), S3 credentials.
3. **dbt failed?** The Dagster step log shows the failing model or test. Common
   causes: a data test failing on new data, a source schema change, or another
   process holding the `.duckdb` lock.
4. **Export failed?** Usually credentials (`STORAGE_TARGET=s3|onelake`) or a
   paused Fabric capacity (`CapacityNotActive`).
5. **Rerun.** Every stage is safe to repeat: extraction writes a new
   timestamped file and Silver ignores unchanged rows, dbt's `NOT EXISTS` guard
   prevents duplicate versions, Silver export anti-joins against the target, and
   Gold export overwrites.

More scenarios and recovery commands are in [runbook.md](runbook.md).

## Related Documentation

- [python_code_explained.md](python_code_explained.md) — how each Python module works
- [dbt_transformations.md](dbt_transformations.md) — the CDC/SCD2 SQL in detail
- [design_decisions.md](design_decisions.md) — why the pipeline runs this way (see ADR-5)
- [project_reference.md](project_reference.md#running-the-project) — running jobs by hand
