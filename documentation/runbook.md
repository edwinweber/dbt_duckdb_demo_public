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
| dbt hangs or fails with a lock error on the `.duckdb` file | Outside Docker: another process has the file open (DBeaver, a DuckDB shell, a second dbt) | [§1](#1-duckdb-file-is-locked) |
| Metabase shows no data or cannot connect | `duckdb_server` is down, or the quack token does not match | [§2](#2-metabase-not-loading-or-connection-refused) |
| `NoSuchBucket` / 404 on extraction | S3 bucket not created | [§3](#3-s3-bucket-missing-or-credentials-wrong) |
| `InvalidAccessKeyId`, `SignatureDoesNotMatch`, S3 400 errors | Wrong S3 credentials, or `S3_URL_STYLE=vhost` on RustFS | [§3](#3-s3-bucket-missing-or-credentials-wrong) |
| `CapacityNotActive` in the Delta export | Fabric capacity paused (`STORAGE_TARGET=onelake`) | `python -m ddd_python.ddd_utils.fabric_capacity_pause_resume resume`, then re-run the export |
| Bronze models return no rows | `DANISH_DEMOCRACY_DATA_SOURCE` / `RFAM_DATA_SOURCE` wrong, or nothing extracted yet | [§4](#4-bronze-is-empty) |
| dbt cannot find a `bronze_ddd_…` / `silver_ddd_…` model | Models not regenerated after a config change | [§5](#5-models-missing-after-adding-an-entity) |
| DuckLake attach errors | `DUCKLAKE_CATALOG_LOCATION` / `DUCKLAKE_DATA_PATH` missing or wrong | [§6](#6-ducklake-catalog-misconfigured) |
| Many unexpected `D` rows in Silver | A `_full` Bronze file that was not complete | [§7](#7-silver-needs-a-rebuild) |
| A Bronze file was never processed | It arrived after a newer file was processed (its name sorts before the bookmark) | [§7](#7-silver-needs-a-rebuild) |
| dbt picks the wrong target | Storage switches not what you expect | `profiles.yml` picks `local_ducklake_s3`, `local_ducklake`, `onelake` or `local`, in that order; check `SILVER_STORAGE_FORMAT`, `RAW_STORAGE_TARGET`, `STORAGE_TARGET` |
| Source freshness warning | No extraction in `DBT_FRESHNESS_WARN_AFTER_DAYS` (default 2) | [§9](#9-source-freshness-warning) |
| No ntfy.sh alert | `NTFY_TOPIC` unset or ntfy.sh unreachable | [§8](#8-no-ntfysh-alerts) |
| Silver differs from the source, or the daily runs stopped for weeks | An extraction gap longer than the lookback window; deletes of incremental entities not seen yet | Full extraction of the incremental entities, then [§10](#10-checking-silver-against-the-source) |
| `FileNotFoundError` on the DuckDB path | `DUCKDB_DATABASE_LOCATION` unset or its directory missing | Set it in `.env` and create the directory |
| Odd errors after a dependency change | Package versions drifted from the pins | `pip install -e ".[dagster,dev]"` (includes dbt-core and dbt-duckdb; never add dbt-oss) |
| Asset shows as unsynced in Dagster | An upstream asset materialised without its downstream | Materialise the downstream asset or run the job |

---

## 1. DuckDB file is locked

DuckDB allows one writer. In Docker mode `duckdb_server` is the sole owner of
the file; all other containers connect via quack RPC, so file-lock conflicts
should not arise. If you see a lock error outside Docker:

```bash
lsof /data/duckdb/danish_democracy_data.duckdb   # anything left? close it
# close DBeaver or duckdb_connect.sh sessions, then retry
docker compose run --rm run python -m ddd_python.ddd_dbt.dbt_build_with_unique_logfile
```

In DuckLake mode the catalog file has the same rule.

## 2. Metabase not loading or connection refused

Metabase connects to `duckdb_server` via quack. If it cannot reach the server:

```bash
docker compose ps duckdb_server          # must be running
docker compose logs duckdb_server        # look for startup errors or token mismatches
docker compose restart metabase          # triggers re-render of duckdb-init.sql
curl -s http://localhost:3001/api/health # Metabase listens on 3001
```

Check that `QUACK_SERVER_TOKEN` is set in `.env` and that
`/data/metabase/duckdb-init.sql` (rendered by the entrypoint) matches the
server's token.

## 3. S3 bucket missing or credentials wrong

Buckets must exist before the first extraction (`ddd-bronze`, plus
`ddd-ducklake` for DuckLake and `ddd-delta` for an S3 export). Create them in the
RustFS console (<http://localhost:9001>) or with `aws s3 mb`; see
[storage_combinations.md](storage_combinations.md#rustfs-for-local-development).

For credential errors, check that the container sees the values and that RustFS
uses path-style URLs:

```bash
docker compose run --rm run bash -c 'echo "$S3_ENDPOINT $S3_URL_STYLE $S3_ACCESS_KEY_ID"'
AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$S3_SECRET_ACCESS_KEY" aws --endpoint-url http://localhost:9000 s3 ls
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

Silver is the state: each run compares the Bronze files newer than the model's
bookmark (`<model>_last_file`) with Silver, and dbt ignores `--full-refresh` for
every Silver model (`full_refresh=false`,
[dbt_transformations.md §3.7](dbt_transformations.md#37-no-rebuild-from-bronze)).
Repairs go through the backups:

- **A wrong Silver table:** restore the `duckdb` (or `ducklake`) backup from before
  the problem; its bookmark comes with it. The next Silver run processes the Bronze
  files after it again (restore archived files first if they are needed).
- **Many unexpected `D` rows** mean a `_full` file that was not complete, for
  example a run that failed half way or used a limited start date. The next real
  full extraction brings the keys back as `I`, so `_cv` and Gold are correct
  again, but the false `D` rows stay in the history. To remove them: restore the
  Silver backup from before that run, take `_full` out of the file name and run
  Silver again.
- **"… Bronze file(s) are not newer than the processed file … but were never
  processed"**: a late file. Its name sorts before the bookmark, so Silver cannot
  merge it into its history, and it is not in the model's `_files` log. Usually a
  later file covers its content (the next snapshot of a full-extract entity, the
  overlapping lookback window of an incremental one). Then log it as skipped with
  one run with the switch on: `--vars '{silver_skip_late_files: true}'` for dbt,
  `--skip_late_files` for `dbt_build_with_unique_logfile`, or
  `skip_late_files: true` in the Launchpad config of the Silver assets. To put it
  into the history instead, restore Silver from before its timestamp and run
  again with the file in place.
- **"… would delete N of M live keys"**: the delete guard stopped a run because a
  file would delete every live key, or more than 10 keys that are also more than
  10 % of them (dbt variables `silver_delete_guard_min_rows` and
  `silver_delete_guard_fraction`). Nothing was written. Look at the file: a
  truncated or failed extraction is removed (move it aside, extract again). When
  the source really deleted those rows, run Silver once with the guard off:
  `--vars '{silver_delete_guard: false}'`, `--no_silver_delete_guard`, or
  `delete_guard: false` in the Launchpad.
- **A conversion error in a Silver model** (`Could not convert …`): the source
  sends a value that does not fit the type of the Silver column, a real type
  change. Change the column with `ALTER TABLE … ALTER COLUMN … TYPE …` and run
  again. A dbt warning "Bronze column types differ from Silver" on its own needs
  no action: Bronze is cast to Silver's type.
- **"a _full file must be the only Bronze file to process"** stops a Silver run
  when a full extraction and other new files are waiting together. Move the
  `_full` file out of the entity directory, run Silver, move it back and run Silver
  again.
- **"the bookmark … is missing or empty"** stops a run when `<model>_last_file` was
  dropped or emptied. Restore it from the backup, or insert the newest processed
  Bronze file by hand; without it every file would be compared with Silver again.
- **A complete rebuild** is a replay: drop the Silver table, its `_last_file` and its `_files`,
  restore the archived files (`python -m ddd_python.ddd_utils.archive_bronze
  --restore`) and run Silver again; for incremental entities each `_full` file
  must be processed on its own.

To inspect a bookmark and the files a model has read:

```sql
SELECT * FROM main_silver.silver_ddd_aktoer_last_file;   -- ducklake_catalog.main_silver… in DuckLake mode
SELECT * FROM main_silver.silver_ddd_aktoer_files ORDER BY LKHS_filename;
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

A gap longer than the lookback window (31 days DDD, 365 days Rfam) means the
incremental entities missed every change in it. Run a full extraction of them
(`--date_to_load_from 1900-01-01`) and process each `_full` file on its own
(§10, [dbt_transformations.md §3](dbt_transformations.md#3-silver-cdc-into-scd-type-2)).

## 10. Checking Silver against the source

Silver's current state should equal what the Bronze files say: for a
full-extract entity the newest snapshot, for an incremental entity the latest
`_full` file plus the newest version of every key in the files after it. This
query counts the differences for one entity (shown for `sagstrin`; use
`ducklake_catalog.main_silver` in DuckLake mode). It matches columns by name, so
the column order of Silver and Bronze does not matter:

```sql
WITH expected AS (   -- the latest _full file and the newest version of each key after it
    SELECT COLUMNS(lambda c: NOT starts_with(c, 'LKHS_')) FROM main_bronze.bronze_ddd_sagstrin
    WHERE  LKHS_filename >= (SELECT MAX(LKHS_filename) FROM main_bronze.bronze_ddd_sagstrin
                             WHERE LKHS_filename LIKE '%\_full.%' ESCAPE '\')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY LKHS_filename DESC) = 1
), actual AS (       -- Silver's live current versions
    SELECT COLUMNS(lambda c: NOT starts_with(c, 'LKHS_')) FROM main_silver.silver_ddd_sagstrin_cv
    WHERE  LKHS_cdc_operation != 'D'
), both_sides AS (
    SELECT *, 1 AS side FROM expected UNION ALL BY NAME SELECT *, -1 AS side FROM actual
)
SELECT COUNT(*) FILTER (WHERE n > 0) AS missing_or_different_in_silver,
       COUNT(*) FILTER (WHERE n < 0) AS extra_or_different_in_silver
FROM  (SELECT COLUMNS(lambda c: c != 'side'), SUM(side) AS n FROM both_sides
       GROUP BY ALL HAVING SUM(side) != 0);
```

For a full-extract entity, `expected` is the newest file only
(`WHERE LKHS_filename = (SELECT MAX(LKHS_filename) FROM …)`, no `QUALIFY`). Both
counts are 0 when Silver is consistent with its Bronze files; a column the source
no longer sends shows up as a difference. After archiving, the latest `_full` file
of an incremental entity is usually in the archive; `expected` then covers only the
keys in the remaining files. `missing_or_different_in_silver` must still be 0, while
`extra_or_different_in_silver` counts the keys whose last change is older than the
files that stayed. The complete comparison needs a fresh full extraction. A difference with the source itself
only shows after a fresh full extraction: run the query before Silver processes
the `_full` file, then run Silver to repair it.

---

## Recovery commands

### Metabase

```bash
docker compose restart metabase                # re-renders duckdb-init.sql with current token
docker compose logs -f metabase | grep -i error
curl -s http://localhost:3001/api/health
```

### dbt

```bash
docker compose run --rm run bash -c "cd /app/dbt && dbt build --select silver_ddd_aktoer --profiles-dir ."
docker compose run --rm run bash -c "cd /app/dbt && dbt test --profiles-dir ."
jq -c '.info | {ts, level, msg}' "$(ls -t /data/dbt_logs/dbt_build_log_*.json | head -1)" | tail -20
```

### DuckDB format upgrade (1.5.x → 2.0)

`scripts/upgrade_duckdb_to_v2.sh` backs up, copies each file into the DuckDB 2.0
storage format and replaces the originals. Run it once after upgrading the image
if files were created by DuckDB 1.5.x:

```bash
scripts/upgrade_duckdb_to_v2.sh --dry-run    # verify without changing anything
scripts/upgrade_duckdb_to_v2.sh              # asks for confirmation, then upgrades
```

Containers are left stopped after a real upgrade; restart them once the image is
updated to DuckDB 2.0.

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
