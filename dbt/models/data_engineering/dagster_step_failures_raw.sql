{{ config(materialized='table') }}
{#-
  dagster_step_failures_raw: one row per failed asset per run.

  STEP_FAILURE events live in one SQLite file per run, which sqlite_scan()
  cannot glob. The run-failure sensor (ddd_dagster/sensors.py) therefore exports
  them to NDJSON, one file per run, under
  <DLT_PIPELINE_RUN_LOG_DIR>/dagster/step_failures/<run_id>.ndjson, which is
  read here with a single read_json glob.

  Maps step_key → asset_key(s) via ASSET_MATERIALIZATION_PLANNED events in the
  consolidated index.db, expanding to one row per planned asset (a step such as
  dbt_silver_assets covers many assets); asset_key is NULL when no planned
  event exists.
-#}
{%- set log_dir = env_var('DLT_PIPELINE_RUN_LOG_DIR', env_var('LOCAL_STORAGE_PATH', 'data') ~ '/logs') -%}
{%- set failures_glob = log_dir ~ '/dagster/step_failures/*.ndjson' -%}
{%- set has_files = false -%}
{%- if execute -%}
    {%- set file_count = run_query("SELECT count(*) FROM glob('" ~ failures_glob ~ "')") -%}
    {%- set has_files = file_count.columns[0].values()[0] > 0 -%}
{%- endif %}

WITH step_failures AS (
{%- if has_files %}
    SELECT run_id, step_key, failed_at, error_class, error_message
    FROM read_json(
        '{{ failures_glob }}',
        format  = 'newline_delimited',
        columns = {
            run_id: 'VARCHAR', step_key: 'VARCHAR', failed_at: 'TIMESTAMP',
            error_class: 'VARCHAR', error_message: 'VARCHAR'
        }
    )
{%- else %}
    -- No failure files exported yet: typed empty set keeps downstream models valid.
    SELECT
        NULL::VARCHAR   AS run_id
    ,   NULL::VARCHAR   AS step_key
    ,   NULL::TIMESTAMP AS failed_at
    ,   NULL::VARCHAR   AS error_class
    ,   NULL::VARCHAR   AS error_message
    WHERE false
{%- endif %}
),

planned_assets AS (
    SELECT DISTINCT
        run_id
    ,   step_key
    ,   asset_key
    FROM sqlite_scan('{{ env_var("DAGSTER_HOME", ".dagster") }}/history/runs/index.db', 'event_logs')
    WHERE dagster_event_type = 'ASSET_MATERIALIZATION_PLANNED'
      AND asset_key IS NOT NULL
)

SELECT
    f.run_id::VARCHAR                  AS run_id
,   f.step_key::VARCHAR                AS step_key
,   p.asset_key::VARCHAR               AS asset_key
,   f.failed_at::TIMESTAMP             AS failed_at
,   f.error_class                      AS error_class
,   f.error_message                    AS error_message
FROM step_failures f
LEFT JOIN planned_assets p
    ON  p.run_id   = f.run_id
    AND p.step_key = f.step_key
