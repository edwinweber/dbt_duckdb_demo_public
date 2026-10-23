{{ config(materialized='table') }}
{#-
  dagster_step_failures_raw: one row per failed asset per run.

  Reads STEP_FAILURE events from the individual per-run SQLite files
  ($DAGSTER_HOME/history/runs/{run_id}.db) — these are NOT in the consolidated
  index.db. sqlite_scan() reads exactly one file per call (no list, glob or
  lateral column parameter), so the file list is read with glob() at run time
  and turned into one sqlite_scan() per file, combined with UNION ALL.

  Maps step_key → asset_key(s) via ASSET_MATERIALIZATION_PLANNED events in the
  consolidated index.db, expanding to one row per planned asset (a step such as
  dbt_silver_assets covers many assets); asset_key is NULL when no planned
  event exists.

  Why SQL: in quack CONNECT mode every statement runs on the server, which
  cannot see the in-memory DataFrame a Python model hands back. glob() and
  sqlite_scan() run wherever the SQL runs — on the server under CONNECT, locally
  when dbt opens the .duckdb file directly — so the paths just need to exist
  there.
-#}
{%- set runs_dir = env_var('DAGSTER_HOME', '.dagster') ~ '/history/runs' -%}
{%- set run_files = [] -%}
{%- if execute -%}
    {%- set file_list = run_query(
        "SELECT file FROM glob('" ~ runs_dir ~ "/*.db') "
        ~ "WHERE file NOT LIKE '%/index.db' ORDER BY file"
    ) -%}
    {%- set run_files = file_list.columns[0].values() -%}
{%- endif %}

WITH step_failures AS (
{%- if run_files | length == 0 %}
    SELECT
        NULL::VARCHAR   AS run_id
    ,   NULL::VARCHAR   AS step_key
    ,   NULL::TIMESTAMP AS failed_at
    ,   NULL::VARCHAR   AS event_json
    WHERE false
{%- else %}
{%- for run_file in run_files %}
    SELECT
        run_id
    ,   step_key
    ,   timestamp AS failed_at
    ,   event     AS event_json
    FROM sqlite_scan('{{ run_file }}', 'event_logs')
    WHERE dagster_event_type = 'STEP_FAILURE'
    {%- if not loop.last %}
    UNION ALL
    {%- endif %}
{%- endfor %}
{%- endif %}
),

planned_assets AS (
    SELECT DISTINCT
        run_id
    ,   step_key
    ,   asset_key
    FROM sqlite_scan('{{ runs_dir }}/index.db', 'event_logs')
    WHERE dagster_event_type = 'ASSET_MATERIALIZATION_PLANNED'
      AND asset_key IS NOT NULL
)

SELECT
    f.run_id::VARCHAR                                                                      AS run_id
,   f.step_key::VARCHAR                                                                    AS step_key
,   p.asset_key::VARCHAR                                                                   AS asset_key
,   f.failed_at::TIMESTAMP                                                                 AS failed_at
,   json_extract_string(f.event_json, '$.dagster_event.event_specific_data.error.cls_name') AS error_class
,   json_extract_string(f.event_json, '$.dagster_event.event_specific_data.error.message')  AS error_message
FROM step_failures f
LEFT JOIN planned_assets p
    ON  p.run_id   = f.run_id
    AND p.step_key = f.step_key
