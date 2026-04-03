-- dagster_asset_materialization: one row per ASSET_MATERIALIZATION event.
-- Reads directly from $DAGSTER_HOME/history/runs/index.db (consolidated event index).
-- Materialized as table so host tools (e.g. DBeaver) can query it without
-- needing access to the container-internal SQLite file path.
{{ config(materialized='table') }}
--
-- Step duration: ASSET_MATERIALIZATION_PLANNED → ASSET_MATERIALIZATION.
-- Joined on (run_id, step_key, asset_key) to avoid fan-out on shared step keys
-- (e.g. dbt_silver_assets covers many assets under the same step_key).
--
-- run_status / run_error_message reflect the overall run outcome. Individual
-- asset materializations are always successful by definition (no ASSET_MATERIALIZATION
-- event is written for a failed step).
--
-- Date FK joins gold.date (date_day = DATE).
-- Time FK joins gold.time (time_key = integer 100*hour + minute).

WITH events AS (
    SELECT
        id
    ,   run_id
    ,   asset_key
    ,   step_key
    ,   partition
    ,   timestamp
    ,   dagster_event_type
    ,   event
    FROM sqlite_scan(
        '{{ env_var("DAGSTER_HOME", ".dagster") }}/history/runs/index.db',
        'event_logs'
    )
),

step_planned AS (
    SELECT
        run_id
    ,   step_key
    ,   asset_key
    ,   timestamp   AS step_planned_ts
    FROM  events
    WHERE dagster_event_type = 'ASSET_MATERIALIZATION_PLANNED'
      AND step_key            IS NOT NULL
),

materializations AS (
    SELECT * FROM events
    WHERE dagster_event_type = 'ASSET_MATERIALIZATION'
),

run_failures AS (
    SELECT
        run_id
    ,   json_extract_string(event, '$.user_message') AS error_message
    FROM events
    WHERE dagster_event_type = 'PIPELINE_FAILURE'
)

SELECT
    -- Surrogate foreign keys
    {{ cast_hash_to_bigint('e.asset_key') }}                                     AS asset_sk
,   {{ cast_hash_to_bigint('e.run_id') }}                                        AS run_sk
,   {{ cast_hash_to_bigint("json_extract_string(e.event, '$.pipeline_name')") }} AS job_sk

    -- Step start date / time FKs (from ASSET_MATERIALIZATION_PLANNED event)
,   CASE WHEN sp.step_planned_ts IS NOT NULL
    THEN CAST(sp.step_planned_ts AS DATE) END                                     AS start_date_sk
,   CASE WHEN sp.step_planned_ts IS NOT NULL
    THEN (EXTRACT(HOUR FROM DATE_TRUNC('minute', sp.step_planned_ts))::INTEGER * 100
        + EXTRACT(MINUTE FROM DATE_TRUNC('minute', sp.step_planned_ts))::INTEGER) END AS start_time_sk

    -- Step end date / time FKs (from ASSET_MATERIALIZATION event)
,   CAST(e.timestamp AS DATE)                                                     AS end_date_sk
,   (EXTRACT(HOUR FROM DATE_TRUNC('minute', e.timestamp))::INTEGER * 100
    + EXTRACT(MINUTE FROM DATE_TRUNC('minute', e.timestamp))::INTEGER)           AS end_time_sk

    -- Natural / degenerate keys
,   e.id                                                                          AS materialization_id
,   e.run_id
,   e.asset_key
,   e.step_key
,   e.partition                                                                   AS partition_key

    -- Timestamps
,   sp.step_planned_ts                                                            AS step_planned_at
,   e.timestamp                                                                   AS materialized_at

    -- Measures
,   CASE
        WHEN sp.step_planned_ts IS NOT NULL
        THEN ROUND(
            CAST(EXTRACT('epoch' FROM e.timestamp - sp.step_planned_ts) AS DOUBLE), 1
        )
    END                                                                           AS duration_seconds
,   COALESCE(
        TRY_CAST(
            json_extract_string(e.event, '$.dagster_event.event_specific_data.materialization.metadata."dagster/row_count".value')
        AS BIGINT),
        TRY_CAST(
            json_extract_string(e.event, '$.dagster_event.event_specific_data.materialization.metadata_entries[0].entry_data.value')
        AS BIGINT)
    )                                                                             AS rows_processed

    -- Run outcome (the asset itself was always materialized successfully;
    -- run_status reflects whether the overall run completed without failures)
,   pr.run_status
,   rf.error_message                                                              AS run_error_message

FROM       materializations  e
LEFT JOIN  step_planned      sp ON e.run_id   = sp.run_id
                               AND e.step_key = sp.step_key
                               AND e.asset_key = sp.asset_key
LEFT JOIN  {{ ref('dagster_pipeline_runs') }} pr ON e.run_id = pr.run_id
LEFT JOIN  run_failures      rf ON e.run_id   = rf.run_id
