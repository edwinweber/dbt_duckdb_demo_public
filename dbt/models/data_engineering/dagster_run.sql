-- dagster_run: one row per Dagster run.
-- Builds on dagster_pipeline_runs and dagster_event_logs so all SQLite access
-- stays in one place.
-- Joins: job_sk → dagster_job; start/end date/time FKs → gold.date / gold.time.

WITH runs AS (
    SELECT * FROM {{ ref('dagster_pipeline_runs') }}
),

run_stats AS (
    -- rows_processed: ingestion assets report records_written, export assets rows_written.
    SELECT
        run_id
    ,   SUM(COALESCE(records_written, rows_written, 0))  AS rows_processed
    ,   COUNT(*)                                          AS assets_materialized
    FROM {{ ref('dagster_event_logs') }}
    GROUP BY run_id
)

SELECT
    -- Surrogate / foreign keys
    {{ cast_hash_to_bigint('r.run_id') }}                                        AS run_sk
,   {{ cast_hash_to_bigint('r.job_name') }}                                      AS job_sk

    -- Start date / time FKs
    -- Cast to TIMESTAMP first to drop the TIMESTAMPTZ zone before DATE/TIME extraction
,   CASE WHEN r.run_started_at IS NOT NULL
    THEN CAST(r.run_started_at::TIMESTAMP AS DATE) END                            AS start_date_sk
,   CASE WHEN r.run_started_at IS NOT NULL
    THEN (EXTRACT(HOUR FROM DATE_TRUNC('minute', r.run_started_at::TIMESTAMP))::INTEGER * 100
        + EXTRACT(MINUTE FROM DATE_TRUNC('minute', r.run_started_at::TIMESTAMP))::INTEGER) END AS start_time_sk

    -- End date / time FKs (NULL when run is still in progress)
,   CASE WHEN r.run_ended_at IS NOT NULL
    THEN CAST(r.run_ended_at::TIMESTAMP AS DATE) END                              AS end_date_sk
,   CASE WHEN r.run_ended_at IS NOT NULL
    THEN (EXTRACT(HOUR FROM DATE_TRUNC('minute', r.run_ended_at::TIMESTAMP))::INTEGER * 100
        + EXTRACT(MINUTE FROM DATE_TRUNC('minute', r.run_ended_at::TIMESTAMP))::INTEGER) END AS end_time_sk

    -- Natural / degenerate keys
,   r.run_id
,   r.run_status                                                                  AS status

    -- Timestamps
,   r.run_created_at                                                              AS created_at
,   r.run_started_at                                                              AS started_at
,   r.run_ended_at                                                                AS ended_at

    -- Measures
,   r.run_duration_seconds                                                        AS duration_seconds
,   COALESCE(rs.rows_processed,      0)                                           AS rows_processed
,   COALESCE(rs.assets_materialized, 0)                                           AS assets_materialized

FROM      runs      r
LEFT JOIN run_stats rs ON r.run_id = rs.run_id
