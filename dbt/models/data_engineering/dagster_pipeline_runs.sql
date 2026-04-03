-- dagster_pipeline_runs: one row per Dagster pipeline run.
-- Reads directly from the Dagster SQLite runs database via sqlite_scan.

SELECT
    run_id
,   pipeline_name                                                               AS job_name
,   status                                                                      AS run_status
,   CAST(create_timestamp AS TIMESTAMP)                                         AS run_created_at
,   CASE WHEN start_time IS NOT NULL THEN TO_TIMESTAMP(start_time) END          AS run_started_at
,   CASE WHEN end_time   IS NOT NULL THEN TO_TIMESTAMP(end_time)   END          AS run_ended_at
,   CASE
        WHEN start_time IS NOT NULL AND end_time IS NOT NULL
            THEN ROUND(CAST(end_time - start_time AS DOUBLE), 1)
    END                                                                         AS run_duration_seconds
FROM sqlite_scan(
    '{{ env_var("DAGSTER_HOME", ".dagster") }}/history/runs.db',
    'runs'
)
