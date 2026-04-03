-- dagster_step_failure: one row per failed asset per run.
-- Adds surrogate keys and date/time FKs on top of dagster_step_failures_raw.
--
-- Joins:
--   asset_sk  → dagster_asset (asset_key)
--   run_sk    → dagster_run (run_id)
--   job_sk    → dagster_job (job_name)
--   date_sk   → gold.date (date_day)
--   time_sk   → gold.time (time_key = 100*hour + minute)

SELECT
    -- Surrogate keys
    {{ cast_hash_to_bigint('f.run_id || f.step_key') }}                          AS failure_sk
,   {{ cast_hash_to_bigint('f.asset_key') }}                                     AS asset_sk
,   {{ cast_hash_to_bigint('f.run_id') }}                                        AS run_sk
,   {{ cast_hash_to_bigint('pr.job_name') }}                                     AS job_sk

    -- Date / time FKs derived from failed_at
,   CAST(f.failed_at AS DATE)                                                    AS date_sk
,   (EXTRACT(HOUR   FROM DATE_TRUNC('minute', f.failed_at))::INTEGER * 100
    + EXTRACT(MINUTE FROM DATE_TRUNC('minute', f.failed_at))::INTEGER)          AS time_sk

    -- Natural keys
,   f.run_id
,   f.step_key
,   f.asset_key

    -- Error details
,   f.error_class
,   f.error_message

    -- Timestamp
,   f.failed_at

FROM      {{ ref('dagster_step_failures_raw') }}   f
LEFT JOIN {{ ref('dagster_pipeline_runs') }}        pr ON f.run_id = pr.run_id
