-- dagster_job: one row per unique Dagster job name ever observed.
-- Surrogate key derived from job_name via hash, consistent with gold layer pattern.

WITH jobs AS (
    SELECT DISTINCT job_name
    FROM {{ ref('dagster_pipeline_runs') }}
    WHERE job_name IS NOT NULL
)

SELECT
    {{ cast_hash_to_bigint('job_name') }}                                        AS job_key
,   job_name
FROM jobs
