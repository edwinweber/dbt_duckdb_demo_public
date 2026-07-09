
WITH planned AS (
    -- ASSET_MATERIALIZATION_PLANNED fires for every step of every run regardless
    -- of outcome (success, failure, or skip), so it is the complete catalog of
    -- every asset Dagster has ever known about -- unlike sourcing from
    -- dagster_asset_materialization (misses assets that only ever failed) or
    -- dagster_step_failure (misses everything that succeeded). This also keeps
    -- the dimension independent of the fact models instead of being derived
    -- from them.
    SELECT DISTINCT asset_key
    FROM sqlite_scan(
        '{{ env_var("DAGSTER_HOME", ".dagster") }}/history/runs/index.db',
        'event_logs'
    )
    WHERE dagster_event_type = 'ASSET_MATERIALIZATION_PLANNED'
      AND asset_key IS NOT NULL
),

assets AS (
    SELECT
        {{ cast_hash_to_bigint('asset_key') }}                                    AS asset_sk
    ,   asset_key
    FROM planned
),

parsed AS (
    SELECT
        asset_sk
    ,   asset_key
    ,   CAST(asset_key AS VARCHAR[])                                          AS asset_path
    FROM assets
)

SELECT
    asset_sk
,   asset_key
,   asset_path[1]                                                                 AS asset_key_group
,   asset_path[2]                                                                 AS asset_key_layer
,   asset_path[-1]                                                                AS asset_key_name
,   array_to_string(asset_path, '/')                                              AS asset_key_full
FROM parsed
