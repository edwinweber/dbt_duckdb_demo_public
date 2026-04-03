
WITH assets AS (
    SELECT DISTINCT
        asset_sk
    ,   asset_key
    FROM {{ ref('dagster_asset_materialization') }}
    WHERE asset_key IS NOT NULL
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
