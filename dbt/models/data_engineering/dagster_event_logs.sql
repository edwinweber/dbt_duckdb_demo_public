-- dagster_event_logs: one row per ASSET_MATERIALIZATION event.
-- Reads from the Dagster consolidated SQLite event index
-- ($DAGSTER_HOME/history/runs/index.db) and flattens nested JSON metadata
-- into dedicated columns. Metadata entries are extracted by label via a
-- generate_series lateral join over the JSON array.

WITH raw AS (
    SELECT
        run_id
    ,   event                                                                    AS event_json
    ,   timestamp                                                                AS event_timestamp
    ,   step_key
    FROM sqlite_scan(
        '{{ env_var("DAGSTER_HOME", ".dagster") }}/history/runs/index.db',
        'event_logs'
    )
    WHERE dagster_event_type = 'ASSET_MATERIALIZATION'
),

unpacked AS (
    SELECT
        run_id
    ,   event_timestamp
    ,   step_key
    ,   json_extract_string(event_json, '$.pipeline_name')                      AS pipeline_name
    ,   CAST(
            json_extract(event_json,
                '$.dagster_event.event_specific_data.materialization.asset_key.path'
            )
        AS VARCHAR[])                                                            AS asset_path
    ,   json_extract(event_json,
            '$.dagster_event.event_specific_data.materialization.metadata_entries'
        )                                                                        AS metadata_entries_json
    FROM raw
),

with_meta AS (
    SELECT
        u.run_id
    ,   u.event_timestamp
    ,   u.step_key
    ,   u.pipeline_name
    ,   u.asset_path[1]                                                          AS asset_key_group
    ,   u.asset_path[2]                                                          AS asset_key_layer
    ,   u.asset_path[-1]                                                         AS asset_key_name
    ,   array_to_string(u.asset_path, '/')                                       AS asset_key_full
    ,   MAX(CASE WHEN m.label = 'records_written'    THEN m.value END)           AS records_written_raw
    ,   MAX(CASE WHEN m.label = 'destination_file'   THEN m.value END)           AS destination_file
    ,   MAX(CASE WHEN m.label = 'destination_path'   THEN m.value END)           AS destination_path
    ,   MAX(CASE WHEN m.label = 'duration_seconds'   THEN m.value END)           AS duration_seconds_raw
    ,   MAX(CASE WHEN m.label = 'pipeline_status'    THEN m.value END)           AS ingestion_status
    ,   MAX(CASE WHEN m.label = 'rows_written'       THEN m.value END)           AS rows_written_raw
    ,   MAX(CASE WHEN m.label = 'table'              THEN m.value END)           AS export_table
    ,   MAX(CASE WHEN m.label = 'Execution Duration' THEN m.value END)           AS execution_duration_raw
    ,   MAX(CASE WHEN m.label = 'unique_id'          THEN m.value END)           AS dbt_unique_id
    ,   MAX(CASE WHEN m.label = 'invocation_id'      THEN m.value END)           AS dbt_invocation_id
    FROM unpacked u
    LEFT JOIN LATERAL (
        SELECT
            json_extract_string(
                u.metadata_entries_json, '$[' || i::VARCHAR || '].label'
            )                                                                    AS label
        ,   COALESCE(
                json_extract_string(
                    u.metadata_entries_json, '$[' || i::VARCHAR || '].entry_data.text'
                ),
                json_extract_string(
                    u.metadata_entries_json, '$[' || i::VARCHAR || '].entry_data.value'
                )
            )                                                                    AS value
        FROM generate_series(
            0,
            COALESCE(CAST(json_array_length(u.metadata_entries_json) AS INTEGER), 0) - 1
        ) AS t(i)
    ) m ON true
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)

SELECT
    run_id
,   event_timestamp
,   step_key
,   pipeline_name
,   asset_key_group
,   asset_key_layer
,   asset_key_name
,   asset_key_full
,   TRY_CAST(records_written_raw AS BIGINT)                                      AS records_written
,   destination_file
,   destination_path
,   TRY_CAST(duration_seconds_raw AS DOUBLE)                                     AS ingestion_duration_seconds
,   ingestion_status
,   TRY_CAST(rows_written_raw AS BIGINT)                                         AS rows_written
,   export_table
,   TRY_CAST(duration_seconds_raw AS DOUBLE)                                     AS export_duration_seconds
,   TRY_CAST(execution_duration_raw AS DOUBLE)                                   AS execution_duration_seconds
,   dbt_unique_id
,   dbt_invocation_id
FROM with_meta
