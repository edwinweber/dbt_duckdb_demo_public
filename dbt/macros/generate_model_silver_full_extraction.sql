{%- macro generate_model_silver_full_extraction(file_name,bronze_table_name,primary_key_columns,base_for_hash,data_source_env_var='DANISH_DEMOCRACY_DATA_SOURCE',key_source_columns=none) -%}
{#- Silver is the state: it is never rebuilt from Bronze (full_refresh=false), and a run only reads
    the Bronze files newer than the bookmark.  Every file of a full-extract entity is a complete
    snapshot.  The first new file is compared with Silver's latest version of every key, each
    following new file with the new file before it:
      key not in the previous snapshot           -> I (also a key that comes back after a D)
      key in it, hash differs                     -> U
      key in the previous snapshot, not in this   -> D, as of this file
    A first build starts from an empty state and processes all files the same way.  A normal run
    stops when a new file would delete too many keys (silver_delete_guard). -#}
{{ config(full_refresh=false, post_hook=["{{ silver_bookmark_post_hook() }}", "{{ silver_files_post_hook() }}", "{{ silver_row_counts_post_hook() }}"]) }}
{%- set files = silver_files(file_name, data_source_env_var) -%}
{%- set run_started = "CAST('" ~ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] ~ "' AS DATETIME)" -%}
WITH CTE_FILES AS (
                  SELECT LKHS_filename
                  ,      {{ parse_filename_ts('LKHS_filename') }} AS LKHS_date_valid_from
                  ,      LAG(LKHS_filename)  OVER (ORDER BY LKHS_filename) AS LKHS_filename_previous
                  ,      LEAD(LKHS_filename) OVER (ORDER BY LKHS_filename) AS LKHS_filename_next
                  ,      LEAD({{ parse_filename_ts('LKHS_filename') }}) OVER (ORDER BY LKHS_filename) AS LKHS_date_valid_from_next
                  FROM  (SELECT regexp_extract(file, '[^/]+$') AS LKHS_filename
                         FROM   glob('{{ env_var(data_source_env_var) }}/{{ file_name }}/{{ file_name }}_*.json*')
                        ) files
                  WHERE  LKHS_filename > '{{ files.processed }}' AND LKHS_filename <= '{{ files.newest }}'
                  )
,CTE_BRONZE AS (
               SELECT src.*
               ,      sha256({{ base_for_hash }}) AS LKHS_hash_value
               FROM  (SELECT {{ silver_bronze_columns(bronze_table_name, key_source_columns) }}
                      FROM   {{ ref(bronze_table_name) }}
                      WHERE  LKHS_filename > '{{ files.processed }}' AND LKHS_filename <= '{{ files.newest }}'
                     ) src
               )
{%- if is_incremental() %}
,CTE_SILVER_LATEST AS (
                      SELECT * FROM {{ this }}
                      QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ primary_key_columns }} ORDER BY LKHS_date_valid_from DESC) = 1
                      )
-- Silver's state: the latest version of every key that is not deleted, hashed with the current columns
,CTE_STATE AS (
              SELECT {{ primary_key_columns }}, sha256({{ base_for_hash }}) AS LKHS_hash_value
              FROM  (SELECT {{ silver_state_columns(bronze_table_name) }}
                     FROM   CTE_SILVER_LATEST
                     WHERE  LKHS_cdc_operation != 'D'
                    ) state
              )
{%- endif %}
-- The previous snapshot of each new file: the new file before it, or Silver's state for the first one
,CTE_PREVIOUS AS (
                 SELECT {{ primary_key_columns }}, LKHS_hash_value, LKHS_filename FROM CTE_BRONZE
{%- if is_incremental() %}
                 UNION ALL
                 SELECT {{ primary_key_columns }}, LKHS_hash_value, '<silver>' AS LKHS_filename FROM CTE_STATE
{%- endif %}
                 )
,CTE_CHAIN AS (
              SELECT CTE_FILES.*
              ,      {% if is_incremental() %}COALESCE(LKHS_filename_previous, '<silver>'){% else %}LKHS_filename_previous{% endif %} AS LKHS_filename_compared_with
              FROM   CTE_FILES
              )
,CTE_CHANGES AS (
                SELECT cur.* EXCLUDE (LKHS_filename)
                ,      cur.LKHS_filename
                ,      chain.LKHS_date_valid_from
                ,      {{ run_started }} AS LKHS_date_inserted
                ,      CASE
                           WHEN prev.{{ primary_key_columns }} IS NULL THEN 'I'
                           WHEN cur.LKHS_hash_value != prev.LKHS_hash_value THEN 'U'
                       END AS LKHS_cdc_operation
                FROM       CTE_BRONZE cur
                INNER JOIN CTE_CHAIN chain
                ON         chain.LKHS_filename = cur.LKHS_filename
                LEFT JOIN  CTE_PREVIOUS prev
                ON         prev.LKHS_filename = chain.LKHS_filename_compared_with
                AND        prev.{{ primary_key_columns }} = cur.{{ primary_key_columns }}
                WHERE      LKHS_cdc_operation IN ('I','U')
                )
-- The versions a delete between two new files can close: the I and U rows of this run and the
-- latest Silver version of every live key, as stored
,CTE_VERSIONS AS (
                 SELECT * EXCLUDE (LKHS_filename,LKHS_date_inserted,LKHS_cdc_operation) FROM CTE_CHANGES
{%- if is_incremental() %}
                 UNION ALL BY NAME
                 SELECT * EXCLUDE (LKHS_filename,LKHS_date_inserted,LKHS_cdc_operation) FROM CTE_SILVER_LATEST
                 WHERE  LKHS_cdc_operation != 'D'
{%- endif %}
                 )
-- Deletes between two new files: the key is in a new file but not in the next one, with the
-- content of the version it closes.  Not the key's Bronze row: that is hashed with the current
-- columns, which after a new or dropped source column differ from the stored version's.
,CTE_DELETES AS (
                SELECT ver.* EXCLUDE (LKHS_date_valid_from)
                ,      chain.LKHS_filename_next AS LKHS_filename
                ,      chain.LKHS_date_valid_from_next AS LKHS_date_valid_from
                ,      {{ run_started }} AS LKHS_date_inserted
                ,      'D' AS LKHS_cdc_operation
                FROM       CTE_BRONZE cur
                INNER JOIN CTE_CHAIN chain
                ON         chain.LKHS_filename = cur.LKHS_filename
                ASOF JOIN  CTE_VERSIONS ver
                ON         ver.{{ primary_key_columns }} = cur.{{ primary_key_columns }}
                AND        ver.LKHS_date_valid_from <= chain.LKHS_date_valid_from
                WHERE      chain.LKHS_filename_next IS NOT NULL
                AND        NOT EXISTS (SELECT 1 FROM CTE_BRONZE nxt
                                       WHERE  nxt.LKHS_filename = chain.LKHS_filename_next
                                       AND    nxt.{{ primary_key_columns }} = cur.{{ primary_key_columns }})
                )
{%- if is_incremental() %}
-- Deletes against Silver's state: a live key missing from the first new file, with the content of
-- its latest Silver version
,CTE_STATE_DELETES AS (
                      SELECT latest.* EXCLUDE (LKHS_filename,LKHS_date_valid_from,LKHS_date_inserted,LKHS_cdc_operation)
                      ,      first_file.LKHS_filename
                      ,      first_file.LKHS_date_valid_from
                      ,      {{ run_started }} AS LKHS_date_inserted
                      ,      'D' AS LKHS_cdc_operation
                      FROM       CTE_SILVER_LATEST latest
                      CROSS JOIN (SELECT LKHS_filename, LKHS_date_valid_from FROM CTE_CHAIN
                                  WHERE  LKHS_filename_previous IS NULL) first_file
                      WHERE      latest.LKHS_cdc_operation != 'D'
                      AND        NOT EXISTS (SELECT 1 FROM CTE_BRONZE b
                                             WHERE  b.LKHS_filename = first_file.LKHS_filename
                                             AND    b.{{ primary_key_columns }} = latest.{{ primary_key_columns }})
                      )
{%- endif %}
-- Branches are combined by column name: after a new source column the Silver rows have it at
-- the end, the rows from Bronze in their Bronze position.
,CTE_ALL_ROWS AS (
SELECT * FROM CTE_CHANGES
UNION ALL BY NAME
SELECT * FROM CTE_DELETES
{%- if is_incremental() %}
UNION ALL BY NAME
SELECT * FROM CTE_STATE_DELETES
{%- endif %}
)
{%- if is_incremental() %}
-- Per new file: the keys it deletes and the live keys of the snapshot it is compared with
,CTE_DELETE_GUARD AS (
                     SELECT chain.LKHS_filename
                     ,      COALESCE(deletes.n, 0) AS LKHS_deletes
                     ,      COALESCE(keys_before.n, 0) AS LKHS_keys_before
                     FROM      CTE_CHAIN chain
                     LEFT JOIN (SELECT LKHS_filename, COUNT(*) AS n FROM CTE_ALL_ROWS
                                WHERE  LKHS_cdc_operation = 'D' GROUP BY LKHS_filename) deletes
                     ON        deletes.LKHS_filename = chain.LKHS_filename
                     LEFT JOIN (SELECT LKHS_filename, COUNT(*) AS n FROM CTE_PREVIOUS
                                GROUP BY LKHS_filename) keys_before
                     ON        keys_before.LKHS_filename = chain.LKHS_filename_compared_with
                     )
{%- endif %}
SELECT CTE_ALL_ROWS.*
FROM   CTE_ALL_ROWS
{% if is_incremental() %}
WHERE NOT EXISTS (SELECT {{ primary_key_columns }} FROM {{ this }} WHERE {{ primary_key_columns }} = CTE_ALL_ROWS.{{ primary_key_columns }} AND LKHS_date_valid_from = CTE_ALL_ROWS.LKHS_date_valid_from)
AND   {{ silver_delete_guard() }}
{%- endif -%}
{%- endmacro -%}
