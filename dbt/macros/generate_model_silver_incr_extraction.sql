{%- macro generate_model_silver_incr_extraction(file_name,bronze_table_name,primary_key_columns,base_for_hash,data_source_env_var='DANISH_DEMOCRACY_DATA_SOURCE') -%}
{#- Silver is the state: it is never rebuilt from Bronze (full_refresh=false), and a run only reads
    the Bronze files newer than the bookmark.  An incremental file holds only changed rows.  A key's
    first version in the new files is compared with its latest Silver version, a later one with
    its version before it in the new files:
      key not in Silver, or deleted there   -> I
      hash differs                          -> U
    Deletes come from full extractions only: a file named <entity>_<YYYYMMDD_HHMMSS>_full.json holds
    every key.  When it is the only new file, every key that is alive in Silver and missing from it
    gets a D.  A full file that is new together with other files stops the run; several plain files
    are processed in one run.  A first build starts from an empty state and writes no deletes.
    A run stops when the full file would delete too many keys (silver_delete_guard). -#}
{{ config(full_refresh=false, post_hook=["{{ silver_bookmark_post_hook() }}", "{{ silver_files_post_hook() }}"]) }}
{%- set files = silver_files(file_name, data_source_env_var) -%}
{%- set run_started = "CAST('" ~ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] ~ "' AS DATETIME)" -%}
{%- set ns = namespace(full_files=[]) -%}
{%- for new_file in files.new_files -%}
{%- if '_full.' in new_file -%}{%- set ns.full_files = ns.full_files + [new_file] -%}{%- endif -%}
{%- endfor -%}
{%- set full_files = ns.full_files -%}
{%- if is_incremental() and full_files | length > 0 and files.new_files | length > 1 -%}
{{ exceptions.raise_compiler_error(this.name ~ ": a _full file must be the only Bronze file to process, new files: " ~ files.new_files | join(', ') ~ ". Process the files before it first, then the _full file on its own.") }}
{%- endif -%}
{%- set delete_file = full_files[0] if is_incremental() and full_files | length == 1 else none %}
WITH CTE_FILES AS (
                  SELECT LKHS_filename
                  ,      {{ parse_filename_ts('LKHS_filename') }} AS LKHS_date_valid_from
                  FROM  (SELECT regexp_extract(file, '[^/]+$') AS LKHS_filename
                         FROM   glob('{{ env_var(data_source_env_var) }}/{{ file_name }}/{{ file_name }}_*.json*')
                        ) files
                  WHERE  LKHS_filename > '{{ files.processed }}' AND LKHS_filename <= '{{ files.newest }}'
                  )
,CTE_BRONZE AS (
               SELECT src.*
               ,      sha256({{ base_for_hash }}) AS LKHS_hash_value
               FROM  (SELECT {{ silver_bronze_columns(bronze_table_name) }}
                      FROM   {{ ref(bronze_table_name) }}
                      WHERE  LKHS_filename > '{{ files.processed }}' AND LKHS_filename <= '{{ files.newest }}'
                     ) src
               )
-- Each key's versions in the new files, in file order
,CTE_NEW AS (
            SELECT CTE_BRONZE.*
            ,      CTE_FILES.LKHS_date_valid_from
            ,      LAG(CTE_BRONZE.LKHS_hash_value) OVER (PARTITION BY {{ primary_key_columns }} ORDER BY CTE_BRONZE.LKHS_filename) AS LKHS_hash_value_previous
            ,      ROW_NUMBER() OVER (PARTITION BY {{ primary_key_columns }} ORDER BY CTE_BRONZE.LKHS_filename) AS LKHS_appearance
            FROM       CTE_BRONZE
            INNER JOIN CTE_FILES
            ON         CTE_BRONZE.LKHS_filename = CTE_FILES.LKHS_filename
            )
{%- if is_incremental() %}
,CTE_SILVER_LATEST AS (
                      SELECT * FROM {{ this }}
                      QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ primary_key_columns }} ORDER BY LKHS_date_valid_from DESC) = 1
                      )
-- Silver's state: the latest version of every key, hashed with the current columns
,CTE_STATE AS (
              SELECT {{ primary_key_columns }}, LKHS_cdc_operation AS LKHS_latest_operation, sha256({{ base_for_hash }}) AS LKHS_hash_value
              FROM  (SELECT {{ silver_state_columns(bronze_table_name) }}, LKHS_cdc_operation
                     FROM   CTE_SILVER_LATEST
                    ) state
              )
{%- endif %}
,CTE_CHANGES AS (
                SELECT n.* EXCLUDE (LKHS_hash_value_previous,LKHS_appearance,LKHS_date_valid_from)
                ,      n.LKHS_date_valid_from
                ,      {{ run_started }} AS LKHS_date_inserted
                ,      CASE
                           WHEN n.LKHS_appearance > 1 THEN CASE WHEN n.LKHS_hash_value != n.LKHS_hash_value_previous THEN 'U' END
{%- if is_incremental() %}
                           WHEN state.{{ primary_key_columns }} IS NULL OR state.LKHS_latest_operation = 'D' THEN 'I'
                           WHEN n.LKHS_hash_value != state.LKHS_hash_value THEN 'U'
{%- else %}
                           ELSE 'I'
{%- endif %}
                       END AS LKHS_cdc_operation
                FROM   CTE_NEW n
{%- if is_incremental() %}
                LEFT JOIN CTE_STATE state
                ON        state.{{ primary_key_columns }} = n.{{ primary_key_columns }}
{%- endif %}
                WHERE  LKHS_cdc_operation IN ('I','U')
                )
{%- if delete_file %}
-- The only new file is a full extraction: every key whose latest Silver version is not a D and
-- that is missing from it is deleted as of that file, with the content of that version.
,CTE_NEW_DELETES AS (
                    SELECT latest.* EXCLUDE (LKHS_filename,LKHS_date_valid_from,LKHS_date_inserted,LKHS_cdc_operation)
                    ,      '{{ delete_file }}' AS LKHS_filename
                    ,      {{ parse_filename_ts("'" ~ delete_file ~ "'") }} AS LKHS_date_valid_from
                    ,      {{ run_started }} AS LKHS_date_inserted
                    ,      'D' AS LKHS_cdc_operation
                    FROM   CTE_SILVER_LATEST latest
                    WHERE  latest.LKHS_cdc_operation != 'D'
                    AND    NOT EXISTS (SELECT 1 FROM CTE_BRONZE full_file
                                       WHERE  full_file.LKHS_filename = '{{ delete_file }}'
                                       AND    full_file.{{ primary_key_columns }} = latest.{{ primary_key_columns }})
                    )
{%- endif %}
-- Branches are combined by column name: after a new source column the Silver rows have it at
-- the end, the rows from Bronze in their Bronze position.
,CTE_ALL_ROWS AS (
SELECT * FROM CTE_CHANGES
{%- if delete_file %}
UNION ALL BY NAME
SELECT * FROM CTE_NEW_DELETES
{%- endif %}
)
{%- if delete_file %}
-- The full file's deletes and the live keys they are taken from
,CTE_DELETE_GUARD AS (
                     SELECT '{{ delete_file }}' AS LKHS_filename
                     ,      (SELECT COUNT(*) FROM CTE_NEW_DELETES) AS LKHS_deletes
                     ,      (SELECT COUNT(*) FROM CTE_SILVER_LATEST WHERE LKHS_cdc_operation != 'D') AS LKHS_keys_before
                     )
{%- endif %}
SELECT CTE_ALL_ROWS.*
FROM   CTE_ALL_ROWS
{% if is_incremental() %}
WHERE NOT EXISTS (SELECT {{ primary_key_columns }} FROM {{ this }} WHERE {{ primary_key_columns }} = CTE_ALL_ROWS.{{ primary_key_columns }} AND LKHS_date_valid_from = CTE_ALL_ROWS.LKHS_date_valid_from)
{%- if delete_file %}
AND   {{ silver_delete_guard() }}
{%- endif %}
{%- endif -%}
{%- endmacro -%}
