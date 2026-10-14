{%- macro generate_model_silver_incr_extraction(file_name,bronze_table_name,primary_key_columns,date_column,base_for_hash,data_source_env_var='DANISH_DEMOCRACY_DATA_SOURCE') -%}
{{ config( materialized='incremental',incremental_strategy='append',on_schema_change='append_new_columns',unique_key=[primary_key_columns,'LKHS_date_valid_from']
,pre_hook  = "{{ generate_pre_hook_silver_full_refresh(primary_key_columns='" ~ primary_key_columns ~ "') }}"
,post_hook = "DROP TABLE IF EXISTS {{ this.database }}.{{ this.schema }}.{{ this.name }}_current_temp;"
) }}
{#- Deletes of incremental entities are only written by a --full-refresh, so they are not in
    Bronze.  They are read from Silver: from the table itself on a normal run, from the
    pre-hook's _current_temp copy on a full refresh.  A first build has none. -#}
{%- set existing_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) -%}
{%- set is_rebuild = is_incremental() == False and existing_relation is not none -%}
{%- set current_temp = this.database ~ '.' ~ this.schema ~ '.' ~ this.name ~ '_current_temp' -%}
{%- if is_incremental() -%}
{%- set silver_rows = this -%}
{%- elif is_rebuild -%}
{%- set silver_rows = current_temp -%}
{%- else -%}
{%- set silver_rows = none -%}
{%- endif %}
WITH CTE_BRONZE AS (
SELECT src.*
,sha256({{ base_for_hash }}) AS LKHS_hash_value
,{{ base_for_hash }} AS LKHS_base_for_hash_value
,CAST(MIN({{ date_column }}) OVER (PARTITION BY {{ primary_key_columns }}) AS DATETIME) AS LKHS_date_inserted_src
FROM {{ ref(bronze_table_name) }} src
)
,CTE_FILES AS   (
                SELECT LKHS_filename
                ,      {{ parse_filename_ts('LKHS_filename') }} AS LKHS_date_valid_from
                ,      LAG(LKHS_filename)  OVER (ORDER BY LKHS_filename) AS LKHS_filename_previous
                ,      LEAD(LKHS_filename) OVER (ORDER BY LKHS_filename) AS LKHS_filename_next
                ,      LAG({{ parse_filename_ts('LKHS_filename') }}) OVER (ORDER BY LKHS_filename) AS LKHS_date_valid_from_previous
                ,      LEAD({{ parse_filename_ts('LKHS_filename') }}) OVER (ORDER BY LKHS_filename) AS LKHS_date_valid_from_next
                FROM    (SELECT SUBSTRING(filename, LENGTH(filename) - POSITION('/' IN REVERSE(filename)) + 2) AS LKHS_filename
                        FROM read_text('{{ env_var(data_source_env_var) }}/{{ file_name }}/{{ file_name }}_*.json*')
                        ) files
                )
,CTE_FILE_LATEST AS (
                    SELECT  MAX(LKHS_filename)        AS LKHS_filename
                    ,       MAX(LKHS_date_valid_from) AS LKHS_date_valid_from
                    FROM    CTE_FILES
                    )
,CTE_BRONZE_INCL_LAG AS (
                        SELECT CTE_BRONZE.*
                        ,      CTE_FILES.LKHS_date_valid_from
                        ,      LAG(CTE_BRONZE.LKHS_hash_value) OVER (PARTITION BY {{ primary_key_columns }} ORDER BY CTE_BRONZE.LKHS_filename) AS LKHS_hash_value_previous
                        ,      LAG(CTE_FILES.LKHS_date_valid_from) OVER (PARTITION BY {{ primary_key_columns }} ORDER BY CTE_BRONZE.LKHS_filename) AS LKHS_date_valid_from_previous
                        FROM       CTE_BRONZE
                        INNER JOIN CTE_FILES
                        ON         CTE_BRONZE.LKHS_filename = CTE_FILES.LKHS_filename
                        )
{%- if silver_rows is not none %}
-- A key that Silver deleted between its previous and its current Bronze version has come
-- back: that version is an insert, whatever its hash.
,CTE_SILVER_DELETES AS (
                       SELECT {{ primary_key_columns }}, LKHS_date_valid_from
                       FROM   {{ silver_rows }}
                       WHERE  LKHS_cdc_operation = 'D'
                       )
,CTE_BRONZE_INCL_DELETES AS (
                            SELECT lagged.*
                            ,      EXISTS (SELECT 1
                                           FROM   CTE_SILVER_DELETES del
                                           WHERE  del.{{ primary_key_columns }} = lagged.{{ primary_key_columns }}
                                           AND    del.LKHS_date_valid_from > lagged.LKHS_date_valid_from_previous
                                           AND    del.LKHS_date_valid_from < lagged.LKHS_date_valid_from
                                          ) AS LKHS_deleted_since_previous
                            FROM   CTE_BRONZE_INCL_LAG lagged
                            )
{%- else %}
,CTE_BRONZE_INCL_DELETES AS (
                            SELECT lagged.*, FALSE AS LKHS_deleted_since_previous
                            FROM   CTE_BRONZE_INCL_LAG lagged
                            )
{%- endif %}
,CTE_CHANGES AS
(
SELECT  CTE_BRONZE_INCL_DELETES.* EXCLUDE (LKHS_base_for_hash_value,LKHS_hash_value_previous,LKHS_date_valid_from,LKHS_date_valid_from_previous,LKHS_deleted_since_previous)
,       CTE_BRONZE_INCL_DELETES.LKHS_date_valid_from
,       CAST('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] }}' AS DATETIME) AS LKHS_date_inserted
,       CASE
            WHEN LKHS_hash_value_previous IS NULL THEN 'I'
            WHEN LKHS_deleted_since_previous THEN 'I'
            WHEN LKHS_hash_value != LKHS_hash_value_previous THEN 'U'
        END AS LKHS_cdc_operation
FROM    CTE_BRONZE_INCL_DELETES
WHERE   LKHS_cdc_operation IN ('I','U')
)
{%- if is_rebuild %}
{%- set bronze_latest_version = bronze_table_name + '_latest' %}
-- Full refresh: every existing D row is carried forward.  Its LKHS_filename is set to the
-- Bronze file of its LKHS_date_valid_from (older versions of this macro stored the previous
-- version's file name); if that file no longer exists the stored name is kept.
,CTE_CARRIED_DELETES AS (
                        SELECT    cv.* EXCLUDE (LKHS_filename)
                        ,         COALESCE(files.LKHS_filename, cv.LKHS_filename) AS LKHS_filename
                        FROM      {{ current_temp }} cv
                        LEFT JOIN (SELECT LKHS_date_valid_from, MIN(LKHS_filename) AS LKHS_filename
                                   FROM   CTE_FILES
                                   GROUP BY LKHS_date_valid_from
                                  ) files
                        ON        files.LKHS_date_valid_from = cv.LKHS_date_valid_from
                        WHERE     cv.LKHS_cdc_operation = 'D'
                        )
-- Full refresh: the newest version of every key, rebuilt from Bronze or, for a key no longer
-- in Bronze, the newest one in Silver.  On equal timestamps the rebuilt version wins.
,CTE_NEWEST_VERSION AS (
                       SELECT versions.*
                       FROM  (SELECT CTE_CHANGES.*, 1 AS LKHS_version_rank FROM CTE_CHANGES
                              UNION ALL BY NAME
                              SELECT cv.*, 2 AS LKHS_version_rank FROM {{ current_temp }} cv
                             ) versions
                       QUALIFY ROW_NUMBER() OVER (PARTITION BY versions.{{ primary_key_columns }} ORDER BY versions.LKHS_date_valid_from DESC, versions.LKHS_version_rank) = 1
                       )
-- Full refresh: a key whose newest version is not a D and that is missing from the newest
-- Bronze file is deleted as of that file.
,CTE_NEW_DELETES AS (
                    SELECT     nv.* EXCLUDE (LKHS_filename,LKHS_date_valid_from,LKHS_date_inserted,LKHS_cdc_operation,LKHS_version_rank)
                    ,          CTE_FILE_LATEST.LKHS_filename
                    ,          CTE_FILE_LATEST.LKHS_date_valid_from
                    ,          CAST('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] }}' AS DATETIME) AS LKHS_date_inserted
                    ,          'D' AS LKHS_cdc_operation
                    FROM       CTE_NEWEST_VERSION nv
                    CROSS JOIN CTE_FILE_LATEST
                    LEFT JOIN  {{ ref(bronze_latest_version) }} bronze_latest
                    ON         nv.{{ primary_key_columns }} = bronze_latest.{{ primary_key_columns }}
                    WHERE      nv.LKHS_cdc_operation != 'D'
                    AND        bronze_latest.{{ primary_key_columns }} IS NULL
                    )
{%- endif %}
-- Branches are combined by column name: after a new source column the existing Silver
-- rows have it at the end, the rebuilt rows in its Bronze position.
,CTE_ALL_ROWS AS (
SELECT * FROM CTE_CHANGES
{%- if is_rebuild %}
UNION ALL BY NAME
SELECT * FROM CTE_CARRIED_DELETES
UNION ALL BY NAME
SELECT * FROM CTE_NEW_DELETES
{%- endif %}
)
SELECT CTE_ALL_ROWS.*
FROM   CTE_ALL_ROWS
WHERE  (CTE_ALL_ROWS.LKHS_filename >= (SELECT LKHS_filename_previous FROM {{ this.database }}.{{ this.schema }}.{{ this.name}}_last_file)
        OR (SELECT LKHS_filename_previous FROM {{ this.database }}.{{ this.schema }}.{{ this.name}}_last_file) IS NULL
       )
{% if is_incremental() %}
AND NOT EXISTS (SELECT {{ primary_key_columns }} FROM {{ this }} WHERE {{ primary_key_columns }} = CTE_ALL_ROWS.{{ primary_key_columns }} AND LKHS_date_valid_from = CTE_ALL_ROWS.LKHS_date_valid_from)
{%- endif -%}
{%- endmacro -%}
