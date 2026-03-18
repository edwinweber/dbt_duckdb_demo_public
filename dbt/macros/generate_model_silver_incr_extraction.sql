{%- macro generate_model_silver_incr_extraction(file_name,bronze_table_name,primary_key_columns,date_column,base_for_hash) -%}
{{ config( materialized='incremental',incremental_strategy='append',on_schema_change='append_new_columns',unique_key=['id','LKHS_date_valid_from']
,pre_hook  = "{{ generate_pre_hook_silver_full_refresh() }}"
,post_hook = "DROP TABLE IF EXISTS {{ this.schema }}.{{ this.name }}_current_temp;"
) }}
WITH CTE_BRONZE AS (
SELECT src.*
,sha256({{ base_for_hash }}) AS LKHS_hash_value
,{{ base_for_hash }} AS LKHS_base_for_hash_value
,CAST(MIN({{ date_column }}) OVER (PARTITION BY {{ primary_key_columns }}) AS DATETIME) AS LKHS_date_inserted_src
FROM {{ ref(bronze_table_name) }} src
)
,CTE_FILES AS   (
                SELECT LKHS_filename
                ,      strptime(SUBSTRING(LKHS_filename, LENGTH(LKHS_filename) - 19, 15),'%Y%m%d_%H%M%S') AS LKHS_date_valid_from
                ,      LAG(LKHS_filename)  OVER (ORDER BY LKHS_filename) AS LKHS_filename_previous
                ,      LEAD(LKHS_filename) OVER (ORDER BY LKHS_filename) AS LKHS_filename_next
                ,      LAG(strptime(SUBSTRING(LKHS_filename, LENGTH(LKHS_filename) - 19, 15),'%Y%m%d_%H%M%S')) OVER (ORDER BY LKHS_filename) AS LKHS_date_valid_from_previous
                ,      LEAD(strptime(SUBSTRING(LKHS_filename, LENGTH(LKHS_filename) - 19, 15),'%Y%m%d_%H%M%S')) OVER (ORDER BY LKHS_filename) AS LKHS_date_valid_from_next
                FROM    (SELECT SUBSTRING(filename, LENGTH(filename) - POSITION('/' IN REVERSE(filename)) + 2) AS LKHS_filename
                        FROM read_text('{{ var('danish_democracy_data_source') }}/{{ file_name }}/{{ file_name }}_*.json')
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
                        FROM       CTE_BRONZE
                        INNER JOIN CTE_FILES
                        ON         CTE_BRONZE.LKHS_filename = CTE_FILES.LKHS_filename
                        )
,CTE_ALL_ROWS AS
(
SELECT  CTE_BRONZE_INCL_LAG.* EXCLUDE (LKHS_base_for_hash_value,LKHS_hash_value_previous,LKHS_date_valid_from)
,       CTE_BRONZE_INCL_LAG.LKHS_date_valid_from
,       CAST('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] }}' AS DATETIME) AS LKHS_date_inserted
,       CASE
            WHEN LKHS_hash_value_previous IS NULL THEN 'I'
            WHEN LKHS_hash_value != LKHS_hash_value_previous THEN 'U'
        END AS LKHS_cdc_operation
FROM    CTE_BRONZE_INCL_LAG
WHERE   LKHS_cdc_operation IN ('I','U')
{%- if is_incremental() == False -%}
{%- set current_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) -%}
{%- if current_relation is not none -%}
{% set bronze_latest_version = bronze_table_name + '_latest' %}
UNION ALL
SELECT  cv.* EXCLUDE (LKHS_date_inserted,LKHS_cdc_operation,LKHS_date_valid_from)
,       cv.LKHS_date_valid_from
,       cv.LKHS_date_inserted
,       cv.LKHS_cdc_operation
FROM     {{ this.schema }}.{{ this.name }}_current_temp cv
WHERE   cv.LKHS_cdc_operation = 'D'
UNION ALL
SELECT  cv.* EXCLUDE (LKHS_date_inserted,LKHS_cdc_operation,LKHS_date_valid_from)
,       CTE_FILE_LATEST.LKHS_date_valid_from
,       CAST('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] }}' AS DATETIME) AS LKHS_date_inserted
,       'D' AS LKHS_cdc_operation
FROM      {{ this.schema }}.{{ this.name }}_current_temp cv
CROSS JOIN CTE_FILE_LATEST
LEFT JOIN {{ ref(bronze_latest_version) }} bronze_latest
ON        cv.id = bronze_latest.id
WHERE     cv.LKHS_cdc_operation != 'D'
AND       bronze_latest.id IS NULL
{%- endif -%}
{%- endif -%}
)
SELECT CTE_ALL_ROWS.*
FROM   CTE_ALL_ROWS
WHERE  (CTE_ALL_ROWS.LKHS_filename >= (SELECT LKHS_filename_previous FROM {{ this.schema }}.{{ this.name}}_last_file)
        OR (SELECT LKHS_filename_previous FROM {{ this.schema }}.{{ this.name}}_last_file) IS NULL
       )
{% if is_incremental() %}
AND NOT EXISTS (SELECT id FROM {{ this }} WHERE id = CTE_ALL_ROWS.id AND LKHS_date_valid_from = CTE_ALL_ROWS.LKHS_date_valid_from)
{%- endif -%}
{%- endmacro -%}