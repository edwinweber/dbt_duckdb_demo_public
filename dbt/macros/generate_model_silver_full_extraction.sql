{%- macro generate_model_silver_full_extraction(file_name,bronze_table_name,primary_key_columns,date_column,base_for_hash) -%}
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
,CTE_BRONZE_INCL_LAG AS (
                        SELECT CTE_BRONZE.*
                        ,       CTE_FILES.LKHS_date_valid_from
                        ,       CTE_BRONZE_PREVIOUS.LKHS_hash_value AS LKHS_hash_value_previous
                        ,       CTE_BRONZE_PREVIOUS.id AS LKHS_primary_key_previous
                        FROM       CTE_BRONZE
                        INNER JOIN CTE_FILES
                        ON         CTE_BRONZE.LKHS_filename = CTE_FILES.LKHS_filename
                        LEFT JOIN  CTE_BRONZE CTE_BRONZE_PREVIOUS
                        ON         CTE_FILES.LKHS_filename_previous = CTE_BRONZE_PREVIOUS.LKHS_filename
                        AND        CTE_BRONZE.id = CTE_BRONZE_PREVIOUS.id
                        )
,CTE_ALL_ROWS AS
(
SELECT      CTE_BRONZE_INCL_LAG.* EXCLUDE (LKHS_base_for_hash_value,LKHS_hash_value_previous,LKHS_primary_key_previous,LKHS_filename,LKHS_date_valid_from)
,           CTE_BRONZE_INCL_LAG.LKHS_filename
,           CTE_BRONZE_INCL_LAG.LKHS_date_valid_from
,           CAST('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] }}' AS DATETIME) AS LKHS_date_inserted
,           CASE
                WHEN LKHS_primary_key_previous IS NULL THEN 'I'
                WHEN LKHS_primary_key_previous IS NOT NULL AND LKHS_hash_value != LKHS_hash_value_previous THEN 'U'
            END AS LKHS_cdc_operation
FROM        CTE_BRONZE_INCL_LAG
WHERE       LKHS_cdc_operation IN ('I','U')
UNION ALL
SELECT      CTE_BRONZE_INCL_LAG.* EXCLUDE (LKHS_base_for_hash_value,LKHS_hash_value_previous,LKHS_primary_key_previous,LKHS_filename,LKHS_date_valid_from)
,           CTE_FILES.LKHS_filename_next AS LKHS_filename
,           LKHS_date_valid_from_next AS LKHS_date_valid_from
,           CAST('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] }}' AS DATETIME) AS LKHS_date_inserted
,           'D' AS LKHS_cdc_operation
FROM        CTE_BRONZE_INCL_LAG
INNER JOIN  CTE_FILES
ON          CTE_BRONZE_INCL_LAG.LKHS_filename = CTE_FILES.LKHS_filename
LEFT  JOIN  CTE_BRONZE_INCL_LAG CTE_BRONZE_INCL_LAG_NEXT
ON          CTE_FILES.LKHS_filename_next = CTE_BRONZE_INCL_LAG_NEXT.LKHS_filename
AND         CTE_BRONZE_INCL_LAG.id = CTE_BRONZE_INCL_LAG_NEXT.id
WHERE       CTE_BRONZE_INCL_LAG_NEXT.id IS NULL
AND         CTE_FILES.LKHS_filename_next IS NOT NULL
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