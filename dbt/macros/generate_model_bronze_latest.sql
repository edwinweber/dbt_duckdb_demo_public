{#- Glob *.json* matches both .json (legacy) and .jsonl (dlt filesystem destination). -#}
{%- macro generate_model_bronze_latest(file_name,source_system_code,source_name,data_source_env_var='DANISH_DEMOCRACY_DATA_SOURCE') -%}
WITH cte_most_recent_file AS
(
SELECT MAX(filename) AS most_recent_file
FROM read_text('{{ env_var(data_source_env_var) }}/{{ file_name }}/{{ file_name }}_*.json*')
)
SELECT DISTINCT COLUMNS(c -> c != 'filename' AND NOT starts_with(c, '_dlt_'))
,      SUBSTRING(filename, LENGTH(filename) - POSITION('/' IN REVERSE(filename)) + 2) AS LKHS_filename
,      '{{ source_system_code }}' AS LKHS_source_system_code
,      'N' AS LKHS_deleted_ind
FROM   read_json_auto('{{ env_var(data_source_env_var) }}/{{ file_name }}/{{ file_name }}_*.json*', filename=True)
WHERE  filename = (SELECT most_recent_file FROM cte_most_recent_file)
{%- endmacro -%}

