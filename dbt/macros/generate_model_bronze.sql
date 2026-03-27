{#- LKHS_ prefix = Lakehouse metadata columns added by this pipeline (not from the source API). -#}
{%- macro generate_model_bronze(table_name,source_system_code,source_name) -%}
SELECT DISTINCT COLUMNS(c -> c != 'filename' AND NOT starts_with(c, '_dlt_'))
,      SUBSTRING(src.filename, LENGTH(src.filename) - POSITION('/' IN REVERSE(src.filename)) + 2) AS LKHS_filename
,      '{{ source_system_code }}' AS LKHS_source_system_code
FROM
    {{ source(source_name, table_name) }} src
{%- endmacro -%}