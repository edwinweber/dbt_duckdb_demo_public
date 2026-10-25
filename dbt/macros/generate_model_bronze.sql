{#- LKHS_ prefix = Lakehouse metadata columns added by this pipeline (not from the source API).
    primary_key_columns: only for a primary key of several columns (a list); the view then
    adds LKHS_primary_key_1_column, the key Silver's CDC uses (generate_primary_key_column). -#}
{%- macro generate_model_bronze(table_name,source_system_code,source_name,primary_key_columns=none) -%}
SELECT DISTINCT COLUMNS(lambda c: c != 'filename' AND NOT starts_with(c, '_dlt_'))
,      SUBSTRING(src.filename, LENGTH(src.filename) - POSITION('/' IN REVERSE(src.filename)) + 2) AS LKHS_filename
,      '{{ source_system_code }}' AS LKHS_source_system_code
{%- if primary_key_columns is not none %}
{{ generate_primary_key_column(primary_key_columns) }}
{%- endif %}
FROM
    {{ source(source_name, table_name) }} src
{%- endmacro -%}
