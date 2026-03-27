{%- macro generate_pre_hook_silver_full_refresh(primary_key_columns='id') -%}
{%- if is_incremental() == False -%}
DROP TABLE IF EXISTS  {{ this.schema }}.{{ this.name }}_current_temp;
{%- set current_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) -%}
{%- if current_relation is not none -%}
CREATE TABLE {{ this.schema }}.{{ this.name }}_current_temp AS
SELECT src.* FROM {{ this }} src QUALIFY ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.{{ primary_key_columns }} ORDER BY src.LKHS_date_valid_from DESC) = 1
{%- endif -%}
{%- endif -%}
{%- endmacro -%}