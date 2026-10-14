{#- Before a --full-refresh of an incremental Silver model, copy to <model>_current_temp the
    newest version of every key plus every D row.  The newest versions are used to detect
    new deletes; the D rows are carried forward, because deletes of incremental entities
    cannot be derived from Bronze. -#}
{%- macro generate_pre_hook_silver_full_refresh(primary_key_columns='id') -%}
{%- if is_incremental() == False -%}
DROP TABLE IF EXISTS {{ this.database }}.{{ this.schema }}.{{ this.name }}_current_temp;
{%- set current_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) -%}
{%- if current_relation is not none -%}
CREATE TABLE {{ this.database }}.{{ this.schema }}.{{ this.name }}_current_temp AS
SELECT src.* FROM {{ this }} src
QUALIFY ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.{{ primary_key_columns }} ORDER BY src.LKHS_date_valid_from DESC) = 1
OR      src.LKHS_cdc_operation = 'D'
{%- endif -%}
{%- endif -%}
{%- endmacro -%}
