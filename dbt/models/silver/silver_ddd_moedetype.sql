{%- set bronze_table_name = this.name.replace('silver', 'bronze', 1) -%}
{%- set base_for_hash = generate_base_for_hash(table_name=bronze_table_name,columns_to_exclude=var('bronze_columns_to_exclude_in_silver_hash'),primary_key_columns="'id'") -%}
{{ config( materialized='incremental',incremental_strategy='append',on_schema_change='append_new_columns',unique_key=['id','LKHS_date_valid_from'],tags=['ddd'],
pre_hook  = "{{ generate_pre_hook_silver('moedetype') }}",
post_hook = "{{ generate_post_hook_silver('moedetype') }}"
) }}
{{ generate_model_silver_full_extraction(file_name='moedetype',bronze_table_name=bronze_table_name,primary_key_columns='id',date_column='opdateringsdato',base_for_hash=base_for_hash) }}
