{%- set bronze_table_name = this.name.replace('silver', 'bronze', 1) -%}
{%- set base_for_hash = generate_base_for_hash(table_name=bronze_table_name,columns_to_exclude=var('bronze_columns_to_exclude_in_silver_hash'),primary_key_columns="'rfam_acc'") -%}
-- depends_on: {{ ref(bronze_table_name ~ '_latest') }}
{{ config( materialized='incremental',incremental_strategy='append',on_schema_change='append_new_columns',unique_key=['rfam_acc','LKHS_date_valid_from'],tags=['rfam'],
pre_hook  = "{{ generate_pre_hook_silver('family',data_source_env_var='RFAM_DATA_SOURCE') }}",
post_hook = "{{ generate_post_hook_silver('family',data_source_env_var='RFAM_DATA_SOURCE') }}"
) }}
{{ generate_model_silver_incr_extraction(file_name='family',bronze_table_name=bronze_table_name,primary_key_columns='rfam_acc',date_column='updated',base_for_hash=base_for_hash,data_source_env_var='RFAM_DATA_SOURCE') }}
