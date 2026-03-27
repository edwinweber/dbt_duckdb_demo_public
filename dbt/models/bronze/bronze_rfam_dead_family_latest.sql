{{ config(tags=['rfam']) }}
{{ generate_model_bronze_latest('dead_family','RFAM','rfam',data_source_env_var='RFAM_DATA_SOURCE') }}