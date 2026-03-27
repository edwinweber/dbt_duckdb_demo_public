{{ config(tags=['rfam']) }}
{{ generate_model_bronze_latest('literature_reference','RFAM','rfam',data_source_env_var='RFAM_DATA_SOURCE') }}