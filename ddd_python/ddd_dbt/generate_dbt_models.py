"""Generate dbt model SQL files for Bronze, Silver, and Gold layers.

Reads entity lists from :mod:`ddd_python.ddd_utils.configuration_variables`
so that adding a new entity only requires updating that single file.

Usage::

    python -m ddd_python.ddd_dbt.generate_dbt_models
"""

import logging
import os

from ddd_python.ddd_utils import configuration_variables, get_variables_from_env

logger = logging.getLogger(__name__)

PREFIX = '{{'
SUFFIX = '}}'
PREFIX_STMT = '{%-'
SUFFIX_STMT = '-%}'
PRIMARY_KEY_COLUMNS_BRONZE = "'id'"

# Build the set of Silver model names that use incremental extraction,
# derived from the canonical list in configuration_variables — NOT hardcoded.
_INCREMENTAL_SILVER_MODELS: frozenset[str] = frozenset(
    f"silver_{name.replace('ø', 'oe').replace('æ', 'ae').replace('å', 'aa').lower()}"
    for name in configuration_variables.DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL
)


def generate_dbt_models_bronze(
    table_names: list[str],
    file_names: list[str],
    source_system_code: str = "DDD",
    source_name: str = "danish_parliament",
) -> None:
    """Generate dbt Bronze model files (one view + one ``_latest`` view per entity)."""
    target_dir = os.path.join(get_variables_from_env.DBT_MODELS_DIRECTORY, "bronze")
    os.makedirs(target_dir, exist_ok=True)

    for table_name in table_names:
        model_name = table_name.lower()
        model_path = os.path.join(target_dir, model_name)
        query = f"{PREFIX} generate_model_bronze(this.name,'{source_system_code}','{source_name}') {SUFFIX}"
        with open(f"{model_path}.sql", "w") as f:
            f.write(query)
        logger.info("Generated Bronze model %s.sql", model_name)

    for file_name in file_names:
        file_name = file_name.replace("ø", "oe").replace("æ", "ae").replace("å", "aa").lower()
        model_name = f"bronze_{file_name}_latest"
        model_path = os.path.join(target_dir, model_name)
        query = f"{PREFIX} generate_model_bronze_latest('{file_name}','{source_system_code}','{source_name}') {SUFFIX}"
        with open(f"{model_path}.sql", "w") as f:
            f.write(query)
        logger.info("Generated Bronze model %s.sql", model_name)


def generate_dbt_models_silver(table_names: list[str]) -> None:
    """Generate dbt Silver model files (incremental table + ``_cv`` view per entity).

    The macro selection (``generate_model_silver_incr_extraction`` vs
    ``generate_model_silver_full_extraction``) is derived from
    ``configuration_variables.DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL``.
    """
    target_dir = os.path.join(get_variables_from_env.DBT_MODELS_DIRECTORY, "silver")
    os.makedirs(target_dir, exist_ok=True)

    for table_name in table_names:
        model_name = table_name.lower()
        model_path = os.path.join(target_dir, model_name)

        # Derive the Bronze file name from the Silver model name.
        file_name = model_name.replace("silver_", "", 1)

        # Select the correct macro based on the canonical incremental list.
        if model_name in _INCREMENTAL_SILVER_MODELS:
            macro_name = "generate_model_silver_incr_extraction"
        else:
            macro_name = "generate_model_silver_full_extraction"

        query = (
            f"{PREFIX_STMT} set bronze_table_name = this.name.replace('silver', 'bronze', 1) {SUFFIX_STMT}\n"
            f"{PREFIX_STMT} set base_for_hash = generate_base_for_hash(table_name=bronze_table_name,"
            f"columns_to_exclude=var('bronze_columns_to_exclude_in_silver_hash'),"
            f"primary_key_columns=\"{PRIMARY_KEY_COLUMNS_BRONZE}\") {SUFFIX_STMT}\n"
            f"{PREFIX} config( materialized='incremental',incremental_strategy='append',"
            f"on_schema_change='append_new_columns',unique_key=['id','LKHS_date_valid_from'],\n"
            f"pre_hook  = \"{PREFIX} generate_pre_hook_silver('{file_name}') {SUFFIX}\",\n"
            f"post_hook = \"{PREFIX} generate_post_hook_silver('{file_name}') {SUFFIX}\"\n"
            f") {SUFFIX}\n"
            f"{PREFIX} {macro_name}(file_name='{file_name}',bronze_table_name=bronze_table_name,"
            f"primary_key_columns='id',date_column='opdateringsdato',base_for_hash=base_for_hash) {SUFFIX}\n"
        )
        with open(f"{model_path}.sql", "w") as f:
            f.write(query)
        logger.info("Generated Silver model %s.sql (macro: %s)", model_name, macro_name)

        query_cv = (
            f"{PREFIX} config( materialized='view' ) {SUFFIX}\n"
            f"SELECT src.*\n"
            f"FROM {PREFIX} ref('{model_name}') {SUFFIX} src\n"
            f"QUALIFY ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.id ORDER BY src.LKHS_date_valid_from DESC) = 1\n"
        )
        with open(f"{model_path}_cv.sql", "w") as f:
            f.write(query_cv)
        logger.info("Generated Silver model %s_cv.sql", model_name)


def generate_dbt_models_gold_cv(table_names: list[str]) -> None:
    """Generate dbt Gold ``_cv`` (current-version) model files."""
    target_dir = os.path.join(get_variables_from_env.DBT_MODELS_DIRECTORY, "gold")
    os.makedirs(target_dir, exist_ok=True)

    # The 'date' table is manually developed — skip it.
    tables = [t for t in table_names if t != "date"]

    for table_name in tables:
        model_name = table_name.lower()
        model_path = os.path.join(target_dir, model_name)

        query = (
            f"SELECT  src.* EXCLUDE (LKHS_date_inserted_src,LKHS_date_valid_from,LKHS_date_valid_to,LKHS_row_version)\n"
            f"FROM {PREFIX} ref('{table_name}') {SUFFIX} src\n"
            f"QUALIFY ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.{table_name}_bk ORDER BY src.LKHS_date_valid_from DESC) = 1\n"
        )
        with open(f"{model_path}_cv.sql", "w") as f:
            f.write(query)
        logger.info("Generated Gold model %s_cv.sql", model_name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    generate_dbt_models_bronze(
        configuration_variables.DANISH_DEMOCRACY_MODELS_BRONZE,
        configuration_variables.DANISH_DEMOCRACY_FILE_NAMES,
        "DDD",
        "danish_parliament",
    )
    generate_dbt_models_silver(configuration_variables.DANISH_DEMOCRACY_MODELS_SILVER)
    generate_dbt_models_gold_cv(configuration_variables.DANISH_DEMOCRACY_MODELS_GOLD)
