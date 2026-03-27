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
# Build the set of Silver model names that use incremental extraction,
# derived from the canonical list in configuration_variables — NOT hardcoded.
_INCREMENTAL_SILVER_MODELS_DDD: frozenset[str] = frozenset(
    f"silver_ddd_{name.replace('ø', 'oe').replace('æ', 'ae').replace('å', 'aa').lower()}"
    for name in configuration_variables.DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL
)

_INCREMENTAL_SILVER_MODELS_RFAM: frozenset[str] = frozenset(
    f"silver_rfam_{name}"
    for name in configuration_variables.RFAM_TABLE_NAMES_INCREMENTAL
)


def generate_dbt_models_bronze(
    table_names: list[str],
    file_names: list[str],
    source_system_code: str = "DDD",
    source_name: str = "danish_parliament",
    data_source_env_var: str = "DANISH_DEMOCRACY_DATA_SOURCE",
) -> None:
    """Generate dbt Bronze model files (one view + one ``_latest`` view per entity).

    Args:
        table_names: Bronze model names (e.g. ``["bronze_aktoer", ...]``).
        file_names: Raw entity names used for file glob patterns and ``_latest``
            views.  For DDD these are the API resource names; for Rfam these
            are the MySQL table names.
        source_system_code: Short code for the source system (e.g. ``"DDD"``).
        source_name: dbt source name matching ``__sources*.yml``.
        data_source_env_var: Environment variable name for the Bronze data path.
            Only passed to ``generate_model_bronze_latest`` when non-default.
    """
    target_dir = os.path.join(get_variables_from_env.DBT_MODELS_DIRECTORY, "bronze")
    os.makedirs(target_dir, exist_ok=True)

    for table_name in table_names:
        model_name = table_name.lower()
        model_path = os.path.join(target_dir, model_name)
        source_tag = source_system_code.lower()
        query = f"{PREFIX} config(tags=['{source_tag}']) {SUFFIX}\n{PREFIX} generate_model_bronze(this.name,'{source_system_code}','{source_name}') {SUFFIX}"
        with open(f"{model_path}.sql", "w") as f:
            f.write(query)
        logger.info("Generated Bronze model %s.sql", model_name)

    # Build optional data_source_env_var argument for _latest macro
    dsev_arg = ""
    if data_source_env_var != "DANISH_DEMOCRACY_DATA_SOURCE":
        dsev_arg = f",data_source_env_var='{data_source_env_var}'"

    for file_name in file_names:
        file_name = file_name.replace("ø", "oe").replace("æ", "ae").replace("å", "aa").lower()
        # Derive _latest model name from the Bronze model naming convention.
        # For DDD: bronze_{file_name}_latest.  For Rfam: bronze_rfam_{file_name}_latest.
        # We find the matching Bronze model name and append _latest.
        matching = [t for t in table_names if t.lower().endswith(f"_{file_name}")]
        if matching:
            model_name = f"{matching[0].lower()}_latest"
        else:
            model_name = f"bronze_{file_name}_latest"
        model_path = os.path.join(target_dir, model_name)
        source_tag = source_system_code.lower()
        query = f"{PREFIX} config(tags=['{source_tag}']) {SUFFIX}\n{PREFIX} generate_model_bronze_latest('{file_name}','{source_system_code}','{source_name}'{dsev_arg}) {SUFFIX}"
        with open(f"{model_path}.sql", "w") as f:
            f.write(query)
        logger.info("Generated Bronze model %s.sql", model_name)


def generate_dbt_models_silver(
    table_names: list[str],
    incremental_models: frozenset[str] | None = None,
    date_column: str = "opdateringsdato",
    date_column_map: dict[str, str] | None = None,
    data_source_env_var: str = "DANISH_DEMOCRACY_DATA_SOURCE",
    bronze_prefix: str = "silver_ddd_",
    file_name_prefix: str = "",
    primary_key_map: dict[str, str] | None = None,
    source_system_code: str = "DDD",
) -> None:
    """Generate dbt Silver model files (incremental table + ``_cv`` view per entity).

    Args:
        table_names: List of Silver model names (e.g. ``["silver_ddd_aktoer", ...]``).
        incremental_models: Set of model names that use incremental extraction.
            Defaults to ``_INCREMENTAL_SILVER_MODELS_DDD``.
        date_column: Default source timestamp column for ``LKHS_date_inserted_src``.
        date_column_map: Per-table override for the date column.  When a table
            maps to ``""``, the Silver macro derives ``LKHS_date_inserted_src``
            from the filename timestamp.  Falls back to ``date_column``.
        data_source_env_var: Environment variable name for the Bronze data path.
        bronze_prefix: Prefix to strip from Silver model name to get the Bronze
            file name.  Default ``"silver_ddd_"`` works for DDD; for Rfam use
            ``"silver_rfam_"``.
        file_name_prefix: Not used — kept for API symmetry.
        primary_key_map: Mapping from file/table name to its PK column name.
            When ``None``, all tables default to ``id``.
        source_system_code: Short code for the source system (e.g. ``"DDD"``).
            Used to assign a source-system tag to each generated model.
    """
    if incremental_models is None:
        incremental_models = _INCREMENTAL_SILVER_MODELS_DDD

    target_dir = os.path.join(get_variables_from_env.DBT_MODELS_DIRECTORY, "silver")
    os.makedirs(target_dir, exist_ok=True)

    for table_name in table_names:
        model_name = table_name.lower()
        model_path = os.path.join(target_dir, model_name)

        # Derive the Bronze file name from the Silver model name.
        file_name = model_name.replace(bronze_prefix, "", 1)

        # Resolve the primary key and date column for this table.
        pk = primary_key_map.get(file_name, "id") if primary_key_map else "id"
        dc = date_column_map.get(file_name, date_column) if date_column_map else date_column
        pk_columns_bronze = f"'{pk}'"

        # Select the correct macro based on the canonical incremental list.
        if model_name in incremental_models:
            macro_name = "generate_model_silver_incr_extraction"
        else:
            macro_name = "generate_model_silver_full_extraction"

        # Build data_source_env_var argument — only include if non-default
        dsev_arg = ""
        if data_source_env_var != "DANISH_DEMOCRACY_DATA_SOURCE":
            dsev_arg = f",data_source_env_var='{data_source_env_var}'"

        pre_hook_args = f"'{file_name}'"
        post_hook_args = f"'{file_name}'"
        if data_source_env_var != "DANISH_DEMOCRACY_DATA_SOURCE":
            pre_hook_args += f",data_source_env_var='{data_source_env_var}'"
            post_hook_args += f",data_source_env_var='{data_source_env_var}'"

        # Incremental Silver models reference bronze_*_latest inside a
        # conditional block in the macro, so dbt cannot infer the dependency
        # automatically.  Add an explicit -- depends_on hint.
        depends_on_line = ""
        if macro_name == "generate_model_silver_incr_extraction":
            depends_on_line = f"-- depends_on: {PREFIX} ref(bronze_table_name ~ '_latest') {SUFFIX}\n"

        query = (
            f"{PREFIX_STMT} set bronze_table_name = this.name.replace('silver', 'bronze', 1) {SUFFIX_STMT}\n"
            f"{PREFIX_STMT} set base_for_hash = generate_base_for_hash(table_name=bronze_table_name,"
            f"columns_to_exclude=var('bronze_columns_to_exclude_in_silver_hash'),"
            f"primary_key_columns=\"{pk_columns_bronze}\") {SUFFIX_STMT}\n"
            f"{depends_on_line}"
            f"{PREFIX} config( materialized='incremental',incremental_strategy='append',"
            f"on_schema_change='append_new_columns',unique_key=['{pk}','LKHS_date_valid_from'],"
            f"tags=['{source_system_code.lower()}'],\n"
            f"pre_hook  = \"{PREFIX} generate_pre_hook_silver({pre_hook_args}) {SUFFIX}\",\n"
            f"post_hook = \"{PREFIX} generate_post_hook_silver({post_hook_args}) {SUFFIX}\"\n"
            f") {SUFFIX}\n"
            f"{PREFIX} {macro_name}(file_name='{file_name}',bronze_table_name=bronze_table_name,"
            f"primary_key_columns='{pk}',date_column='{dc}',base_for_hash=base_for_hash{dsev_arg}) {SUFFIX}\n"
        )
        with open(f"{model_path}.sql", "w") as f:
            f.write(query)
        logger.info("Generated Silver model %s.sql (macro: %s)", model_name, macro_name)

        query_cv = (
            f"{PREFIX} config( materialized='view',tags=['{source_system_code.lower()}'] ) {SUFFIX}\n"
            f"SELECT src.*\n"
            f"FROM {PREFIX} ref('{model_name}') {SUFFIX} src\n"
            f"QUALIFY ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.{pk} ORDER BY src.LKHS_date_valid_from DESC) = 1\n"
        )
        with open(f"{model_path}_cv.sql", "w") as f:
            f.write(query_cv)
        logger.info("Generated Silver model %s_cv.sql", model_name)


def generate_dbt_models_gold_cv(table_names: list[str]) -> None:
    """Generate dbt Gold ``_cv`` (current-version) model files."""
    target_dir = os.path.join(get_variables_from_env.DBT_MODELS_DIRECTORY, "gold")
    os.makedirs(target_dir, exist_ok=True)

    # 'date' and 'individual_votes' are handcrafted — skip them.
    tables = [t for t in table_names if t not in {"date", "individual_votes"}]

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

    # ── Danish Democracy Data (DDD) ──────────────────────────────────────
    generate_dbt_models_bronze(
        configuration_variables.DANISH_DEMOCRACY_MODELS_BRONZE,
        configuration_variables.DANISH_DEMOCRACY_FILE_NAMES,
        "DDD",
        "danish_parliament",
    )
    generate_dbt_models_silver(
        configuration_variables.DANISH_DEMOCRACY_MODELS_SILVER,
        primary_key_map=configuration_variables.DANISH_DEMOCRACY_TABLE_PRIMARY_KEYS,
    )
    generate_dbt_models_gold_cv(configuration_variables.DANISH_DEMOCRACY_MODELS_GOLD)

    # ── Rfam ─────────────────────────────────────────────────────────────
    generate_dbt_models_bronze(
        configuration_variables.RFAM_MODELS_BRONZE,
        configuration_variables.RFAM_TABLE_NAMES,
        "RFAM",
        "rfam",
        data_source_env_var="RFAM_DATA_SOURCE",
    )
    generate_dbt_models_silver(
        configuration_variables.RFAM_MODELS_SILVER,
        incremental_models=_INCREMENTAL_SILVER_MODELS_RFAM,
        date_column="updated",
        date_column_map=configuration_variables.RFAM_TABLE_DATE_COLUMNS,
        data_source_env_var="RFAM_DATA_SOURCE",
        bronze_prefix="silver_rfam_",
        primary_key_map=configuration_variables.RFAM_TABLE_PRIMARY_KEYS,
        source_system_code="RFAM",
    )
