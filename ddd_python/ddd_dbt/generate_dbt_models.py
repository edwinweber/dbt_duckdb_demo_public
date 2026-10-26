"""Generate dbt model SQL files for Bronze, Silver, and Gold layers.

Reads entity lists from :mod:`ddd_python.ddd_utils.configuration_variables`
so that adding a new entity only requires updating that single file.

Usage::

    python -m ddd_python.ddd_dbt.generate_dbt_models
"""

import logging
from pathlib import Path

from ddd_python.ddd_utils import configuration_variables, get_variables_from_env
from ddd_python.ddd_utils.primary_keys import (
    PrimaryKey,
    is_composite,
    key_columns,
    stored_key_column,
)
from ddd_python.ddd_utils.string_utils import normalize_danish_name

logger = logging.getLogger(__name__)

PREFIX = "{{"
SUFFIX = "}}"
PREFIX_STMT = "{%-"
SUFFIX_STMT = "-%}"
# Build the set of Silver model names that use incremental extraction,
# derived from the canonical list in configuration_variables — NOT hardcoded.
_INCREMENTAL_SILVER_MODELS_DDD: frozenset[str] = frozenset(
    f"silver_ddd_{normalize_danish_name(name)}"
    for name in configuration_variables.DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL
)

_INCREMENTAL_SILVER_MODELS_RFAM: frozenset[str] = frozenset(
    f"silver_rfam_{name}" for name in configuration_variables.RFAM_TABLE_NAMES_INCREMENTAL
)


def generate_dbt_models_bronze(
    table_names: list[str],
    source_system_code: str = "DDD",
    source_name: str = "danish_parliament",
    primary_key_map: dict[str, PrimaryKey] | None = None,
    bronze_prefix: str = "bronze_ddd_",
) -> None:
    """Generate dbt Bronze model files (one view per entity).

    Args:
        table_names: Bronze model names (e.g. ``["bronze_ddd_aktoer", ...]``).
        source_system_code: Short code for the source system (e.g. ``"DDD"``).
        source_name: dbt source name matching ``__sources*.yml``.
        primary_key_map: Mapping from file/table name to its primary key.  A key of
            several columns makes the view add ``LKHS_primary_key_1_column``.
        bronze_prefix: Prefix to strip from the model name to get the file name.
    """
    models_dir = get_variables_from_env.DBT_MODELS_DIRECTORY
    if models_dir is None:
        raise OSError("DBT_MODELS_DIRECTORY must be set")
    target_dir = Path(models_dir) / "bronze"
    target_dir.mkdir(parents=True, exist_ok=True)

    for table_name in table_names:
        model_name = table_name.lower()
        model_path = target_dir / model_name
        source_tag = source_system_code.lower()
        pk = (primary_key_map or {}).get(model_name.replace(bronze_prefix, "", 1), "id")
        pk_arg = f",primary_key_columns={key_columns(pk)!r}" if is_composite(pk) else ""
        query = f"{PREFIX} config(tags=['{source_tag}']) {SUFFIX}\n{PREFIX} generate_model_bronze(this.name,'{source_system_code}','{source_name}'{pk_arg}) {SUFFIX}"  # noqa: E501 — SQL template string
        with open(f"{model_path}.sql", "w") as f:
            f.write(query)
        logger.info("Generated Bronze model %s.sql", model_name)


def generate_dbt_models_silver(
    table_names: list[str],
    incremental_models: frozenset[str] | None = None,
    data_source_env_var: str = "DANISH_DEMOCRACY_DATA_SOURCE",
    bronze_prefix: str = "silver_ddd_",
    primary_key_map: dict[str, PrimaryKey] | None = None,
    source_system_code: str = "DDD",
) -> None:
    """Generate dbt Silver model files (incremental table + ``_cv`` view per entity).

    Args:
        table_names: List of Silver model names (e.g. ``["silver_ddd_aktoer", ...]``).
        incremental_models: Set of model names that use incremental extraction.
            Defaults to ``_INCREMENTAL_SILVER_MODELS_DDD``.
        data_source_env_var: Environment variable name for the Bronze data path.
        bronze_prefix: Prefix to strip from Silver model name to get the Bronze
            file name.  Default ``"silver_ddd_"`` works for DDD; for Rfam use
            ``"silver_rfam_"``.
        primary_key_map: Mapping from file/table name to its primary key: a column
            name, or a list of columns for a key of several columns, which Silver keys
            on ``LKHS_primary_key_1_column``.  When ``None``, all tables default to ``id``.
        source_system_code: Short code for the source system (e.g. ``"DDD"``).
            Used to assign a source-system tag to each generated model.
    """
    if incremental_models is None:
        incremental_models = _INCREMENTAL_SILVER_MODELS_DDD

    models_dir = get_variables_from_env.DBT_MODELS_DIRECTORY
    if models_dir is None:
        raise OSError("DBT_MODELS_DIRECTORY must be set")
    target_dir = Path(models_dir) / "silver"
    target_dir.mkdir(parents=True, exist_ok=True)

    for table_name in table_names:
        model_name = table_name.lower()
        model_path = target_dir / model_name

        # Derive the Bronze file name from the Silver model name.
        file_name = model_name.replace(bronze_prefix, "", 1)

        # Resolve the primary key for this table: the CDC keys on one column, the
        # column itself or LKHS_primary_key_1_column for a key of several columns.
        # The hash leaves out every key column.
        primary_key = primary_key_map.get(file_name, "id") if primary_key_map else "id"
        pk = stored_key_column(primary_key)
        hash_excluded = key_columns(primary_key) + ([pk] if is_composite(primary_key) else [])
        pk_columns_bronze = ",".join(f"'{c}'" for c in hash_excluded)

        # Select the correct macro based on the canonical incremental list.
        if model_name in incremental_models:
            macro_name = "generate_model_silver_incr_extraction"
        else:
            macro_name = "generate_model_silver_full_extraction"

        # Build data_source_env_var argument — only include if non-default
        dsev_arg = ""
        if data_source_env_var != "DANISH_DEMOCRACY_DATA_SOURCE":
            dsev_arg = f",data_source_env_var='{data_source_env_var}'"
        # A key of several columns: Silver builds the key again from the columns cast to
        # its own types (silver_bronze_columns); a key of one column gets no argument.
        if is_composite(primary_key):
            dsev_arg += f",key_source_columns={key_columns(primary_key)!r}"

        query = (
            f"{PREFIX_STMT} set bronze_table_name = this.name.replace('silver', 'bronze', 1) {SUFFIX_STMT}\n"  # noqa: E501 — SQL template string
            f"{PREFIX_STMT} set base_for_hash = generate_base_for_hash(table_name=bronze_table_name,"  # noqa: E501 — SQL template string
            f"columns_to_exclude=var('bronze_columns_to_exclude_in_silver_hash'),"
            f'primary_key_columns="{pk_columns_bronze}") {SUFFIX_STMT}\n'
            f"{PREFIX} config( materialized='incremental',incremental_strategy='append',"
            f"on_schema_change='append_new_columns',unique_key=['{pk}','LKHS_date_valid_from'],"
            f"tags=['{source_system_code.lower()}'] ) {SUFFIX}\n"
            f"{PREFIX} {macro_name}(file_name='{file_name}',bronze_table_name=bronze_table_name,"
            f"primary_key_columns='{pk}',base_for_hash=base_for_hash{dsev_arg}) {SUFFIX}\n"  # noqa: E501 — SQL template string
        )
        with open(f"{model_path}.sql", "w") as f:
            f.write(query)
        logger.info("Generated Silver model %s.sql (macro: %s)", model_name, macro_name)

        query_cv = (
            f"{PREFIX} config( materialized='view',tags=['{source_system_code.lower()}'] ) {SUFFIX}\n"  # noqa: E501 — SQL template string
            f"SELECT src.*\n"
            f"FROM {PREFIX} ref('{model_name}') {SUFFIX} src\n"
            f"QUALIFY ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.{pk} ORDER BY src.LKHS_date_valid_from DESC) = 1\n"  # noqa: E501 — SQL template string
        )
        with open(f"{model_path}_cv.sql", "w") as f:
            f.write(query_cv)
        logger.info("Generated Silver model %s_cv.sql", model_name)


def generate_dbt_models_gold_cv(table_names: list[str]) -> None:
    """Generate dbt Gold ``_cv`` (current-version) model files."""
    models_dir = get_variables_from_env.DBT_MODELS_DIRECTORY
    if models_dir is None:
        raise OSError("DBT_MODELS_DIRECTORY must be set")
    target_dir = Path(models_dir) / "gold"
    target_dir.mkdir(parents=True, exist_ok=True)

    # 'date' and 'individual_votes' are handcrafted — skip them.
    tables = [t for t in table_names if t not in {"date", "individual_votes"}]

    for table_name in tables:
        model_name = table_name.lower()
        model_path = target_dir / model_name

        query = (
            f"SELECT src.* EXCLUDE (LKHS_date_valid_from,LKHS_date_valid_to,LKHS_row_version)\n"  # noqa: E501 — SQL template string
            f"FROM {PREFIX} ref('{table_name}') {SUFFIX} src\n"
            f"QUALIFY ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.{table_name}_bk ORDER BY src.LKHS_date_valid_from DESC) = 1\n"  # noqa: E501 — SQL template string
        )
        with open(f"{model_path}_cv.sql", "w") as f:
            f.write(query)
        logger.info("Generated Gold model %s_cv.sql", model_name)


def generate_dbt_test_silver_no_duplicate(
    table_names: list[str],
    test_name: str = "silver_no_duplicate_id_date_valid_from",
    primary_key_map: dict[str, PrimaryKey] | None = None,
    bronze_prefix: str = "silver_ddd_",
) -> None:
    """Generate the singular test asserting Silver SCD2 keys are unique.

    Emits ``dbt/tests/<test_name>.sql`` which checks that no
    ``(id, LKHS_date_valid_from)`` pair is duplicated across the DDD Silver
    tables.  The table list is written as a **static** Jinja literal (derived
    from *table_names*) rather than from ``graph.nodes`` so that dbt can infer
    the test's ``ref()`` dependencies at parse time — ``graph.nodes`` is only
    populated at run time, which leaves the dependencies undeclared and breaks
    ``dbt build``.

    Args:
        table_names: DDD Silver base-model names (e.g. ``["silver_ddd_aktoer", ...]``).
            Rfam tables are excluded by the caller; their key is tested per model.
        primary_key_map: Mapping from file name to its primary key; each table is grouped
            by the column its CDC keys on (``id``, or ``LKHS_primary_key_1_column``).
        bronze_prefix: Prefix to strip from the model name to get the file name.
    """
    project_dir = get_variables_from_env.DBT_PROJECT_DIRECTORY
    if project_dir is None:
        raise OSError("DBT_PROJECT_DIRECTORY must be set")
    target_dir = Path(project_dir) / "tests"
    target_dir.mkdir(parents=True, exist_ok=True)

    def _key(name: str) -> str:
        pk = (primary_key_map or {}).get(name.lower().replace(bronze_prefix, "", 1), "id")
        return stored_key_column(pk)

    list_lines = "\n".join(
        f"    ('{name.lower()}', '{_key(name)}')," for name in sorted(table_names)
    )

    header = (
        "-- AUTO-GENERATED by ddd_python.ddd_dbt.generate_dbt_models — do not edit by hand.\n"
        "-- Regenerate with: python -m ddd_python.ddd_dbt.generate_dbt_models\n"
        "--\n"
        "-- Verify the SCD2 unique key constraint: no duplicate (key, LKHS_date_valid_from)\n"
        "-- pairs across the DDD Silver tables (test passes when it returns no rows). The key\n"
        "-- is the column the CDC keys on: id, or LKHS_primary_key_1_column for a key of\n"
        "-- several columns.\n"
        "--\n"
        "-- The table list is a static literal (from\n"
        "-- configuration_variables.DANISH_DEMOCRACY_MODELS_SILVER) so dbt can infer the\n"
        "-- test's ref() dependencies at parse time; graph.nodes is only populated at run\n"
        "-- time. Rfam Silver tables are excluded; their keys are tested per model.\n"
    )
    body = (
        "{% set silver_ddd_tables = [\n"
        f"{list_lines}\n"
        "] %}\n\n"
        "{% for table, key in silver_ddd_tables %}\n"
        "SELECT '{{ table }}' AS table_name, CAST({{ key }} AS VARCHAR) AS key_value,"
        " LKHS_date_valid_from, COUNT(*) AS cnt\n"
        "FROM {{ ref(table) }}\n"
        "GROUP BY {{ key }}, LKHS_date_valid_from\n"
        "HAVING COUNT(*) > 1\n"
        "    {% if not loop.last %}UNION ALL{% endif %}\n"
        "{% endfor %}\n"
    )

    with open(target_dir / f"{test_name}.sql", "w") as f:
        f.write(header + "\n" + body)
    logger.info("Generated dbt test %s.sql", test_name)


def generate_dbt_model_silver_load_history(
    silver_tables: list[tuple[str, str]],
    model_name: str = "silver_load_history",
) -> None:
    """Generate the data_engineering view of the rows every dbt run wrote to each Silver table.

    Emits ``models/data_engineering/<model_name>.sql``, which calls the macro
    ``silver_load_history`` with every Silver table as a static Jinja literal, so that dbt
    infers the ``ref()`` dependencies at parse time (see
    :func:`generate_dbt_test_silver_no_duplicate`).

    Args:
        silver_tables: ``(Silver base-model name, source system code)`` pairs, e.g.
            ``[("silver_ddd_aktoer", "DDD"), ("silver_rfam_family", "RFAM")]``.
    """
    models_dir = get_variables_from_env.DBT_MODELS_DIRECTORY
    if models_dir is None:
        raise OSError("DBT_MODELS_DIRECTORY must be set")
    target_dir = Path(models_dir) / "data_engineering"
    target_dir.mkdir(parents=True, exist_ok=True)

    list_lines = "\n".join(
        f"    ('{name.lower()}', '{code}')," for name, code in sorted(silver_tables)
    )
    header = (
        "-- AUTO-GENERATED by ddd_python.ddd_dbt.generate_dbt_models — do not edit by hand.\n"
        "-- Regenerate with: python -m ddd_python.ddd_dbt.generate_dbt_models\n"
        "--\n"
        "-- One row per Silver table and dbt run: the rows it inserted, updated and deleted and\n"
        "-- the Bronze files it processed, read from Silver itself (macro silver_load_history).\n"
        "-- The table list is a static literal (from configuration_variables) so dbt can infer\n"
        "-- the ref() dependencies at parse time.\n"
    )
    body = (
        "{{ config(materialized='view') }}\n"
        "{% set silver_tables = [\n"
        f"{list_lines}\n"
        "] %}\n"
        "{{ silver_load_history(silver_tables) }}\n"
    )
    with open(target_dir / f"{model_name}.sql", "w") as f:
        f.write(header + "\n" + body)
    logger.info("Generated data_engineering model %s.sql", model_name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # ── Danish Democracy Data (DDD) ──────────────────────────────────────
    generate_dbt_models_bronze(
        configuration_variables.DANISH_DEMOCRACY_MODELS_BRONZE,
        "DDD",
        "danish_parliament",
        primary_key_map=configuration_variables.DANISH_DEMOCRACY_TABLE_PRIMARY_KEYS,
    )
    generate_dbt_models_silver(
        configuration_variables.DANISH_DEMOCRACY_MODELS_SILVER,
        primary_key_map=configuration_variables.DANISH_DEMOCRACY_TABLE_PRIMARY_KEYS,
    )
    generate_dbt_models_gold_cv(configuration_variables.DANISH_DEMOCRACY_MODELS_GOLD)
    # Singular test over the DDD Silver tables (Rfam excluded — see docstring).
    generate_dbt_test_silver_no_duplicate(
        configuration_variables.DANISH_DEMOCRACY_MODELS_SILVER,
        primary_key_map=configuration_variables.DANISH_DEMOCRACY_TABLE_PRIMARY_KEYS,
    )

    # ── Rfam ─────────────────────────────────────────────────────────────
    generate_dbt_models_bronze(
        configuration_variables.RFAM_MODELS_BRONZE,
        "RFAM",
        "rfam",
        primary_key_map=configuration_variables.RFAM_TABLE_PRIMARY_KEYS,
        bronze_prefix="bronze_rfam_",
    )
    generate_dbt_models_silver(
        configuration_variables.RFAM_MODELS_SILVER,
        incremental_models=_INCREMENTAL_SILVER_MODELS_RFAM,
        data_source_env_var="RFAM_DATA_SOURCE",
        bronze_prefix="silver_rfam_",
        primary_key_map=configuration_variables.RFAM_TABLE_PRIMARY_KEYS,
        source_system_code="RFAM",
    )

    # ── Both sources ─────────────────────────────────────────────────────
    generate_dbt_model_silver_load_history(
        [(name, "DDD") for name in configuration_variables.DANISH_DEMOCRACY_MODELS_SILVER]
        + [(name, "RFAM") for name in configuration_variables.RFAM_MODELS_SILVER]
    )
