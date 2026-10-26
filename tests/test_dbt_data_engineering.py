"""Real dbt runs of the data_engineering models over a Dagster instance of their own.

Materializes a few assets in a temporary ``DAGSTER_HOME`` with the storage layout of the
Docker entrypoint (``storage: sqlite``), then builds the data_engineering models over it and
checks the row counts they read from the materialization metadata: ``records_written``
(extraction), ``rows_written`` (export) and the ``silver_*`` counts of a Silver table
(``dbt_assets.stream_with_silver_row_counts``).  A dbt model reports none; its first
metadata entry, ``Execution Duration``, must not be read as a row count.

Skipped unless ``DBT_INTEGRATION_TESTS=1`` and the ``dbt`` binary is on ``PATH``.
"""

import os
import shutil

import pytest
from dagster import AssetKey, DagsterInstance, MetadataValue, Output, asset, materialize

import duckdb
from ddd_python.ddd_dagster.dbt_assets import silver_row_count_metadata
from tests.test_dbt_silver import SilverProject

pytestmark = [
    pytest.mark.dbt_integration,
    pytest.mark.skipif(
        os.getenv("DBT_INTEGRATION_TESTS") != "1" or shutil.which("dbt") is None,
        reason="real dbt runs: set DBT_INTEGRATION_TESTS=1 and put the dbt binary on PATH",
    ),
]


@asset(key=AssetKey(["ingestion", "DDD", "aktoer"]))
def _extraction():
    return Output(None, metadata={"records_written": MetadataValue.int(42)})


@asset(key=AssetKey(["silver", "silver_ddd_aktoer"]))
def _silver_table():
    metadata = {"Execution Duration": MetadataValue.float(12.6)}
    metadata |= silver_row_count_metadata({"I": 3, "U": 2, "D": 1, "files": 2})
    return Output(None, metadata=metadata)


@asset(key=AssetKey(["gold", "actor"]))
def _gold_view():
    return Output(None, metadata={"Execution Duration": MetadataValue.float(12.6)})


@asset(key=AssetKey(["export", "silver", "silver_ddd_aktoer"]))
def _export():
    return Output(None, metadata={"rows_written": MetadataValue.int(6)})


def test_data_engineering_models_read_the_row_counts(tmp_path):
    home = tmp_path / "dagster"
    home.mkdir()
    (home / "dagster.yaml").write_text(f"storage:\n  sqlite:\n    base_dir: {home}\n")
    with DagsterInstance.from_config(str(home)) as instance:
        result = materialize([_extraction, _silver_table, _gold_view, _export], instance=instance)
    assert result.success

    root = tmp_path / "dbt"
    root.mkdir()
    p = SilverProject(root, "duckdb", "sagskategori")
    p.env["DAGSTER_HOME"] = str(home)
    run = p.dbt("run", "--select", "data_engineering", "--exclude", "silver_load_history")
    assert run.returncode == 0, run.stdout[-3000:]

    columns = ("rows_processed, silver_rows_inserted, silver_rows_updated, silver_rows_deleted, "
               "silver_rows_written, silver_files_processed")  # fmt: skip
    expected = {
        "ingestion/DDD/aktoer": (42, None, None, None, None, None),
        "silver/silver_ddd_aktoer": (6, 3, 2, 1, 6, 2),
        "gold/actor": (None, None, None, None, None, None),
        "export/silver/silver_ddd_aktoer": (6, None, None, None, None, None),
    }
    schema = "test.main_data_engineering"
    events = p.sql(f"SELECT asset_key_full, {columns} FROM {schema}.dagster_event_logs")
    assert {r[0]: r[1:] for r in events} == expected
    facts = p.sql(
        f"SELECT array_to_string(CAST(asset_key AS VARCHAR[]), '/'), {columns} "
        f"FROM {schema}.dagster_asset_materialization"
    )
    assert {r[0]: r[1:] for r in facts} == expected
    runs = p.sql(
        f"SELECT rows_processed, silver_rows_inserted, silver_rows_updated, silver_rows_deleted, "
        f"silver_rows_written, silver_files_processed, assets_materialized FROM {schema}.dagster_run"
    )
    assert runs == [(42 + 6 + 6, 3, 2, 1, 6, 2, 4)]


@pytest.mark.parametrize("storage_format", ["duckdb", "ducklake"])
def test_silver_load_history_counts_every_run_from_silver(tmp_path, storage_format):
    """silver_load_history has one row per Silver table and dbt run: a first build of all 25
    tables, a run with a change and a delete, and a run whose file changes nothing (no rows,
    one file).  Per table its rows_written add up to the table's rows.  In DuckLake mode the
    tables and their _files logs are in the DuckLake catalog."""
    p = SilverProject(tmp_path, storage_format, "sagskategori")
    rows = p.base_rows()
    first_files = len(list(p.entity_dir.glob("*.json*")))
    schema = "test.main_data_engineering"
    silver = "ducklake_catalog.main_silver" if storage_format == "ducklake" else "test.main_silver"

    def sql(query: str) -> list[tuple]:
        """Like SilverProject.sql, with the catalog attached under the name the views use."""
        con = duckdb.connect(str(tmp_path / "test.duckdb"), read_only=True)
        try:
            if storage_format == "ducklake":
                con.execute("INSTALL ducklake; LOAD ducklake")
                catalog = tmp_path / "catalog.ducklake"
                con.execute(f"ATTACH 'ducklake:{catalog}' AS ducklake_catalog (READ_ONLY)")
            return con.execute(query).fetchall()
        finally:
            con.close()

    def build() -> None:
        for args in (["seed"], ["run", "--select", "+date", "+time", "+silver_load_history"]):
            result = p.dbt(*args)
            assert result.returncode == 0, result.stdout[-3000:]

    def history() -> list[tuple]:
        return sql(
            "SELECT rows_inserted, rows_updated, rows_deleted, rows_written, files_processed "
            f"FROM {schema}.silver_load_history WHERE silver_table = '{p.model}' ORDER BY loaded_at"
        )

    build()
    assert history() == [(len(rows), 0, 0, len(rows), first_files)]
    day2 = [p.change(rows[0]), *rows[2:]]  # rows[0] changed, rows[1] deleted
    p.write(2, day2)
    p.run()
    p.write(3, day2)  # nothing changes
    p.run()
    build()  # normal runs now: only sagskategori has new files, and they were processed
    assert history() == [
        (len(rows), 0, 0, len(rows), first_files),
        (0, 1, 1, 2, 1),
        (0, 0, 0, 0, 1),
    ]
    totals = dict(
        sql(f"SELECT silver_table, SUM(rows_written) FROM {schema}.silver_load_history GROUP BY 1")
    )
    assert len(totals) == 25
    for table, written in totals.items():
        assert written == sql(f"SELECT COUNT(*) FROM {silver}.{table}")[0][0], table
    result = p.dbt("test", "--select", "silver_load_history")
    assert result.returncode == 0, result.stdout[-3000:]
