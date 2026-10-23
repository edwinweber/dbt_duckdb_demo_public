"""Only full extractions of incremental entities get the ``_full`` file name suffix.

Silver deletes a key of an incremental entity only when it is missing from a ``_full`` file,
so a wrong name either hides deletes or invents them.  The extraction CLIs build their tasks
and the Dagster assets run with a stubbed pipeline; nothing is extracted.
"""

from unittest.mock import patch

import pytest
from dagster import materialize

from ddd_python.ddd_dagster import assets as ddd_assets
from ddd_python.ddd_dagster import rfam_assets
from ddd_python.ddd_dagster.resources import DltPipelineResource
from ddd_python.ddd_dlt import (
    dlt_run_extraction_pipelines_danish_parliament_data as ddd_cli,
)
from ddd_python.ddd_dlt import dlt_run_extraction_pipelines_rfam as rfam_cli

FULL_DATE = "1900-01-01"
LOOKBACK_DATE = "2026-01-15"


def _file_names_of_cli(module, run, date, names):
    with patch.object(module.dpef, "run_extraction_pool") as pool:
        run(date_to_load_from=date, **names)
    return {
        task["name"]: task["kwargs"]["destination_file_name"]
        for task in pool.call_args.kwargs["tasks"]
    }


def _file_name_of_asset(asset_def, key, date):
    run_config = {"ops": {key: {"config": {"date_to_load_from": date}}}} if date else None
    with patch.object(
        DltPipelineResource,
        "execute_pipeline",
        return_value={"status": "success", "records_written": 1},
    ) as stub:
        assert materialize(
            [asset_def], resources={"dlt_onelake": DltPipelineResource()}, run_config=run_config
        ).success
    return stub.call_args.kwargs["destination_file_name"]


class TestCli:
    def test_ddd_incremental_entity_from_the_full_date_is_full(self):
        names = _file_names_of_cli(
            ddd_cli,
            ddd_cli.run_extraction_pipelines_danish_parliament_data,
            FULL_DATE,
            {"file_names_to_retrieve": ["Aktør", "Periode"]},
        )
        assert names["Aktør"].endswith("_full.json")
        assert names["Periode"].count("_full") == 0  # full-extract entity: every file is complete

    def test_ddd_incremental_entity_with_a_lookback_date_is_plain(self):
        names = _file_names_of_cli(
            ddd_cli,
            ddd_cli.run_extraction_pipelines_danish_parliament_data,
            LOOKBACK_DATE,
            {"file_names_to_retrieve": ["Aktør"]},
        )
        assert "_full" not in names["Aktør"]

    def test_ddd_default_run_is_plain(self):
        names = _file_names_of_cli(
            ddd_cli,
            ddd_cli.run_extraction_pipelines_danish_parliament_data,
            None,
            {"file_names_to_retrieve": ["Aktør"]},
        )
        assert "_full" not in names["Aktør"]

    def test_rfam_incremental_table_from_the_full_date_is_full(self):
        names = _file_names_of_cli(
            rfam_cli,
            rfam_cli.run_extraction_pipelines_rfam,
            FULL_DATE,
            {"table_names_to_retrieve": ["family", "clan"]},
        )
        assert names["family"].endswith("_full.json")
        assert "_full" not in names["clan"]


class TestAssets:
    def test_ddd_incremental_asset(self):
        asset_def = ddd_assets._make_incremental_asset("Aktør")
        assert _file_name_of_asset(asset_def, "ingestion__DDD__aktoer", FULL_DATE).endswith(
            "_full.json"
        )
        assert "_full" not in _file_name_of_asset(
            asset_def, "ingestion__DDD__aktoer", LOOKBACK_DATE
        )
        assert "_full" not in _file_name_of_asset(asset_def, "ingestion__DDD__aktoer", None)

    def test_rfam_incremental_asset(self):
        asset_def = rfam_assets._make_incremental_asset("family")
        assert _file_name_of_asset(asset_def, "ingestion__RFAM__family", FULL_DATE).endswith(
            "_full.json"
        )
        assert "_full" not in _file_name_of_asset(
            asset_def, "ingestion__RFAM__family", LOOKBACK_DATE
        )

    @pytest.mark.parametrize(
        ("factory", "entity"),
        [
            (ddd_assets._make_full_extract_asset, "Afstemningstype"),
            (rfam_assets._make_full_extract_asset, "clan"),
        ],
    )
    def test_full_extract_assets_never_get_the_suffix(self, factory, entity):
        assert "_full" not in _file_name_of_asset(factory(entity), "unused", None)
