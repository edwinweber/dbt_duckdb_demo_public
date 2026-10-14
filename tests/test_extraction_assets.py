"""Execute the DDD and Rfam extraction assets with a stubbed pipeline resource.

The assets are materialised through Dagster (real context, config and metadata
handling); only ``DltPipelineResource.execute_pipeline`` is replaced, so no
network or dlt run is involved.
"""

from unittest.mock import patch

import pytest
from dagster import build_asset_context, materialize

from ddd_python.ddd_dagster import assets as ddd_assets
from ddd_python.ddd_dagster import rfam_assets
from ddd_python.ddd_dagster.resources import DltPipelineResource


def _run(asset_def, run_config=None):
    with patch.object(
        DltPipelineResource,
        "execute_pipeline",
        return_value={"status": "success", "records_written": 42},
    ) as stub:
        result = materialize(
            [asset_def],
            resources={"dlt_onelake": DltPipelineResource()},
            run_config=run_config,
        )
    assert result.success
    (event,) = result.get_asset_materialization_events()
    metadata = event.materialization.metadata
    return stub, metadata


class TestDddAssets:
    def test_incremental_asset_builds_odata_filter(self):
        asset_def = ddd_assets._make_incremental_asset("Aktør")
        run_config = {
            "ops": {"ingestion__DDD__aktoer": {"config": {"date_to_load_from": "2026-01-15"}}}
        }
        stub, metadata = _run(asset_def, run_config)

        kwargs = stub.call_args.kwargs
        assert kwargs["pipeline_type"] == "api_to_file"
        assert kwargs["source_api_resource"] == "Aktør"
        assert kwargs["source_api_filter"] == (
            "$filter=opdateringsdato ge DateTime'2026-01-15'&$orderby=id"
        )
        assert kwargs["destination_directory_path"] == "Files/Bronze/DDD/aktoer"
        assert kwargs["destination_file_name"].startswith("aktoer_")
        assert metadata["records_written"].value == 42
        assert metadata["date_from"].value == "2026-01-15"

    def test_incremental_asset_rejects_malformed_date(self):
        # Invoked directly (not materialized) so the retry policy does not delay the failure.
        asset_def = ddd_assets._make_incremental_asset("Aktør")
        with (
            patch.object(DltPipelineResource, "execute_pipeline") as stub,
            pytest.raises(ValueError),
        ):
            asset_def(
                build_asset_context(),
                config=ddd_assets.ExtractionConfig(date_to_load_from="2026-01-15' OR 1"),
                dlt_onelake=DltPipelineResource(),
            )
        stub.assert_not_called()

    def test_full_extract_asset_has_no_date_filter(self):
        stub, metadata = _run(ddd_assets._make_full_extract_asset("Afstemningstype"))
        kwargs = stub.call_args.kwargs
        assert kwargs["source_api_filter"] == "$inlinecount=allpages&$orderby=id"
        assert metadata["records_written"].value == 42
        assert metadata["pipeline_status"].value == "success"


class TestRfamAssets:
    def test_incremental_asset_passes_sql_params(self):
        asset_def = rfam_assets._make_incremental_asset("family")
        run_config = {
            "ops": {"ingestion__RFAM__family": {"config": {"date_to_load_from": "2026-02-01"}}}
        }
        stub, metadata = _run(asset_def, run_config)

        kwargs = stub.call_args.kwargs
        assert kwargs["pipeline_type"] == "sql_to_file"
        assert kwargs["destination_directory_path"] == "Files/Bronze/RFAM/family"
        assert kwargs["sql_params"] is not None
        assert metadata["date_from"].value == "2026-02-01"

    def test_full_extract_asset_runs_without_params(self):
        table = "clan"
        stub, metadata = _run(rfam_assets._make_full_extract_asset(table))
        kwargs = stub.call_args.kwargs
        assert "sql_params" not in kwargs
        assert kwargs["destination_file_name"].startswith(f"{table}_")
        assert metadata["records_written"].value == 42

    @pytest.mark.parametrize("table", ["family", "genome"])
    def test_incremental_tables_are_configured(self, table):
        assert table in rfam_assets._INCREMENTAL_NAMES
