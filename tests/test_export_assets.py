"""Execute the Delta export assets and the export CLI helpers with stubbed IO."""

import sys
from unittest.mock import MagicMock, patch

import pytest
from dagster import build_asset_context, materialize

import ddd_python.ddd_dlt.export_gold as gold_mod
import ddd_python.ddd_dlt.export_silver as silver_mod
from ddd_python.ddd_dagster import export_assets


class TestExportAssets:
    def test_silver_export_asset_reports_rows_and_closes_connection(self):
        connection = MagicMock()
        asset_def = export_assets._make_export_silver_asset("silver_ddd_aktoer")
        with (
            patch.object(export_assets, "open_export_connection", return_value=connection),
            patch.object(export_assets, "export_single_silver_table", return_value=7) as export,
        ):
            result = materialize([asset_def])
        assert result.success
        export.assert_called_once_with(connection, "silver_ddd_aktoer")
        connection.close.assert_called_once()
        (event,) = result.get_asset_materialization_events()
        assert event.materialization.metadata["rows_written"].value == 7

    def test_gold_export_closes_connection_even_on_failure(self):
        connection = MagicMock()
        asset_def = export_assets._make_export_gold_asset("actor")
        with (
            patch.object(export_assets, "open_export_connection", return_value=connection),
            patch.object(export_assets, "export_single_gold_table", side_effect=OSError("boom")),
            pytest.raises(OSError),
        ):
            asset_def(build_asset_context())  # direct call: avoids the retry-policy delay
        connection.close.assert_called_once()

    def test_barriers_are_noops(self):
        barriers = [
            export_assets.barrier_dbt_gold_complete,
            export_assets.barrier_all_silver_exported,
            export_assets.barrier_all_gold_exported,
        ]
        assert materialize(barriers).success


@pytest.mark.parametrize(
    ("mod", "single", "fn"),
    [
        (silver_mod, "export_single_silver_table", "write_tables_to_delta_silver"),
        (gold_mod, "export_single_gold_table", "write_tables_to_delta_gold"),
    ],
)
class TestWriteTables:
    def test_attempts_all_tables_then_raises(self, mod, single, fn):
        def fake(_conn, table):
            if table == "bad":
                raise ValueError("nope")
            return 1

        with (
            patch.object(mod, single, side_effect=fake) as export,
            pytest.raises(RuntimeError, match="1 table"),
        ):
            getattr(mod, fn)(MagicMock(), ["a", "bad", "c"])
        assert export.call_count == 3  # a failure does not stop the remaining tables

    def test_all_good_does_not_raise(self, mod, single, fn):
        with patch.object(mod, single, return_value=1):
            getattr(mod, fn)(MagicMock(), ["a", "b"])


@pytest.mark.parametrize(
    ("mod", "writer"),
    [(silver_mod, "write_tables_to_delta_silver"), (gold_mod, "write_tables_to_delta_gold")],
)
class TestMain:
    def test_explicit_tables_and_connection_closed(self, mod, writer, monkeypatch):
        connection = MagicMock()
        monkeypatch.setattr(sys, "argv", ["prog", "--tables", "t1", "t2"])
        with (
            patch.object(mod, "open_export_connection", return_value=connection),
            patch.object(mod, writer) as write,
        ):
            mod.main()
        write.assert_called_once_with(connection, ["t1", "t2"])
        connection.close.assert_called_once()

    def test_defaults_to_all_tables_and_closes_on_error(self, mod, writer, monkeypatch):
        connection = MagicMock()
        monkeypatch.setattr(sys, "argv", ["prog"])
        with (
            patch.object(mod, "open_export_connection", return_value=connection),
            patch.object(mod, writer, side_effect=RuntimeError("fail")) as write,
            pytest.raises(RuntimeError),
        ):
            mod.main()
        assert len(write.call_args.args[1]) > 5  # the full configured table list
        connection.close.assert_called_once()


class TestPrepareDeltaRead:
    @pytest.mark.parametrize(
        ("target", "expected"),
        [("onelake", "azure"), ("s3", "httpfs"), ("local", None)],
    )
    def test_loads_extensions_for_target(self, target, expected):
        connection = MagicMock()
        with patch.dict(
            "ddd_python.ddd_dlt.export_silver.get_variables_from_env.__dict__",
            {"STORAGE_TARGET": target, "DUCKDB_DATABASE_LOCATION": "/tmp/x/db.duckdb"},
        ):
            silver_mod._prepare_delta_read(connection)
        statements = " ".join(call.args[0] for call in connection.execute.call_args_list)
        assert "LOAD delta" in statements
        if expected:
            assert f"LOAD {expected}" in statements
        else:
            assert "LOAD httpfs" not in statements and "LOAD azure" not in statements
