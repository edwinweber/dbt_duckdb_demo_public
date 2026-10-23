"""Unit tests for ddd_python.ddd_dlt.duckdb_quack_server.

All tests are unit-level — no live DuckDB quack server is started.  The module
is imported once (module-level global state is reset between tests via monkeypatch
on get_variables_from_env attributes).
"""

import signal
from unittest.mock import MagicMock, patch

import pytest

from ddd_python.ddd_dlt import duckdb_quack_server as server


def _reset_server():
    """Reset server-level mutable state between tests."""
    server._shutdown_requested = False
    server._connection = None


class TestTokenValidation:
    """The server refuses to start if the token is missing or too short."""

    def test_empty_token_raises(self, monkeypatch):
        _reset_server()
        env = MagicMock(
            QUACK_SERVER_TOKEN="",
            QUACK_SERVER_PORT=9494,
            DUCKDB_DATABASE_LOCATION="/tmp/test.duckdb",
            SILVER_STORAGE_FORMAT="duckdb",
        )
        with (
            patch.object(server, "get_variables_from_env", env),
            patch("duckdb.connect"),
            pytest.raises(OSError, match="QUACK_SERVER_TOKEN must be set"),
        ):
            server.main()

    def test_short_token_raises(self, monkeypatch, tmp_path):
        _reset_server()
        env = MagicMock(
            QUACK_SERVER_TOKEN="abc",  # 3 chars < 4 minimum
            QUACK_SERVER_PORT=9494,
            DUCKDB_DATABASE_LOCATION=str(tmp_path / "test.duckdb"),
            SILVER_STORAGE_FORMAT="duckdb",
        )
        with (
            patch.object(server, "get_variables_from_env", env),
            patch("duckdb.connect"),
            pytest.raises(OSError, match="at least 4 characters"),
        ):
            server.main()

    def test_four_char_token_passes_validation(self, tmp_path):
        _reset_server()
        env = MagicMock(
            QUACK_SERVER_TOKEN="abcd",
            QUACK_SERVER_PORT=9494,
            DUCKDB_DATABASE_LOCATION=str(tmp_path / "test.duckdb"),
            SILVER_STORAGE_FORMAT="duckdb",
        )
        mock_conn = MagicMock()

        def _serve_and_stop(sql, *a, **kw):
            if "quack_serve" in sql:
                server._shutdown_requested = True
            return MagicMock()

        mock_conn.execute.side_effect = _serve_and_stop

        with (
            patch.object(server, "get_variables_from_env", env),
            patch("duckdb.connect", return_value=mock_conn),
        ):
            # Must complete without raising
            server.main()


class TestTokenSqlEscaping:
    """Single quotes in the token must be doubled before SQL interpolation."""

    def test_token_single_quote_escaped_in_serve_call(self, tmp_path):
        _reset_server()
        env = MagicMock(
            QUACK_SERVER_TOKEN="it's",
            QUACK_SERVER_PORT=9494,
            DUCKDB_DATABASE_LOCATION=str(tmp_path / "test.duckdb"),
            SILVER_STORAGE_FORMAT="duckdb",
        )
        executed_sqls: list[str] = []

        def _capture(sql, *a, **kw):
            executed_sqls.append(sql)
            if "quack_serve" in sql:
                server._shutdown_requested = True

        mock_conn = MagicMock()
        mock_conn.execute.side_effect = _capture

        with (
            patch.object(server, "get_variables_from_env", env),
            patch("duckdb.connect", return_value=mock_conn),
        ):
            server.main()

        serve_sql = next(s for s in executed_sqls if "quack_serve" in s)
        assert "it''s" in serve_sql, f"Token not SQL-escaped in: {serve_sql}"
        # quack refuses to listen on 0.0.0.0 without this option (server crash-looped without it)
        assert "allow_other_hostname => true" in serve_sql


class TestSignalHandling:
    """SIGTERM / SIGINT must set _shutdown_requested."""

    def test_handle_signal_sets_flag(self):
        server._shutdown_requested = False
        server._handle_signal(signal.SIGTERM, None)
        assert server._shutdown_requested is True


class TestDuckLakeAttach:
    """When SILVER_STORAGE_FORMAT=ducklake the server attaches the catalog."""

    def test_ducklake_attach_called(self, tmp_path):
        _reset_server()
        env = MagicMock(
            QUACK_SERVER_TOKEN="testtoken",
            QUACK_SERVER_PORT=9494,
            DUCKDB_DATABASE_LOCATION=str(tmp_path / "test.duckdb"),
            SILVER_STORAGE_FORMAT="ducklake",
            DUCKLAKE_CATALOG_LOCATION=str(tmp_path / "catalog.ducklake"),
            DUCKLAKE_DATA_PATH="/data/ducklake",
        )
        executed_sqls: list[str] = []

        def _capture(sql, *a, **kw):
            executed_sqls.append(sql)
            if "quack_serve" in sql:
                server._shutdown_requested = True

        mock_conn = MagicMock()
        mock_conn.execute.side_effect = _capture

        with (
            patch.object(server, "get_variables_from_env", env),
            patch("duckdb.connect", return_value=mock_conn),
            patch(
                "ddd_python.ddd_dlt.duckdb_quack_server.init_secrets_on_connection",
                return_value=None,
            ),
        ):
            server.main()

        attach_sqls = [s for s in executed_sqls if "ATTACH" in s]
        assert any("ducklake:" in s for s in attach_sqls)

    def test_missing_catalog_raises(self, tmp_path):
        _reset_server()
        env = MagicMock(
            QUACK_SERVER_TOKEN="testtoken",
            QUACK_SERVER_PORT=9494,
            DUCKDB_DATABASE_LOCATION=str(tmp_path / "test.duckdb"),
            SILVER_STORAGE_FORMAT="ducklake",
            DUCKLAKE_CATALOG_LOCATION="",
            DUCKLAKE_DATA_PATH="/data/ducklake",
        )
        mock_conn = MagicMock()
        mock_conn.execute.return_value = None

        with (
            patch.object(server, "get_variables_from_env", env),
            patch("duckdb.connect", return_value=mock_conn),
            patch(
                "ddd_python.ddd_dlt.duckdb_quack_server.init_secrets_on_connection",
                return_value=None,
            ),
            pytest.raises(OSError, match="DUCKLAKE_CATALOG_LOCATION must be set"),
        ):
            server.main()

    def test_missing_data_path_raises(self, tmp_path):
        _reset_server()
        env = MagicMock(
            QUACK_SERVER_TOKEN="testtoken",
            QUACK_SERVER_PORT=9494,
            DUCKDB_DATABASE_LOCATION=str(tmp_path / "test.duckdb"),
            SILVER_STORAGE_FORMAT="ducklake",
            DUCKLAKE_CATALOG_LOCATION="/data/catalog.ducklake",
            DUCKLAKE_DATA_PATH="",
        )
        mock_conn = MagicMock()
        mock_conn.execute.return_value = None

        with (
            patch.object(server, "get_variables_from_env", env),
            patch("duckdb.connect", return_value=mock_conn),
            patch(
                "ddd_python.ddd_dlt.duckdb_quack_server.init_secrets_on_connection",
                return_value=None,
            ),
            pytest.raises(OSError, match="DUCKLAKE_DATA_PATH must be set"),
        ):
            server.main()
