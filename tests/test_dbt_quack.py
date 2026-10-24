"""Tests for the dbt-quack wrapper (ddd_python.ddd_dbt.dbt_quack): the patches are
applied only with QUACK_SERVER_HOST, the exit code follows dbt's result, and
QUACK_SKIP_PATCHES is parsed."""

import sys
from unittest.mock import MagicMock, patch

import pytest


class TestDbtQuackPatchGuard:
    """Patches are only applied when QUACK_SERVER_HOST is set."""

    def test_no_patches_applied_without_quack_host(self, monkeypatch):
        monkeypatch.delenv("QUACK_SERVER_HOST", raising=False)
        # Import fresh each time so module-level state doesn't bleed through.
        for mod in list(sys.modules):
            if "ddd_python.ddd_dbt.dbt_quack" in mod:
                del sys.modules[mod]
        from ddd_python.ddd_dbt import dbt_quack

        # _apply_patches should not be called when QUACK_SERVER_HOST is absent.
        with (
            patch.object(dbt_quack, "_apply_patches") as mock_apply,
            patch("dbt.cli.main.dbtRunner") as mock_runner,
        ):
            mock_runner.return_value.invoke.return_value = MagicMock(success=True)
            with pytest.raises(SystemExit) as exc_info:
                dbt_quack.main()
        assert exc_info.value.code == 0
        mock_apply.assert_not_called()

    def test_patches_applied_with_quack_host(self, monkeypatch):
        monkeypatch.setenv("QUACK_SERVER_HOST", "duckdb_server")
        for mod in list(sys.modules):
            if "ddd_python.ddd_dbt.dbt_quack" in mod:
                del sys.modules[mod]
        from ddd_python.ddd_dbt import dbt_quack

        with (
            patch.object(dbt_quack, "_apply_patches") as mock_apply,
            patch("dbt.cli.main.dbtRunner") as mock_runner,
        ):
            mock_runner.return_value.invoke.return_value = MagicMock(success=True)
            with pytest.raises(SystemExit) as exc_info:
                dbt_quack.main()
        assert exc_info.value.code == 0
        mock_apply.assert_called_once()

    def test_main_exits_nonzero_on_dbt_failure(self, monkeypatch):
        monkeypatch.setenv("QUACK_SERVER_HOST", "duckdb_server")
        for mod in list(sys.modules):
            if "ddd_python.ddd_dbt.dbt_quack" in mod:
                del sys.modules[mod]
        from ddd_python.ddd_dbt import dbt_quack

        with (
            patch.object(dbt_quack, "_apply_patches"),
            patch("dbt.cli.main.dbtRunner") as mock_runner,
        ):
            mock_runner.return_value.invoke.return_value = MagicMock(success=False)
            with pytest.raises(SystemExit) as exc_info:
                dbt_quack.main()
        assert exc_info.value.code == 1


class TestDbtQuackWarningWithoutHost:
    """A warning is emitted when dbt-quack is called without QUACK_SERVER_HOST."""

    def test_warning_emitted_when_no_quack_host(self, monkeypatch):
        monkeypatch.delenv("QUACK_SERVER_HOST", raising=False)
        for mod in list(sys.modules):
            if "ddd_python.ddd_dbt.dbt_quack" in mod:
                del sys.modules[mod]
        from ddd_python.ddd_dbt import dbt_quack

        with patch("dbt.cli.main.dbtRunner") as mock_runner:
            mock_runner.return_value.invoke.return_value = MagicMock(success=True)
            with (
                pytest.warns(UserWarning, match="QUACK_SERVER_HOST is not set"),
                pytest.raises(SystemExit),
            ):
                dbt_quack.main()


class TestDbtQuackSkipPatches:
    """QUACK_SKIP_PATCHES prevents individual patches from being applied."""

    def test_skip_set_parsed_correctly(self, monkeypatch):
        monkeypatch.setenv("QUACK_SERVER_HOST", "server")
        monkeypatch.setenv("QUACK_SKIP_PATCHES", "connect,is_ducklake")
        for mod in list(sys.modules):
            if "ddd_python.ddd_dbt.dbt_quack" in mod:
                del sys.modules[mod]
        import importlib

        dbt_quack = importlib.import_module("ddd_python.ddd_dbt.dbt_quack")
        assert "connect" in dbt_quack._SKIP
        assert "is_ducklake" in dbt_quack._SKIP
        assert "per_thread" not in dbt_quack._SKIP
