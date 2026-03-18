"""Tests for the _require helper in get_variables_from_env."""

import os
import pytest


def test_require_returns_value(monkeypatch):
    monkeypatch.setenv("TEST_REQUIRED_VAR", "hello")
    # Import after patching so dotenv doesn't interfere
    from ddd_python.ddd_utils.get_variables_from_env import _require
    assert _require("TEST_REQUIRED_VAR") == "hello"


def test_require_raises_on_missing(monkeypatch):
    monkeypatch.delenv("NONEXISTENT_VAR_12345", raising=False)
    from ddd_python.ddd_utils.get_variables_from_env import _require
    with pytest.raises(EnvironmentError, match="NONEXISTENT_VAR_12345"):
        _require("NONEXISTENT_VAR_12345")


def test_require_raises_on_empty(monkeypatch):
    monkeypatch.setenv("EMPTY_VAR_TEST", "")
    from ddd_python.ddd_utils.get_variables_from_env import _require
    with pytest.raises(EnvironmentError, match="EMPTY_VAR_TEST"):
        _require("EMPTY_VAR_TEST")
