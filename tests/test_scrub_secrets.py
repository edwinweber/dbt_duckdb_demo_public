"""Tests for the _scrub_secrets helper in dlt_pipeline_execution_functions."""

from ddd_python.ddd_dlt.dlt_pipeline_execution_functions import _scrub_secrets


def test_redacts_secret_key():
    assert _scrub_secrets({"client_secret": "abc123"}) == {"client_secret": "***"}


def test_redacts_connection_string():
    assert _scrub_secrets({"connection_string": "Server=x;Password=y"}) == {"connection_string": "***"}


def test_redacts_password_key():
    assert _scrub_secrets({"db_password": "hunter2"}) == {"db_password": "***"}


def test_redacts_token_key():
    assert _scrub_secrets({"api_token": "tok-xyz"}) == {"api_token": "***"}


def test_case_insensitive():
    assert _scrub_secrets({"API_TOKEN": "tok"}) == {"API_TOKEN": "***"}
    assert _scrub_secrets({"Client_Secret": "s"}) == {"Client_Secret": "***"}


def test_preserves_non_sensitive():
    params = {"pipeline_name": "test", "url": "https://example.com"}
    assert _scrub_secrets(params) == params


def test_empty_dict():
    assert _scrub_secrets({}) == {}


def test_mixed_keys():
    params = {"name": "pipeline", "secret": "s3cr3t", "count": 42}
    result = _scrub_secrets(params)
    assert result == {"name": "pipeline", "secret": "***", "count": 42}
