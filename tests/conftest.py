"""Shared test fixtures for the Danish Democracy Data test suite."""

import pyarrow.dataset  # ensure it's in sys.modules before any patch.dict runs
import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture
def mock_fabric_clients():
    """Mock the get_fabric_onelake_clients module via sys.modules.

    Intercepts ``from ddd_python.ddd_utils import get_fabric_onelake_clients``
    (which is imported inside functions, not at module level) and replaces it
    with a MagicMock that returns a fake token.
    """
    mock = MagicMock()
    mock.get_fabric_token.return_value = "fake-token"
    with patch.dict("sys.modules", {"ddd_python.ddd_utils.get_fabric_onelake_clients": mock}):
        yield mock
