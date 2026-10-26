"""Unit tests for the dbt commands the dagster-dbt assets run.

A ``DbtCliResource`` that records its arguments instead of starting dbt stands in for the
real one; what dbt then selects is checked in ``test_dbt_data_engineering.py``.
"""

from typing import Any
from unittest.mock import MagicMock

from dagster import build_asset_context
from dagster_dbt import DbtCliResource

from ddd_python.ddd_dagster.dbt_assets import dbt_gold_assets, dbt_project

_calls: list[list[str]] = []


class _RecordingDbt(DbtCliResource):
    def cli(self, args: Any, **kwargs: Any) -> Any:  # type: ignore[override]
        _calls.append(list(args))
        invocation = MagicMock()
        invocation.stream.return_value = iter([])
        return invocation


def test_the_gold_step_leaves_the_data_engineering_tests_to_their_own_step():
    """dbt selects every test that references a selected model, so without the exclude
    the data_engineering relationship tests to date and time would run in the Gold step,
    before their models exist on a first run (production, 2026-10-26)."""
    _calls.clear()
    dbt = _RecordingDbt(project_dir=str(dbt_project.project_dir))
    list(dbt_gold_assets(context=build_asset_context(), dbt=dbt))
    build = next(args for args in _calls if args[0] == "build")
    assert build[build.index("--exclude") + 1] == "data_engineering"
