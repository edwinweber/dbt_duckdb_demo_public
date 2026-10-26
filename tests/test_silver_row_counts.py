"""Unit tests for attaching the Silver row counts to dbt materializations.

``stream_with_silver_row_counts`` reads the line ``silver_row_counts {...}`` that the dbt
post-hook ``silver_row_counts_post_hook`` logs per Silver table and attaches the counts to
that table's materialization.  A fake dbt invocation stands in for dagster-dbt here; the
real path is tested with dbt in ``test_dbt_silver.py``.
"""

from dataclasses import dataclass, field
from typing import Any

from dagster import AssetKey, AssetMaterialization, MetadataValue

from ddd_python.ddd_dagster.dbt_assets import (
    silver_row_count_metadata,
    stream_with_silver_row_counts,
)

SILVER = "model.danish_democracy_data.silver_ddd_aktoer"
BRONZE = "model.danish_democracy_data.bronze_ddd_aktoer"


@dataclass
class _RawEvent:
    raw_event: dict[str, Any]
    events: list[AssetMaterialization] = field(default_factory=list)

    def to_default_asset_events(self, **kwargs: Any) -> list[AssetMaterialization]:
        return self.events


@dataclass
class _Invocation:
    raw_events: list[_RawEvent]
    manifest: dict = field(default_factory=dict)
    dagster_dbt_translator: Any = None
    context: Any = None
    target_path: Any = None
    project: Any = None

    def stream_raw_events(self) -> list[_RawEvent]:
        return self.raw_events


def _log(unique_id: str, msg: str) -> _RawEvent:
    return _RawEvent({"info": {"name": "JinjaLogInfo", "msg": msg},
                      "data": {"node_info": {"unique_id": unique_id}}})  # fmt: skip


def _result(unique_id: str) -> _RawEvent:
    event = AssetMaterialization(
        asset_key=AssetKey(unique_id.split(".")[-1]),
        metadata={"unique_id": MetadataValue.text(unique_id)},
    )
    return _RawEvent({"info": {"name": "NodeFinished", "msg": ""}, "data": {}}, [event])


def _metadata(events: list) -> dict[str, dict[str, Any]]:
    return {
        e.metadata["unique_id"].value: {k: v.value for k, v in e.metadata.items()} for e in events
    }


def test_counts_go_to_the_materialization_of_the_model_that_logged_them():
    invocation = _Invocation([
        _log(SILVER, 'silver_row_counts {"I": 3, "U": 2, "D": 1, "files": 2}'),
        _result(SILVER),
        _log(BRONZE, '{"columns": {}}'),  # column metadata of dagster-dbt, no counts
        _result(BRONZE),
    ])  # fmt: skip
    metadata = _metadata(list(stream_with_silver_row_counts(invocation)))  # type: ignore[arg-type]
    assert metadata[SILVER] == {
        "unique_id": SILVER,
        "silver_rows_inserted": 3,
        "silver_rows_updated": 2,
        "silver_rows_deleted": 1,
        "silver_rows_written": 6,
        "silver_files_processed": 2,
    }
    assert metadata[BRONZE] == {"unique_id": BRONZE}


def test_a_run_without_new_rows_reports_zeros():
    metadata = silver_row_count_metadata({"I": 0, "U": 0, "D": 0, "files": 0})
    assert {k: v.value for k, v in metadata.items()} == {
        "silver_rows_inserted": 0,
        "silver_rows_updated": 0,
        "silver_rows_deleted": 0,
        "silver_files_processed": 0,
        "silver_rows_written": 0,
    }
