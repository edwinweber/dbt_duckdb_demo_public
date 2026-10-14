"""The Bronze file name dlt writes must be exactly the name the caller asked for.

dlt >= 1.30 appends ``.{ext}`` to a layout that has no ``{ext}`` placeholder, which
turned ``x_20260707_133549.json`` into ``x_20260707_133549.json.jsonl``. These tests run a
real (offline) dlt pipeline into a temp directory and check the resulting file names.
"""

from pathlib import Path
from unittest.mock import patch

import pytest

import dlt
from ddd_python.ddd_dlt.dlt_pipeline_execution_functions import _make_destination


def _run(tmp_path: Path, file_name: str, loader_file_format: str) -> list[str]:
    env = patch.dict(
        "ddd_python.ddd_dlt.dlt_pipeline_execution_functions.get_variables_from_env.__dict__",
        {"RAW_STORAGE_TARGET": "local", "LOCAL_STORAGE_PATH": str(tmp_path / "storage")},
        clear=False,
    )
    with env:
        destination, dataset_name = _make_destination("Files/Bronze/DDD/thing", file_name, "thing")
        pipeline = dlt.pipeline(
            pipeline_name="thing",
            destination=destination,
            dataset_name=dataset_name,
            pipelines_dir=str(tmp_path / "pipelines"),
        )

        @dlt.resource(name="thing", write_disposition="append", max_table_nesting=0)
        def rows():
            yield from [{"id": 1}, {"id": 2}]

        pipeline.run(rows(), loader_file_format=loader_file_format)

    data_dir = tmp_path / "storage" / "Files" / "Bronze" / "DDD" / "thing"
    return sorted(p.name for p in data_dir.iterdir() if p.is_file())


@pytest.mark.parametrize(
    ("file_name", "loader_file_format"),
    [
        ("thing_20260707_133549.json", "jsonl"),
        ("thing_20260707_133549.jsonl", "jsonl"),
        ("thing_20260707_133549.parquet", "parquet"),
    ],
)
def test_data_file_has_exactly_the_requested_name(tmp_path, file_name, loader_file_format):
    names = _run(tmp_path, file_name, loader_file_format)
    assert file_name in names
    assert not any(n.endswith(".json.jsonl") for n in names)
