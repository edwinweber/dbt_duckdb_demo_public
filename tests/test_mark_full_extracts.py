"""Tests for ``mark_full_extracts``: which Bronze files are full extractions, and the rename."""

import json
from pathlib import Path

import pytest

from ddd_python.ddd_utils import mark_full_extracts as mfe
from ddd_python.ddd_utils.archive_bronze import Entity

AKTOER = Entity("DDD", "aktoer", incremental=True)
FAMILY = Entity("RFAM", "family", incremental=True)


def _files(directory: Path, entity: Entity, sizes: dict[str, int]) -> list[Path]:
    """Files named after their timestamp, with a given size in bytes."""
    directory.mkdir(parents=True, exist_ok=True)
    paths = []
    for stamp, size in sizes.items():
        path = directory / f"{entity.name}_{stamp}.json"
        path.write_bytes(b"x" * size)
        paths.append(path)
    return paths


def _names(decisions, full=True):
    return [d.file.name for d in decisions if d.full is full]


def test_full_name_keeps_every_extension():
    assert mfe.full_name("aktoer_20260516_214617.json") == "aktoer_20260516_214617_full.json"
    assert (
        mfe.full_name("aktoer_20260516_214617.json.jsonl")
        == "aktoer_20260516_214617_full.json.jsonl"
    )
    with pytest.raises(ValueError):
        mfe.full_name("aktoer.json")


def test_the_run_log_decides_and_beats_the_size(tmp_path):
    files = _files(
        tmp_path, AKTOER, {"20260101_000000": 100, "20260102_000000": 100, "20260103_000000": 100}
    )
    logged = {
        files[0].name: "1900-01-01",  # full although small
        files[1].name: "2025-01-01",  # not full
        files[2].name: "2026-01-01",
    }

    decisions = mfe.decide(files, logged)

    assert _names(decisions) == [files[0].name]
    assert all(d.evidence.startswith("log:") for d in decisions)


def test_a_date_before_1900_counts_as_full(tmp_path):
    files = _files(tmp_path, AKTOER, {"20260101_000000": 10})
    assert mfe.decide(files, {files[0].name: "1800-01-01"})[0].full


def test_size_is_used_for_files_without_a_log_record(tmp_path):
    sizes = {f"2026010{d}_000000": 100 for d in range(1, 8)}
    sizes["20260108_000000"] = 1000  # ten times the median, the largest
    files = _files(tmp_path, AKTOER, sizes)

    decisions = mfe.decide(files, {})

    assert _names(decisions) == ["aktoer_20260108_000000.json"]
    assert "size (no log record)" in next(d for d in decisions if d.full).evidence


def test_a_large_partial_extraction_next_to_a_much_larger_full_one_is_not_full(tmp_path):
    sizes = {f"2026010{d}_000000": 100 for d in range(1, 7)}
    sizes["20260107_000000"] = 1500  # 15x the median, but only 6% of the largest
    sizes["20260108_000000"] = 25000
    files = _files(tmp_path, AKTOER, sizes)

    assert _names(mfe.decide(files, {})) == ["aktoer_20260108_000000.json"]


def test_two_full_extractions_of_similar_size_are_both_found(tmp_path):
    sizes = {f"2026010{d}_000000": 100 for d in range(1, 7)}
    sizes["20260107_000000"] = 9000
    sizes["20260108_000000"] = 10000
    files = _files(tmp_path, AKTOER, sizes)

    assert len(_names(mfe.decide(files, {}))) == 2


def test_ordinary_variation_in_size_finds_nothing(tmp_path):
    files = _files(tmp_path, AKTOER, {f"2026010{d}_000000": 100 + 20 * d for d in range(1, 9)})
    assert _names(mfe.decide(files, {})) == []


def test_size_needs_at_least_three_files(tmp_path):
    files = _files(tmp_path, AKTOER, {"20260101_000000": 10, "20260102_000000": 1000})
    assert _names(mfe.decide(files, {})) == []


def test_log_only_and_size_only_modes(tmp_path):
    sizes = {f"2026010{d}_000000": 100 for d in range(1, 7)}
    sizes["20260107_000000"] = 1000
    files = _files(tmp_path, AKTOER, sizes)
    logged = {files[0].name: "1900-01-01"}

    assert _names(mfe.decide(files, logged, use_size=False)) == [files[0].name]
    assert files[0].name not in _names(mfe.decide(files, logged, use_logs=False))
    assert "aktoer_20260107_000000.json" in _names(mfe.decide(files, logged, use_logs=False))


def test_files_that_are_already_marked_are_skipped(tmp_path):
    files = _files(
        tmp_path, AKTOER, {"20260101_000000": 10, "20260102_000000": 10, "20260103_000000": 10}
    )
    files[0] = files[0].rename(files[0].with_name("aktoer_20260101_000000_full.json"))

    assert files[0].name not in [d.file.name for d in mfe.decide(files, {})]


def test_read_run_log_ddd_and_rfam_and_failures(tmp_path):
    def write(entity: Entity, records: list[dict]) -> None:
        path = tmp_path / entity.source / entity.name / f"{entity.name}_log.ndjson"
        path.parent.mkdir(parents=True)
        path.write_text("\n".join(json.dumps(r) for r in records) + "\nnot json\n")

    ok = "Pipeline execution completed successfully"
    write(AKTOER, [
        {"message": ok, "parameters": {"destination_file_name": "a.json", "source_api_date_to_load_from": "1900-01-01"}},
        {"message": "Pipeline execution failed", "parameters": {"destination_file_name": "b.json", "source_api_date_to_load_from": "1900-01-01"}},
    ])  # fmt: skip
    write(FAMILY, [
        {"message": ok, "parameters": {"destination_file_name": "f.json", "sql_params": {"updated_from": "2025-03-31"}}},
    ])  # fmt: skip

    assert mfe.read_run_log(tmp_path, AKTOER) == {"a.json": "1900-01-01"}
    assert mfe.read_run_log(tmp_path, FAMILY) == {"f.json": "2025-03-31"}
    assert mfe.read_run_log(tmp_path, Entity("DDD", "sag", True)) == {}  # no log at all


def test_apply_renames_only_full_files_and_never_overwrites(tmp_path):
    files = _files(
        tmp_path, AKTOER, {"20260101_000000": 10, "20260102_000000": 10, "20260103_000000": 10}
    )
    decisions = mfe.decide(files, {files[1].name: "1900-01-01"})

    assert mfe.apply_decisions(decisions) == 1
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "aktoer_20260101_000000.json",
        "aktoer_20260102_000000_full.json",
        "aktoer_20260103_000000.json",
    ]

    (tmp_path / "aktoer_20260101_000000_full.json").write_bytes(b"other")
    clash = mfe.decide(
        [tmp_path / "aktoer_20260101_000000.json"], {"aktoer_20260101_000000.json": "1900-01-01"}
    )
    with pytest.raises(FileExistsError):
        mfe.apply_decisions(clash)
    assert (tmp_path / "aktoer_20260101_000000.json").exists()
