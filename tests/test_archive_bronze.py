"""Tests for ``archive_bronze``: which files may go, and a safe move to and from the archive.

Silver is an in-memory DuckDB database with just the bookmark table the script reads; Bronze
files live in pytest's temporary directories.
"""

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from dagster import materialize

import duckdb
from ddd_python.ddd_dagster.bronze_archive_assets import bronze_archive_asset
from ddd_python.ddd_utils import archive_bronze
from ddd_python.ddd_utils.archive_bronze import Entity

INCREMENTAL = Entity("DDD", "aktoer", incremental=True)
FULL = Entity("DDD", "periode", incremental=False)
NOW = datetime(2026, 2, 1, tzinfo=UTC)
DB = "memory"


def _name(entity: Entity, day: int, full: bool = False) -> str:
    return f"{entity.name}_202601{day:02d}_000000{'_full' if full else ''}.json"


def _write(directory: Path, entity: Entity, day: int, keys: list[int], full: bool = False) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / _name(entity, day, full)
    path.write_text("".join(json.dumps({"id": k, "name": f"n{k}"}) + "\n" for k in keys))
    return path


def _files(directory: Path, entity: Entity, days: range) -> list[Path]:
    return [_write(directory, entity, day, [1, 2]) for day in days]


@pytest.fixture
def silver():
    connection = duckdb.connect()
    connection.execute("CREATE SCHEMA main_silver")
    for table in ("silver_ddd_aktoer_last_file", "silver_ddd_periode_last_file"):
        connection.execute(
            f"CREATE TABLE main_silver.{table} (LKHS_filename VARCHAR, LKHS_filename_pending VARCHAR)"
        )
    yield connection
    connection.close()


def test_everything_processed_goes_except_the_newest_processed_file(tmp_path):
    files = _files(tmp_path, FULL, range(1, 9))

    plan = archive_bronze.plan_entity(FULL, files, _name(FULL, 6), 0, NOW)

    assert plan.archive == [_name(FULL, d) for d in range(1, 6)]  # 6 stays, 7 and 8 are new


def test_incremental_entities_follow_the_same_rule(tmp_path):
    files = [_write(tmp_path, INCREMENTAL, d, [1], full=d == 3) for d in range(1, 6)]

    plan = archive_bronze.plan_entity(INCREMENTAL, files, _name(INCREMENTAL, 4), 0, NOW)

    assert plan.archive == [
        _name(INCREMENTAL, 1),
        _name(INCREMENTAL, 2),
        _name(INCREMENTAL, 3, full=True),
    ]


def test_keep_days_protects_recent_files(tmp_path):
    files = _files(tmp_path, FULL, range(1, 9))
    now = datetime(2026, 1, 8, tzinfo=UTC)  # file 8 is 0 days old, file 3 is 5 days old

    plan = archive_bronze.plan_entity(FULL, files, _name(FULL, 8), 5, now)

    assert plan.archive == [_name(FULL, d) for d in range(1, 4)]


def test_late_files_stay_until_silver_logs_them(tmp_path):
    files = _files(tmp_path, FULL, range(1, 7))
    logged = {_name(FULL, d) for d in (1, 2, 4, 5, 6)}  # day 3 arrived late

    plan = archive_bronze.plan_entity(FULL, files, _name(FULL, 6), 0, NOW, logged)

    assert plan.archive == [_name(FULL, d) for d in (1, 2, 4, 5)]
    assert "1 late file(s) kept" in plan.reason and _name(FULL, 3) in plan.reason


def test_nothing_is_archived_without_a_bookmark_or_files(tmp_path):
    files = _files(tmp_path, FULL, range(1, 3))

    no_bookmark = archive_bronze.plan_entity(FULL, files, None, 0, NOW)
    no_files = archive_bronze.plan_entity(FULL, [], _name(FULL, 2), 0, NOW)

    assert no_bookmark.archive == [] and "bookmark" in no_bookmark.reason
    assert no_files.archive == [] and no_files.reason == "no files"


def test_a_bookmark_that_is_not_in_bronze_is_refused(tmp_path):
    files = _files(tmp_path, FULL, range(1, 5))

    plan = archive_bronze.plan_entity(FULL, files, "periode_20261231_000000.json", 0, NOW)

    assert plan.archive == [] and "refusing" in plan.reason


def test_is_full_file():
    assert archive_bronze.is_full_file("aktoer_20260101_000000_full.json")
    assert archive_bronze.is_full_file("aktoer_20260101_000000_full.json.jsonl")
    assert not archive_bronze.is_full_file("aktoer_20260101_000000.json")
    assert not archive_bronze.is_full_file("full_20260101_000000.json")


def test_read_bookmark(silver):
    assert archive_bronze.read_bookmark(silver, DB, INCREMENTAL) is None  # empty table
    silver.execute(
        "INSERT INTO main_silver.silver_ddd_aktoer_last_file VALUES ('b.json', 'b.json')"
    )
    assert archive_bronze.read_bookmark(silver, DB, INCREMENTAL) == "b.json"
    assert archive_bronze.read_bookmark(silver, DB, Entity("DDD", "sag", True)) is None


def test_read_logged_files(silver):
    assert archive_bronze.read_logged_files(silver, DB, INCREMENTAL) is None  # no log
    silver.execute(
        "CREATE TABLE main_silver.silver_ddd_aktoer_files "
        "(LKHS_filename VARCHAR, LKHS_status VARCHAR, LKHS_date_inserted TIMESTAMP)"
    )
    silver.execute(
        "INSERT INTO main_silver.silver_ddd_aktoer_files VALUES ('a.json', 'seeded', NULL), "
        "('b.json', 'processed', NULL), ('c.json', 'skipped', NULL), ('d.json', 'pending', NULL)"
    )
    assert archive_bronze.read_logged_files(silver, DB, INCREMENTAL) == {
        "a.json",
        "b.json",
        "c.json",
    }


def test_run_archive_moves_the_planned_files(tmp_path, silver):
    bronze, archive = tmp_path / "DDD", tmp_path / "archive"
    _files(bronze / "periode", FULL, range(1, 5))
    silver.execute(
        f"INSERT INTO main_silver.silver_ddd_periode_last_file VALUES ('{_name(FULL, 3)}', NULL)"
    )

    with (
        patch.object(archive_bronze, "open_export_connection", return_value=silver),
        patch.object(archive_bronze, "silver_database", return_value=DB),
    ):
        plans, moved = archive_bronze.run_archive(
            {"DDD": bronze}, archive, keep_days=0, execute=True, entity_names=["periode"]
        )

    assert moved == 2
    assert sorted(p.name for p in (bronze / "periode").iterdir()) == [
        _name(FULL, 3),
        _name(FULL, 4),
    ]
    assert sorted(p.name for p in (archive / "DDD" / "periode").iterdir()) == [
        _name(FULL, 1),
        _name(FULL, 2),
    ]
    assert [p.entity.name for p in plans] == ["periode"]


def test_the_dagster_asset_runs_the_archive(tmp_path):
    with (
        patch.object(archive_bronze.get_variables_from_env, "RAW_STORAGE_TARGET", "local"),
        patch.object(archive_bronze.get_variables_from_env, "BRONZE_ARCHIVE_DIR", str(tmp_path)),
        patch(
            "ddd_python.ddd_dagster.bronze_archive_assets.local_bronze_directories", return_value={}
        ),
        patch(
            "ddd_python.ddd_dagster.bronze_archive_assets.run_archive", return_value=([], 0)
        ) as run,
    ):
        result = materialize(
            [bronze_archive_asset],
            run_config={
                "ops": {"bronze_archive_asset": {"config": {"keep_days": 3, "dry_run": True}}}
            },
        )

    assert result.success
    assert run.call_args.kwargs == {"keep_days": 3, "compress": True, "execute": False}


@pytest.mark.parametrize("compress", [False, True])
def test_archive_and_restore_round_trip(tmp_path, compress):
    bronze, archive = tmp_path / "bronze", tmp_path / "archive"
    source = _write(bronze, FULL, 1, [1, 2, 3])
    original = source.read_bytes()
    target_dir = archive / "DDD" / "periode"

    archive_bronze.archive_file(source, target_dir, FULL, compress, archive / "MANIFEST.ndjson")

    assert not source.exists()
    manifest = [json.loads(line) for line in (archive / "MANIFEST.ndjson").read_text().splitlines()]
    assert [m["file"] for m in manifest] == [source.name]
    assert (target_dir / (source.name + (".gz" if compress else ""))).exists()

    assert archive_bronze.restore_entity(FULL, archive, bronze) == 1
    assert source.read_bytes() == original
    assert archive_bronze.restore_entity(FULL, archive, bronze) == 0  # already there, same content


def test_archiving_twice_is_idempotent_and_never_overwrites_different_content(tmp_path):
    bronze, archive = tmp_path / "bronze", tmp_path / "archive"
    target_dir = archive / "DDD" / "periode"
    manifest = archive / "MANIFEST.ndjson"
    source = _write(bronze, FULL, 1, [1])
    archive_bronze.archive_file(source, target_dir, FULL, False, manifest)

    again = _write(bronze, FULL, 1, [1])  # same name, same content
    archive_bronze.archive_file(again, target_dir, FULL, False, manifest)
    assert not again.exists()
    assert len(manifest.read_text().splitlines()) == 1

    different = _write(bronze, FULL, 1, [1, 2])  # same name, other content
    with pytest.raises(RuntimeError, match="different content"):
        archive_bronze.archive_file(different, target_dir, FULL, False, manifest)
    assert different.exists()  # the Bronze file is kept when something is wrong


def test_restore_refuses_to_overwrite_and_detects_a_damaged_archive(tmp_path):
    bronze, archive = tmp_path / "bronze", tmp_path / "archive"
    target_dir = archive / "DDD" / "periode"
    source = _write(bronze, FULL, 1, [1])
    archive_bronze.archive_file(source, target_dir, FULL, False, archive / "MANIFEST.ndjson")

    _write(bronze, FULL, 1, [9])  # someone put a different file in its place
    with pytest.raises(RuntimeError, match="different content"):
        archive_bronze.restore_entity(FULL, archive, bronze)

    (bronze / _name(FULL, 1)).unlink()
    (target_dir / _name(FULL, 1)).write_text("damaged\n")
    with pytest.raises(RuntimeError, match="checksum"):
        archive_bronze.restore_entity(FULL, archive, bronze)


def test_list_entities_follows_the_configuration():
    entities = archive_bronze.list_entities(["DDD", "RFAM"])
    assert len(entities) == 18 + 7
    assert sorted(e.name for e in entities if e.incremental) == [
        "aktoer",
        "family",
        "genome",
        "moede",
        "sag",
        "sagstrin",
        "sagstrinaktoer",
        "stemme",
    ]
    assert Entity("DDD", "aktoer", True).silver_table == "silver_ddd_aktoer"
