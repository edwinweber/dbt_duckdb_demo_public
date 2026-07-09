"""Tests for backup-target assembly — especially the DuckLake ordering invariant.

When ``SILVER_STORAGE_FORMAT=ducklake``, the backup must include the DuckLake
data files, and must archive them **before** the catalog (the catalog lives in
the DuckDB directory and is captured by the ``duckdb`` target).  These tests
pin that contract so a future reordering can't silently break it.
"""

import ddd_python.ddd_utils.backup_common as backup_common
from ddd_python.ddd_utils.backup_common import _build_backup_targets, _ducklake_data_is_local


def _names(include_ducklake: bool) -> list[str]:
    return [t.name for t in _build_backup_targets(include_ducklake)]


def test_ducklake_mode_includes_data_target():
    assert "ducklake" in _names(include_ducklake=True)


def test_ducklake_data_archived_before_catalog():
    """The DuckLake data files must come before the duckdb target (which holds
    the catalog) so the catalog snapshot never references un-backed-up files."""
    names = _names(include_ducklake=True)
    assert names.index("ducklake") < names.index("duckdb")


def test_duckdb_mode_excludes_ducklake_target():
    assert "ducklake" not in _names(include_ducklake=False)


def test_ducklake_target_points_at_data_path():
    target = next(t for t in _build_backup_targets(True) if t.name == "ducklake")
    # Source is the DuckLake data directory (DUCKLAKE_DATA_PATH), not the catalog.
    assert str(target.source) == str(backup_common._DUCKLAKE_DATA_DIR)


def test_ducklake_retention_matches_duckdb():
    """DuckLake data files and the duckdb target (which carries the DuckLake
    catalog) share the same 7-day retention."""
    targets = {t.name: t for t in _build_backup_targets(True)}
    assert targets["ducklake"].max_archive_age_days == targets["duckdb"].max_archive_age_days
    assert targets["duckdb"].max_archive_age_days == 7


def test_catalog_lives_in_the_duckdb_target_directory():
    """The catalog file sits in the DuckDB directory, so the duckdb target's
    source directory is its parent — i.e. the catalog is captured by duckdb."""
    duckdb_target = next(t for t in _build_backup_targets(True) if t.name == "duckdb")
    assert duckdb_target.source == backup_common._DUCKDB_DATA_DIR


# ---------------------------------------------------------------------------
# S3 DuckLake data path — local backup must be skipped
# ---------------------------------------------------------------------------


def test_ducklake_data_is_local_with_local_path(monkeypatch):
    monkeypatch.setenv("DUCKLAKE_DATA_PATH", "/data/ducklake")
    assert _ducklake_data_is_local() is True


def test_ducklake_data_is_local_with_s3_path(monkeypatch):
    monkeypatch.setenv("DUCKLAKE_DATA_PATH", "s3://ddd-ducklake/")
    assert _ducklake_data_is_local() is False


def test_ducklake_data_is_local_when_unset(monkeypatch):
    monkeypatch.delenv("DUCKLAKE_DATA_PATH", raising=False)
    assert _ducklake_data_is_local() is True


def test_s3_ducklake_path_excludes_ducklake_backup_target(monkeypatch):
    """When DUCKLAKE_DATA_PATH=s3://... the local ducklake target must be absent."""
    from unittest.mock import patch

    monkeypatch.setenv("DUCKLAKE_DATA_PATH", "s3://ddd-ducklake/")
    # Force silver_storage_is_ducklake=True so the S3 guard is actually exercised.
    # _build_backup_targets receives the pre-computed bool, mirroring BACKUP_TARGETS assembly.
    with patch("ddd_python.ddd_utils.backup_common.silver_storage_is_ducklake", return_value=True):
        include = (
            backup_common.silver_storage_is_ducklake() and backup_common._ducklake_data_is_local()
        )
        targets = backup_common._build_backup_targets(include_ducklake=include)
    names = [t.name for t in targets]
    assert "ducklake" not in names


def test_local_ducklake_path_includes_ducklake_backup_target(monkeypatch):
    """When DUCKLAKE_DATA_PATH is a local path and ducklake mode is on, include the target."""
    monkeypatch.setenv("DUCKLAKE_DATA_PATH", "/data/ducklake")
    names = _names(include_ducklake=True)
    assert "ducklake" in names
