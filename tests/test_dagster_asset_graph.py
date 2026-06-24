"""Tests for the Dagster asset graph structure.

These tests verify that the Definitions object loads without error and that the
expected assets, jobs, schedules, and sensors are registered. They are intended
to catch silent graph regressions during refactoring — not to re-test Dagster's
own logic.

The full Definitions object requires a dbt manifest (dbt/target/manifest.json).
All tests in the manifest-gated class are skipped when the manifest is absent so
that the test suite still passes in environments where dbt has not been run.

``TestAssetFactoriesWithoutManifest`` tests the asset factory wiring directly and
never requires a manifest — these run in every environment.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from dagster import AssetKey

# Path to the dbt manifest relative to this file's repo root.
_REPO_ROOT = Path(__file__).resolve().parents[1]
_MANIFEST_PATH = _REPO_ROOT / "dbt" / "target" / "manifest.json"

_MANIFEST_MISSING = not _MANIFEST_PATH.exists()
_SKIP_REASON = (
    "dbt manifest not found — run `dbt parse` in dbt/ to generate it "
    "(requires DUCKDB_DATABASE_LOCATION, DANISH_DEMOCRACY_DATA_SOURCE, RFAM_DATA_SOURCE)"
)

# Minimal env vars needed to import definitions.py without Azure credentials.
_REQUIRED_ENV = {
    "STORAGE_TARGET": "local",
    "SILVER_STORAGE_FORMAT": "duckdb",
    "DUCKDB_DATABASE_LOCATION": "/tmp/test_dagster_graph.duckdb",
    "DANISH_DEMOCRACY_DATA_SOURCE": "/tmp/bronze/ddd",
    "RFAM_DATA_SOURCE": "/tmp/bronze/rfam",
    "DAGSTER_HOME": "/tmp/dagster_home_test",
}


@pytest.fixture(scope="module")
def defs(monkeypatch_module):
    """Load the Dagster Definitions object with minimal env vars patched in."""
    for key, value in _REQUIRED_ENV.items():
        monkeypatch_module.setenv(key, value)
    # Import after env is set — definitions.py calls load_dotenv() at module level
    # so sys.modules may already hold a stale version; import fresh.
    import sys

    for mod in list(sys.modules):
        if "ddd_python.ddd_dagster" in mod or "ddd_python.ddd_dlt" in mod:
            del sys.modules[mod]

    from ddd_python.ddd_dagster.definitions import defs as _defs

    return _defs


@pytest.fixture(scope="module")
def monkeypatch_module(request):
    """Module-scoped monkeypatch so env overrides persist for all tests in this module."""
    from _pytest.monkeypatch import MonkeyPatch

    mp = MonkeyPatch()
    yield mp
    mp.undo()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _asset_key_strings(defs) -> set[str]:
    return {str(k) for k in defs.resolve_all_asset_keys()}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(_MANIFEST_MISSING, reason=_SKIP_REASON)
def test_definitions_load(defs) -> None:
    """Definitions object loads without raising."""
    from dagster import Definitions

    assert isinstance(defs, Definitions)


@pytest.mark.skipif(_MANIFEST_MISSING, reason=_SKIP_REASON)
def test_ddd_extraction_asset_present(defs) -> None:
    """At least one DDD extraction asset key is registered."""
    keys = _asset_key_strings(defs)
    assert str(AssetKey(["ingestion", "DDD", "afstemning"])) in keys


@pytest.mark.skipif(_MANIFEST_MISSING, reason=_SKIP_REASON)
def test_rfam_extraction_asset_present(defs) -> None:
    """At least one Rfam extraction asset key is registered."""
    keys = _asset_key_strings(defs)
    assert str(AssetKey(["ingestion", "RFAM", "family"])) in keys


@pytest.mark.skipif(_MANIFEST_MISSING, reason=_SKIP_REASON)
def test_dbt_bronze_asset_present(defs) -> None:
    """At least one dbt Bronze model asset key is registered."""
    keys = _asset_key_strings(defs)
    assert str(AssetKey(["bronze", "bronze_ddd_afstemning"])) in keys


@pytest.mark.skipif(_MANIFEST_MISSING, reason=_SKIP_REASON)
def test_export_silver_asset_present(defs) -> None:
    """At least one Silver export asset key is registered."""
    keys = _asset_key_strings(defs)
    # Silver export keys follow the pattern ['export', 'silver', 'export_<table>']
    assert any("export_silver" in k for k in keys)


@pytest.mark.skipif(_MANIFEST_MISSING, reason=_SKIP_REASON)
@pytest.mark.parametrize(
    "schedule_name",
    [
        "danish_parliament_full_pipeline_schedule",
        "dbt_data_engineering_schedule",
    ],
)
def test_schedule_present(defs, schedule_name: str) -> None:
    """Each expected schedule is registered in Definitions."""
    schedule_names = {s.name for s in defs.schedules}
    assert schedule_name in schedule_names


@pytest.mark.skipif(_MANIFEST_MISSING, reason=_SKIP_REASON)
@pytest.mark.parametrize(
    "sensor_name",
    [
        "danish_parliament_run_success_sensor",
        "danish_parliament_run_failure_sensor",
    ],
)
def test_sensor_present(defs, sensor_name: str) -> None:
    """Each expected run-status sensor is registered in Definitions."""
    sensor_names = {s.name for s in defs.sensors}
    assert sensor_name in sensor_names


@pytest.mark.skipif(_MANIFEST_MISSING, reason=_SKIP_REASON)
def test_all_ddd_extraction_assets_present(defs) -> None:
    """All 18 DDD extraction assets are registered (one per entity)."""
    from ddd_python.ddd_utils import configuration_variables
    from ddd_python.ddd_utils.string_utils import normalize_danish_name

    keys = _asset_key_strings(defs)
    for entity in configuration_variables.DANISH_DEMOCRACY_FILE_NAMES:
        expected = str(AssetKey(["ingestion", "DDD", normalize_danish_name(entity)]))
        assert expected in keys, f"Missing DDD asset key for entity {entity!r}"


@pytest.mark.skipif(_MANIFEST_MISSING, reason=_SKIP_REASON)
def test_all_rfam_extraction_assets_present(defs) -> None:
    """All 7 Rfam extraction assets are registered (one per table)."""
    from ddd_python.ddd_utils import configuration_variables

    keys = _asset_key_strings(defs)
    for table in configuration_variables.RFAM_TABLE_NAMES:
        expected = str(AssetKey(["ingestion", "RFAM", table]))
        assert expected in keys, f"Missing RFAM asset key for table {table!r}"


@pytest.mark.skipif(_MANIFEST_MISSING, reason=_SKIP_REASON)
def test_expected_jobs_registered(defs) -> None:
    """Spot-check that key jobs are registered in Definitions."""
    job_names = {j.name for j in defs.resolve_all_job_defs()}
    expected_jobs = {
        "danish_parliament_incremental_job",
        "danish_parliament_full_extract_job",
        "full_pipeline_job",
        "export_silver_job",
        "export_gold_job",
        "dbt_silver_job",
        "dbt_gold_job",
        "rfam_incremental_job",
        "rfam_all_job",
    }
    missing = expected_jobs - job_names
    assert not missing, f"Jobs missing from Definitions: {sorted(missing)}"


# ---------------------------------------------------------------------------
# Manifest-free tests: asset factory wiring
# ---------------------------------------------------------------------------


def _flush_ddd_dagster_modules() -> None:
    """Remove cached ddd_dagster modules so a re-import picks up env patches."""
    for mod in list(sys.modules):
        if "ddd_python.ddd_dagster" in mod:
            del sys.modules[mod]


@pytest.fixture(scope="class")
def factory_env(monkeypatch_class):
    """Patch the minimal env vars and import the asset factory modules.

    Only ``ddd_python.ddd_dagster`` modules are flushed on setup so that a
    fresh import of assets.py / rfam_assets.py picks up the patched env.
    ``ddd_python.ddd_utils`` is intentionally left in ``sys.modules`` to avoid
    invalidating the module-level references that ``test_path_utils.py`` holds
    from its collection-time import.
    """
    for key, value in _REQUIRED_ENV.items():
        monkeypatch_class.setenv(key, value)
    _flush_ddd_dagster_modules()
    yield
    # No teardown flush needed: monkeypatch_class.undo() restores os.environ,
    # and the cached ddd_dagster modules are fine to leave in sys.modules —
    # they will be re-used or overwritten by subsequent fixtures.


@pytest.fixture(scope="class")
def monkeypatch_class(request):
    """Class-scoped monkeypatch so env overrides persist across the whole class."""
    from _pytest.monkeypatch import MonkeyPatch

    mp = MonkeyPatch()
    yield mp
    mp.undo()


class TestAssetFactoriesWithoutManifest:
    """Asset factory wiring tests — no dbt manifest required.

    These tests import the asset factory modules directly (not ``definitions.py``)
    and inspect the module-level asset lists that are built at import time from
    ``configuration_variables``.  They run in every environment, including fresh
    clones and CI without a prior ``dbt parse``.
    """

    @pytest.fixture(autouse=True)
    def _setup(self, factory_env):
        """Ensure env is patched before each test in this class."""

    # ------------------------------------------------------------------
    # Asset count assertions
    # ------------------------------------------------------------------

    def test_ddd_incremental_asset_count(self) -> None:
        """DDD incremental factory produces exactly 6 assets."""
        from ddd_python.ddd_dagster import assets as ddd_assets
        from ddd_python.ddd_utils import configuration_variables

        expected = len(configuration_variables.DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL)
        assert expected == 6
        assert len(ddd_assets.incremental_assets) == expected

    def test_ddd_full_extract_asset_count(self) -> None:
        """DDD full-extract factory produces exactly 12 assets."""
        from ddd_python.ddd_dagster import assets as ddd_assets
        from ddd_python.ddd_utils import configuration_variables

        incremental_count = len(configuration_variables.DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL)
        full_extract_count = (
            len(configuration_variables.DANISH_DEMOCRACY_FILE_NAMES) - incremental_count
        )
        assert full_extract_count == 12
        assert len(ddd_assets.full_extract_assets) == full_extract_count

    def test_ddd_total_asset_count(self) -> None:
        """Combined DDD asset list contains exactly 18 assets — one per entity."""
        from ddd_python.ddd_dagster import assets as ddd_assets
        from ddd_python.ddd_utils import configuration_variables

        expected = len(configuration_variables.DANISH_DEMOCRACY_FILE_NAMES)
        assert expected == 18
        assert len(ddd_assets.all_extraction_assets) == expected

    def test_rfam_asset_count(self) -> None:
        """Rfam factory produces exactly 7 assets — one per table."""
        from ddd_python.ddd_dagster import rfam_assets
        from ddd_python.ddd_utils import configuration_variables

        expected = len(configuration_variables.RFAM_TABLE_NAMES)
        assert expected == 7
        assert len(rfam_assets.all_rfam_extraction_assets) == expected

    # ------------------------------------------------------------------
    # Asset key prefix / group assertions
    # ------------------------------------------------------------------

    def test_ddd_incremental_assets_have_correct_key_prefix(self) -> None:
        """Every DDD incremental asset has key prefix ['ingestion', 'DDD']."""
        from ddd_python.ddd_dagster import assets as ddd_assets

        for asset_def in ddd_assets.incremental_assets:
            key = asset_def.key
            assert list(key.path[:2]) == ["ingestion", "DDD"], (
                f"Expected key prefix ['ingestion', 'DDD'] but got {list(key.path)!r}"
            )

    def test_ddd_full_extract_assets_have_correct_key_prefix(self) -> None:
        """Every DDD full-extract asset has key prefix ['ingestion', 'DDD']."""
        from ddd_python.ddd_dagster import assets as ddd_assets

        for asset_def in ddd_assets.full_extract_assets:
            key = asset_def.key
            assert list(key.path[:2]) == ["ingestion", "DDD"], (
                f"Expected key prefix ['ingestion', 'DDD'] but got {list(key.path)!r}"
            )

    def test_rfam_assets_have_correct_key_prefix(self) -> None:
        """Every Rfam asset has key prefix ['ingestion', 'RFAM']."""
        from ddd_python.ddd_dagster import rfam_assets

        for asset_def in rfam_assets.all_rfam_extraction_assets:
            key = asset_def.key
            assert list(key.path[:2]) == ["ingestion", "RFAM"], (
                f"Expected key prefix ['ingestion', 'RFAM'] but got {list(key.path)!r}"
            )

    def test_ddd_incremental_assets_group_name(self) -> None:
        """Every DDD incremental asset belongs to group 'ingestion_DDD_incremental'."""
        from ddd_python.ddd_dagster import assets as ddd_assets

        for asset_def in ddd_assets.incremental_assets:
            assert asset_def.group_names_by_key[asset_def.key] == "ingestion_DDD_incremental", (
                f"Asset {asset_def.key} has unexpected group "
                f"{asset_def.group_names_by_key[asset_def.key]!r}"
            )

    def test_ddd_full_extract_assets_group_name(self) -> None:
        """Every DDD full-extract asset belongs to group 'ingestion_DDD_full_extract'."""
        from ddd_python.ddd_dagster import assets as ddd_assets

        for asset_def in ddd_assets.full_extract_assets:
            assert asset_def.group_names_by_key[asset_def.key] == "ingestion_DDD_full_extract", (
                f"Asset {asset_def.key} has unexpected group "
                f"{asset_def.group_names_by_key[asset_def.key]!r}"
            )

    # ------------------------------------------------------------------
    # Incremental vs full-extract split
    # ------------------------------------------------------------------

    def test_incremental_entities_are_in_incremental_factory(self) -> None:
        """Each of the 6 incremental entities appears in the incremental asset list."""
        from ddd_python.ddd_dagster import assets as ddd_assets
        from ddd_python.ddd_utils import configuration_variables
        from ddd_python.ddd_utils.string_utils import normalize_danish_name

        incremental_key_names = {a.key.path[-1] for a in ddd_assets.incremental_assets}
        for entity in configuration_variables.DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL:
            normalized = normalize_danish_name(entity)
            assert normalized in incremental_key_names, (
                f"Incremental entity {entity!r} (normalized: {normalized!r}) "
                "not found in incremental_assets"
            )

    def test_full_extract_entities_are_not_in_incremental_factory(self) -> None:
        """No full-extract entity leaks into the incremental asset list."""
        from ddd_python.ddd_dagster import assets as ddd_assets
        from ddd_python.ddd_utils import configuration_variables
        from ddd_python.ddd_utils.string_utils import normalize_danish_name

        incremental_set = frozenset(configuration_variables.DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL)
        full_extract_entities = [
            e
            for e in configuration_variables.DANISH_DEMOCRACY_FILE_NAMES
            if e not in incremental_set
        ]
        incremental_key_names = {a.key.path[-1] for a in ddd_assets.incremental_assets}
        for entity in full_extract_entities:
            normalized = normalize_danish_name(entity)
            assert normalized not in incremental_key_names, (
                f"Full-extract entity {entity!r} (normalized: {normalized!r}) "
                "incorrectly appears in incremental_assets"
            )

    def test_incremental_entities_are_not_in_full_extract_factory(self) -> None:
        """No incremental entity leaks into the full-extract asset list."""
        from ddd_python.ddd_dagster import assets as ddd_assets
        from ddd_python.ddd_utils import configuration_variables
        from ddd_python.ddd_utils.string_utils import normalize_danish_name

        full_extract_key_names = {a.key.path[-1] for a in ddd_assets.full_extract_assets}
        for entity in configuration_variables.DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL:
            normalized = normalize_danish_name(entity)
            assert normalized not in full_extract_key_names, (
                f"Incremental entity {entity!r} (normalized: {normalized!r}) "
                "incorrectly appears in full_extract_assets"
            )

    # ------------------------------------------------------------------
    # Spot-check normalised entity names appear in asset keys
    # ------------------------------------------------------------------

    @pytest.mark.parametrize(
        "entity,expected_key_name",
        [
            ("Aktør", "aktoer"),
            ("Møde", "moede"),
            ("SagstrinAktør", "sagstrinaktoer"),
            ("Stemme", "stemme"),
        ],
    )
    def test_ddd_normalized_name_in_asset_key(self, entity: str, expected_key_name: str) -> None:
        """Normalized entity names appear as the leaf segment of DDD asset keys."""
        from ddd_python.ddd_dagster import assets as ddd_assets

        all_key_names = {a.key.path[-1] for a in ddd_assets.all_extraction_assets}
        assert expected_key_name in all_key_names, (
            f"Expected normalized key name {expected_key_name!r} "
            f"(from entity {entity!r}) not found in DDD asset keys"
        )

    @pytest.mark.parametrize(
        "table_name",
        ["family", "genome", "clan", "clan_membership", "author"],
    )
    def test_rfam_table_name_in_asset_key(self, table_name: str) -> None:
        """Rfam table names appear as the leaf segment of Rfam asset keys."""
        from ddd_python.ddd_dagster import rfam_assets

        all_key_names = {a.key.path[-1] for a in rfam_assets.all_rfam_extraction_assets}
        assert table_name in all_key_names, (
            f"Expected Rfam table {table_name!r} not found in Rfam asset keys"
        )
