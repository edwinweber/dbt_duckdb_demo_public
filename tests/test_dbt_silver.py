"""Real dbt runs of the Silver layer.

Runs dbt (dbt-core + dbt-duckdb) on copies of the CI sample (``tests/fixtures/bronze_sample``) in pytest's
tmp directories and checks the Silver tables that both Silver macros produce:
``generate_model_silver_full_extraction`` and ``generate_model_silver_incr_extraction``
with their compile-time bookkeeping (``silver_files``) and post-hooks.  Every case in the table of
``documentation/dbt_transformations.md`` §3.10 has a test here, plus the building blocks
of §3.1 (hash, LKHS_ columns, file names, bookmark, ``_cv`` views).
``test_integration_silver_cdc.py`` reproduces parts of the SQL without dbt.

Files are named after their day in January 2026 (day 3 = ``<entity>_20260103_000000``),
so a history such as ``I1 D2 I3`` lists the operation and the day of
``LKHS_date_valid_from`` of every version of a key.

Skipped unless ``DBT_INTEGRATION_TESTS=1`` and the ``dbt`` binary is on ``PATH``; a run
takes several minutes.  CI runs these tests in the ``dbt build (sample data)`` job.
"""

import json
import os
import shutil
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

import duckdb
from ddd_python.ddd_dbt.generate_dbt_models import (
    generate_dbt_models_bronze,
    generate_dbt_models_silver,
)
from ddd_python.ddd_utils.primary_keys import PrimaryKey, stored_key_column

pytestmark = [
    pytest.mark.dbt_integration,
    pytest.mark.skipif(
        os.getenv("DBT_INTEGRATION_TESTS") != "1" or shutil.which("dbt") is None,
        reason="real dbt runs: set DBT_INTEGRATION_TESTS=1 and put the dbt binary on PATH",
    ),
]

REPO = Path(__file__).resolve().parents[1]
SAMPLE = REPO / "tests" / "fixtures" / "bronze_sample"
DBT_DIR = REPO / "dbt"


def _append(column: str) -> Callable[[dict], dict]:
    return lambda r: {**r, column: (r[column] or "") + " (changed)"}


ENTITIES: dict[str, dict[str, Any]] = {
    # incremental entities
    "stemme": {"source": "DDD", "pk": "id", "change": lambda r: {**r, "typeid": r["typeid"] + 100}},
    "family": {"source": "RFAM", "pk": "rfam_acc", "change": _append("description")},
    # full-extract entities
    "sagskategori": {"source": "DDD", "pk": "id", "change": _append("kategori")},
    "author": {"source": "RFAM", "pk": "author_id", "change": _append("name")},
    "periode": {"source": "DDD", "pk": "id", "change": _append("titel")},
}


def _dbt_project_with_key(root: Path, entity: str, primary_key: list[str]) -> Path:
    """A copy of the dbt project whose models of the DDD ``entity`` key on ``primary_key``,
    regenerated as ``generate_dbt_models`` writes them for a key of several columns."""
    dbt_dir = root / "dbt"
    shutil.copytree(DBT_DIR, dbt_dir, ignore=shutil.ignore_patterns("target", "logs"))
    _generate_models(dbt_dir, entity, primary_key)
    return dbt_dir


def _generate_models(dbt_dir: Path, entity: str, primary_key: PrimaryKey) -> None:
    """Write the Bronze and Silver models of the DDD ``entity`` for ``primary_key``."""
    key_map: dict[str, PrimaryKey] = {entity: primary_key}
    with patch("ddd_python.ddd_dbt.generate_dbt_models.get_variables_from_env") as env:
        env.DBT_MODELS_DIRECTORY = str(dbt_dir / "models")
        generate_dbt_models_bronze([f"bronze_ddd_{entity}"], primary_key_map=key_map)
        generate_dbt_models_silver([f"silver_ddd_{entity}"], primary_key_map=key_map)


class SilverProject:
    """dbt on an isolated copy of the sample Bronze files, for one Silver model.

    ``primary_key`` gives a DDD entity a key of several columns: the project then runs on a
    copy of the dbt project with that entity's models regenerated, and Silver keys on
    ``LKHS_primary_key_1_column``."""

    def __init__(
        self, root: Path, storage_format: str, entity: str, primary_key: list[str] | None = None
    ) -> None:
        self.root = root
        self.format = storage_format
        self.entity = entity
        e = ENTITIES[entity]
        self.key_columns: list[str] = primary_key or [e["pk"]]
        self.pk: str = stored_key_column(self.key_columns)
        self.dbt_dir = (
            DBT_DIR if primary_key is None else _dbt_project_with_key(root, entity, primary_key)
        )
        self.code: str = e["source"]
        self.change: Callable[[dict], dict] = e["change"]
        self.model = f"silver_{e['source'].lower()}_{entity}"
        for source in ("DDD", "RFAM"):
            shutil.copytree(SAMPLE / source, root / source)
        (root / "ducklake").mkdir()
        self.entity_dir = root / e["source"] / entity
        self.env = {
            **os.environ,
            "DANISH_DEMOCRACY_DATA_SOURCE": str(root / "DDD"),
            "RFAM_DATA_SOURCE": str(root / "RFAM"),
            "DUCKDB_DATABASE_LOCATION": str(root / "test.duckdb"),
            "DUCKDB_DATABASE": "test",
            "SILVER_STORAGE_FORMAT": storage_format,
            "RAW_STORAGE_TARGET": "local",
            "STORAGE_TARGET": "local",
            "DUCKLAKE_CATALOG_LOCATION": str(root / "catalog.ducklake"),
            "DUCKLAKE_DATA_PATH": str(root / "ducklake"),
        }

    # -- Bronze files --------------------------------------------------------------
    def file_name(self, day: int, suffix: str = ".json", full: bool = False) -> str:
        """``full`` marks a full extraction of an incremental entity (``..._full.json``)."""
        return f"{self.entity}_202601{day:02d}_000000{'_full' if full else ''}{suffix}"

    def key(self, row: dict) -> Any:
        """The Silver key of a Bronze row: its key column, or for a key of several columns
        the text the Bronze view computes, e.g. ``'10605]##[<NULL>]##['``."""
        if len(self.key_columns) == 1:
            return row[self.pk]
        return "".join(
            ("<NULL>" if row[c] is None else str(row[c])) + "]##[" for c in self.key_columns
        )

    def base_rows(self) -> list[dict]:
        text = (self.entity_dir / self.file_name(1)).read_text()
        return [json.loads(line) for line in text.splitlines() if line.strip()]

    def write(self, day: int, rows: list[dict], suffix: str = ".json", full: bool = False) -> None:
        text = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows)
        (self.entity_dir / self.file_name(day, suffix, full)).write_text(text)

    def remove(self, *days: int) -> None:
        for day in days:
            for full in (False, True):
                (self.entity_dir / self.file_name(day, full=full)).unlink(missing_ok=True)

    # -- dbt -----------------------------------------------------------------------
    def dbt(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["dbt", *args, "--project-dir", str(self.dbt_dir), "--profiles-dir", str(self.dbt_dir),
             "--target-path", str(self.root / "target"), "--log-path", str(self.root / "logs")],
            env=self.env, capture_output=True, text=True, check=False,
        )  # fmt: skip

    def _must(self, *args: str) -> None:
        result = self.dbt(*args)
        assert result.returncode == 0, f"dbt {' '.join(args)} failed:\n{result.stdout[-3000:]}"

    def build(self, full_refresh: bool = False) -> None:
        """First build: Bronze views, the Silver table and its _cv view."""
        args = ["run", "--select", f"+{self.model}_cv"]
        if full_refresh:
            args.append("--full-refresh")
        self._must(*args)

    def run(self, full_refresh: bool = False, dbt_vars: str | None = None) -> None:
        """Bronze views + Silver, as the pipeline runs them."""
        self._must(*self._run_args(full_refresh, dbt_vars))

    def run_fails(self, message: str) -> None:
        """A normal run that must fail with ``message`` in dbt's output."""
        result = self.dbt(*self._run_args())
        assert result.returncode != 0, "the run should have failed"
        assert message in result.stdout, result.stdout[-3000:]

    def _run_args(self, full_refresh: bool = False, dbt_vars: str | None = None) -> list[str]:
        args = ["run", "--select", f"+{self.model}"]
        if full_refresh:
            args.append("--full-refresh")
        if dbt_vars:
            args += ["--vars", dbt_vars]
        return args

    def data_tests_pass(self) -> bool:
        """The model's generic data tests (the singular grain test needs all DDD tables)."""
        result = self.dbt(
            "test", "--select", self.model, f"{self.model}_cv", "--exclude", "test_type:singular"
        )
        return result.returncode == 0

    # -- reading the database ------------------------------------------------------
    def sql(self, query: str, params: list | None = None) -> list[tuple]:
        """Run a query; {t}, {cv}, {lf} and {fl} are the Silver table, its _cv view, its
        _last_file table and its _files log, {bronze} is the Bronze view."""
        con = duckdb.connect(str(self.root / "test.duckdb"), read_only=True)
        try:
            schema = "test.main_silver"
            if self.format == "ducklake":
                con.execute("INSTALL ducklake; LOAD ducklake")
                con.execute(f"ATTACH 'ducklake:{self.root / 'catalog.ducklake'}' AS dl (READ_ONLY)")
                schema = "dl.main_silver"
            bronze = f"test.main_bronze.{self.model.replace('silver_', 'bronze_', 1)}"
            query = query.format(
                t=f"{schema}.{self.model}",
                cv=f"{schema}.{self.model}_cv",
                lf=f"{schema}.{self.model}_last_file",
                fl=f"{schema}.{self.model}_files",
                bronze=bronze,
            )
            return con.execute(query, params or []).fetchall()
        finally:
            con.close()

    def histories(self, keys: dict[Any, str]) -> dict[str, str]:
        """'I1 D2 I3' per named key: operation + day of LKHS_date_valid_from."""
        out = {}
        for key, name in keys.items():
            rows = self.sql(
                f"SELECT LKHS_cdc_operation, day(LKHS_date_valid_from) FROM {{t}} "
                f"WHERE {self.pk} = ? ORDER BY LKHS_date_valid_from, LKHS_cdc_operation",
                [key],
            )
            out[name] = " ".join(f"{op}{day}" for op, day in rows)
        return out

    def versions(self, key: Any) -> list[dict]:
        rows = self.sql(
            "SELECT LKHS_cdc_operation, day(LKHS_date_valid_from), LKHS_filename, LKHS_hash_value, "
            "LKHS_date_inserted, LKHS_source_system_code "
            f"FROM {{t}} WHERE {self.pk} = ? ORDER BY LKHS_date_valid_from",
            [key],
        )
        names = ["op", "day", "filename", "hash", "inserted", "code"]
        return [dict(zip(names, r, strict=True)) for r in rows]

    def snapshot(self) -> list[tuple]:
        """Whole table without LKHS_date_inserted, columns in name order."""
        columns = sorted(c[0] for c in self.sql("DESCRIBE {t}") if c[0] != "LKHS_date_inserted")
        rows = self.sql(
            f"SELECT {', '.join(columns)} FROM {{t}} "
            f"ORDER BY {self.pk}, LKHS_date_valid_from, LKHS_cdc_operation"
        )
        return [tuple(columns), *rows]

    def operations_on(self, day: int) -> dict[str, int]:
        rows = self.sql(
            "SELECT LKHS_cdc_operation, COUNT(*) FROM {t} "
            f"WHERE day(LKHS_date_valid_from) = {day} GROUP BY 1"
        )
        return dict(rows)

    def violations(self) -> dict[str, int]:
        """Rows breaking each Silver invariant; all 0 for a consistent history."""
        pk = self.pk
        lag = f"OVER (PARTITION BY {pk} ORDER BY LKHS_date_valid_from)"
        checks = {
            "duplicate (key, valid_from)": f"""
                SELECT COUNT(*) FROM (SELECT {pk}, LKHS_date_valid_from FROM {{t}}
                GROUP BY ALL HAVING COUNT(*) > 1)""",
            "file name timestamp differs from valid_from": """
                SELECT COUNT(*) FROM {t}
                WHERE try_strptime(regexp_extract(LKHS_filename, '([0-9]{{8}}_[0-9]{{6}})', 1),
                                   '%Y%m%d_%H%M%S') IS DISTINCT FROM LKHS_date_valid_from""",
            "D row without the hash of the version it deletes": f"""
                SELECT COUNT(*) FROM (SELECT LKHS_cdc_operation op, LKHS_hash_value h,
                    LAG(LKHS_hash_value) {lag} ph FROM {{t}})
                WHERE op = 'D' AND h IS DISTINCT FROM ph""",
            "U row with the hash of the version before it": f"""
                SELECT COUNT(*) FROM (SELECT LKHS_cdc_operation op, LKHS_hash_value h,
                    LAG(LKHS_hash_value) {lag} ph, LAG(LKHS_cdc_operation) {lag} pop FROM {{t}})
                WHERE op = 'U' AND pop <> 'D' AND h = ph""",
            "D directly after a D": f"""
                SELECT COUNT(*) FROM (SELECT LKHS_cdc_operation op,
                    LAG(LKHS_cdc_operation) {lag} pop FROM {{t}})
                WHERE op = 'D' AND pop = 'D'""",
            "I directly after an I or U": f"""
                SELECT COUNT(*) FROM (SELECT LKHS_cdc_operation op,
                    LAG(LKHS_cdc_operation) {lag} pop FROM {{t}})
                WHERE op = 'I' AND pop IN ('I', 'U')""",
            "first version not I": f"""
                SELECT COUNT(*) FROM (SELECT LKHS_cdc_operation op FROM {{t}}
                    QUALIFY ROW_NUMBER() {lag} = 1)
                WHERE op <> 'I'""",
            "wrong source system code": f"""
                SELECT COUNT(*) FROM {{t}} WHERE LKHS_source_system_code IS DISTINCT FROM '{self.code}'""",
            "_cv is not the newest version per key": f"""
                SELECT (SELECT COUNT(*) FROM {{cv}}) - (SELECT COUNT(DISTINCT {pk}) FROM {{t}})
                     + (SELECT COUNT(*) FROM {{cv}} cv WHERE cv.LKHS_date_valid_from <>
                           (SELECT MAX(t.LKHS_date_valid_from) FROM {{t}} t WHERE t.{pk} = cv.{pk}))""",
        }
        return {name: self.sql(query)[0][0] for name, query in checks.items()}


def _violations(project: SilverProject) -> dict[str, int]:
    return {name: n for name, n in project.violations().items() if n}


@pytest.fixture()
def project(tmp_path) -> Callable[..., SilverProject]:
    """Factory: project("stemme"), project("sagskategori", "ducklake") or
    project("stemme", primary_key=["afstemningid", "aktoerid"])."""
    counter = iter(range(100))

    def make(
        entity: str, storage_format: str = "duckdb", primary_key: list[str] | None = None
    ) -> SilverProject:
        root = tmp_path / f"{entity}_{storage_format}_{next(counter)}"
        root.mkdir()
        return SilverProject(root, storage_format, entity, primary_key)

    return make


def _lifecycle_project(root: Path, combination: tuple[str, ...]) -> SilverProject:
    """(storage format, entity[, key columns joined by '+']) -> SilverProject."""
    storage_format, entity, *key = combination
    return SilverProject(root, storage_format, entity, key[0].split("+") if key else None)


# ═════════════════════════════════════════════════════════════════════════════════
# Building blocks (§3.1)
# ═════════════════════════════════════════════════════════════════════════════════


def test_hash_is_sha256_of_the_business_columns_in_bronze_order(project):
    """LKHS_hash_value = sha256 of COALESCE(col::VARCHAR, '<NULL>') || ']##[' for every
    Bronze column except the key, LKHS_filename and LKHS_date_inserted."""
    p = project("sagskategori")
    p.build()
    bronze_columns = [c[0] for c in p.sql("DESCRIBE {bronze}")]
    hashed = [c for c in bronze_columns if c not in ("id", "LKHS_filename", "LKHS_date_inserted")]
    assert hashed == ["kategori", "opdateringsdato", "LKHS_source_system_code"]
    expression = (
        "CONCAT(" + ", ".join(f"COALESCE({c}::VARCHAR, '<NULL>'), ']##['" for c in hashed) + ")"
    )
    mismatches = p.sql(
        f"SELECT COUNT(*) FROM {{t}} s JOIN (SELECT id, sha256({expression}) AS h FROM {{bronze}}) b "
        "USING (id) WHERE s.LKHS_hash_value <> b.h"
    )[0][0]
    assert mismatches == 0
    assert p.sql("SELECT COUNT(*) FROM {t}")[0][0] == len(p.base_rows())


def test_hash_null_token_and_delimiter(project):
    """NULL and '' hash differently; NULL and the literal '<NULL>' hash the same, and so do
    two adjacent values that only differ in where the delimiter ']##[' falls."""
    p = project("periode")
    rows = p.base_rows()
    a, b, c = (dict(r) for r in rows[:3])
    a["titel"], b["titel"] = None, ""
    c["type"], c["kode"] = "x]##[", "y"
    p.write(1, [a, b, c, *rows[3:]])
    p.build()
    a2 = {**a, "titel": "<NULL>"}  # NULL -> literal '<NULL>'
    b2 = {**b, "titel": None}  # ''   -> NULL
    c2 = {**c, "type": "x", "kode": "]##[y"}  # delimiter moved between the columns
    p.write(2, [a2, b2, c2, *rows[3:]])
    p.run()
    histories = p.histories({a["id"]: "a", b["id"]: "b", c["id"]: "c"})
    assert histories == {"a": "I1", "b": "I1 U2", "c": "I1"}


@pytest.mark.parametrize("storage_format", ["duckdb", "ducklake"])
def test_key_of_several_columns(project, storage_format):
    """A key of several columns is the character column LKHS_primary_key_1_column: computed
    in the Bronze view as COALESCE(col::VARCHAR, '<NULL>') || ']##[' per key column, stored
    in Silver, and left out of the hash together with its source columns."""
    key = ["afstemningid", "aktoerid"]
    p = project("stemme", storage_format, primary_key=key)
    rows = p.base_rows()
    rows[0] = {**rows[0], "aktoerid": None}
    p.write(1, rows)
    p.build()
    assert p.pk == "LKHS_primary_key_1_column"
    bronze_types = dict((c[0], c[1]) for c in p.sql("DESCRIBE {bronze}"))
    assert bronze_types[p.pk] == "VARCHAR"
    assert dict((c[0], c[1]) for c in p.sql("DESCRIBE {t}"))[p.pk] == "VARCHAR"
    assert p.key(rows[0]) == f"{rows[0]['afstemningid']}]##[<NULL>]##["
    assert sorted(r[0] for r in p.sql(f"SELECT {p.pk} FROM {{t}}")) == sorted(map(p.key, rows))

    excluded = {*key, p.pk, "LKHS_filename", "LKHS_date_inserted"}
    hashed = [c for c in bronze_types if c not in excluded]
    assert "id" in hashed  # the source's own id is an ordinary column now
    expression = (
        "CONCAT(" + ", ".join(f"COALESCE({c}::VARCHAR, '<NULL>'), ']##['" for c in hashed) + ")"
    )
    mismatches = p.sql(
        f"SELECT COUNT(*) FROM {{t}} s JOIN (SELECT {p.pk}, sha256({expression}) AS h "
        f"FROM {{bronze}}) b USING ({p.pk}) WHERE s.LKHS_hash_value <> b.h"
    )[0][0]
    assert mismatches == 0

    # A change of a key column is a new key: the old key keeps its history, and only a
    # full file deletes it.
    moved = {**rows[1], "aktoerid": -1, "id": -1}  # own id: the YAML tests id unique in _cv
    p.write(2, [moved])
    p.run()
    p.write(3, [moved, *rows[2:]], full=True)
    p.run()
    histories = p.histories({p.key(rows[1]): "old", p.key(moved): "new", p.key(rows[0]): "null"})
    assert histories == {"old": "I1 D3", "new": "I2", "null": "I1 D3"}
    assert not _violations(p)
    assert p.data_tests_pass()


@pytest.mark.parametrize("migrated", [True, False], ids=["migrated", "not migrated"])
def test_changing_to_a_key_of_several_columns(project, migrated):
    """An existing Silver table keyed on id gets a key of several columns.  After the
    ALTER TABLE and UPDATE of dbt_transformations.md §3.1 the next run writes only the real
    change; without them the run stops, because Silver has no key column yet."""
    key = ["afstemningid", "aktoerid"]
    p = project("stemme", primary_key=key)
    _generate_models(p.dbt_dir, "stemme", "id")
    rows = p.base_rows()
    p.build()
    assert p.pk not in [c[0] for c in p.sql("DESCRIBE {t}")]
    _generate_models(p.dbt_dir, "stemme", key)
    if migrated:
        con = duckdb.connect(str(p.root / "test.duckdb"))
        try:
            con.execute(f"ALTER TABLE test.main_silver.{p.model} ADD COLUMN {p.pk} VARCHAR")
            con.execute(
                f"UPDATE test.main_silver.{p.model} SET {p.pk} = "
                "CONCAT(COALESCE(afstemningid::VARCHAR, '<NULL>'), ']##[', "
                "COALESCE(aktoerid::VARCHAR, '<NULL>'), ']##[')"
            )
        finally:
            con.close()
    p.write(2, [p.change(rows[0]), *rows[1:]])
    if not migrated:
        p.run_fails(f"Referenced column {p.pk} not found")
        assert p.pk not in [c[0] for c in p.sql("DESCRIBE {t}")]
        assert p.sql("SELECT COUNT(*) FROM {t}")[0][0] == len(rows)
        return
    p.build()
    assert p.operations_on(2) == {"U": 1}
    p.write(3, rows[1:], full=True)
    p.run()
    assert p.operations_on(3) == {"D": 1}
    assert not _violations(p)


def test_file_name_variants_and_bookmark(project):
    """Files named .json, .jsonl and .json.jsonl (dlt versions) are all read; valid_from
    comes from the timestamp in the name; _last_file holds the newest processed file and
    _files logs every processed file."""
    p = project("sagskategori")
    rows = p.base_rows()
    p.build()
    assert p.sql("SELECT LKHS_filename FROM {lf}") == [(p.file_name(1),)]
    p.write(2, [p.change(rows[0]), *rows[1:]], suffix=".jsonl")
    p.run()
    p.write(3, [p.change(rows[0]), p.change(rows[1]), *rows[2:]], suffix=".json.jsonl")
    p.run()
    assert p.histories({rows[0]["id"]: "a", rows[1]["id"]: "b"}) == {"a": "I1 U2", "b": "I1 U3"}
    assert [v["filename"] for v in p.versions(rows[1]["id"])] == [
        p.file_name(1),
        p.file_name(3, ".json.jsonl"),
    ]
    assert p.sql("SELECT LKHS_filename FROM {lf}") == [(p.file_name(3, ".json.jsonl"),)]
    assert p.sql("SELECT LKHS_filename, LKHS_status FROM {fl} ORDER BY 1") == [
        (p.file_name(1), "processed"),
        (p.file_name(2, ".jsonl"), "processed"),
        (p.file_name(3, ".json.jsonl"), "processed"),
    ]
    assert not _violations(p)


def test_identical_rows_in_one_file_are_one_version(project):
    """Bronze selects DISTINCT (without the _dlt_ columns), so an exact duplicate in a file
    gives no extra version."""
    p = project("sagskategori")
    rows = p.base_rows()
    p.build()
    p.write(2, [*rows, {**rows[0], "_dlt_id": "another-dlt-id"}])
    p.run()
    assert p.histories({rows[0]["id"]: "a"}) == {"a": "I1"}
    assert not _violations(p)


def test_source_system_code_and_date_inserted(project):
    """LKHS_date_inserted is the start time of the dbt run that wrote the row; every row
    of one run has the same value."""
    p = project("sagskategori")
    rows = p.base_rows()
    p.build()
    first_run = p.sql("SELECT DISTINCT LKHS_date_inserted FROM {t}")
    assert len(first_run) == 1
    p.write(2, [p.change(rows[0]), *rows[1:]])
    p.run()
    inserted = p.sql("SELECT DISTINCT LKHS_date_inserted FROM {t} ORDER BY 1")
    assert len(inserted) == 2 and inserted[0] == first_run[0]
    assert p.sql("SELECT DISTINCT LKHS_source_system_code FROM {t}") == [("DDD",)]


# ═════════════════════════════════════════════════════════════════════════════════
# Full-extract entities: lifecycle (§3.2, §3.5)
# ═════════════════════════════════════════════════════════════════════════════════
#
# Five complete files.
#   A  unchanged                                       I1
#   B  missing on day 2, back unchanged on day 3       I1 D2 I3
#   C  missing on day 2, back changed on day 3         I1 D2 I3
#   D  missing from day 2 on                           I1 D2
#   E  changed on days 2 and 4                         I1 U2 U4
#   G  missing on days 3 and 4, back on day 5          I1 D3 I5
#   J  missing on days 2 and 4, back on days 3 and 5   I1 D2 I3 D4 I5
#   N  new on day 2                                    I2

FULL_EXPECTED = {
    1: {"A": "I1", "B": "I1", "C": "I1", "D": "I1", "E": "I1", "G": "I1", "J": "I1", "N": ""},
    2: {"A": "I1", "B": "I1 D2", "C": "I1 D2", "D": "I1 D2", "E": "I1 U2", "G": "I1",
        "J": "I1 D2", "N": "I2"},
    3: {"A": "I1", "B": "I1 D2 I3", "C": "I1 D2 I3", "D": "I1 D2", "E": "I1 U2", "G": "I1 D3",
        "J": "I1 D2 I3", "N": "I2"},
    4: {"A": "I1", "B": "I1 D2 I3", "C": "I1 D2 I3", "D": "I1 D2", "E": "I1 U2 U4", "G": "I1 D3",
        "J": "I1 D2 I3 D4", "N": "I2"},
    5: {"A": "I1", "B": "I1 D2 I3", "C": "I1 D2 I3", "D": "I1 D2", "E": "I1 U2 U4",
        "G": "I1 D3 I5", "J": "I1 D2 I3 D4 I5", "N": "I2"},
}  # fmt: skip

FULL_PATHS = ["one run per file", "days 2-5 in one normal run", "first build with all files"]

# The last one keys on two columns: Silver keys on LKHS_primary_key_1_column.
FULL_COMBINATIONS = [("duckdb", "sagskategori"), ("ducklake", "sagskategori"),
                     ("duckdb", "periode", "id+kode")]  # fmt: skip


def _full_extract_files(p: SilverProject) -> tuple[dict[int, list[dict]], dict[Any, str]]:
    rows = p.base_rows()
    keys = [p.key(r) for r in rows[:8]]
    names = dict(zip(keys, "ABCDEGJN", strict=True))
    a, b, c, d, e, g, j, n = keys
    state = {p.key(r): r for r in rows}
    order = [p.key(r) for r in rows]

    def complete(missing: set) -> list[dict]:
        return [state[k] for k in order if k not in missing]

    files = {1: complete({n})}
    state[e] = p.change(state[e])
    files[2] = complete({b, c, d, j})
    state[c] = p.change(state[c])
    files[3] = complete({d, g})
    state[e] = p.change(state[e])
    files[4] = complete({d, g, j})
    files[5] = complete({d})
    return files, names


@pytest.fixture(scope="module", params=FULL_COMBINATIONS, ids="-".join)
def full_lifecycle(request, tmp_path_factory) -> dict[str, dict[str, Any]]:
    results = {}
    for path in FULL_PATHS:
        label = "_".join(request.param).replace("+", "_")
        root = tmp_path_factory.mktemp(f"full_{label}_{path.replace(' ', '_')}")
        p = _lifecycle_project(root, request.param)
        files, names = _full_extract_files(p)
        record: dict[str, Any] = {"histories": {}, "violations": {}}
        p.write(1, files[1])
        if path == "one run per file":
            p.build()
            record["histories"][1] = p.histories(names)
            for day in range(2, 6):
                p.write(day, files[day])
                p.run()
                record["histories"][day] = p.histories(names)
                record["violations"][day] = _violations(p)
            before = p.snapshot()
            p.run()
            record["repeat run unchanged"] = p.snapshot() == before
        elif path == "days 2-5 in one normal run":
            p.build()
            for day in range(2, 6):
                p.write(day, files[day])
            p.run()
        else:
            for day in range(2, 6):
                p.write(day, files[day])
            p.build()
        record["histories"][5] = p.histories(names)
        record["violations"][5] = _violations(p)
        record["cv"] = dict(p.sql(f"SELECT {p.pk}, LKHS_cdc_operation FROM {{cv}}"))
        record["names"] = names
        record["entity"] = p.entity
        record["d_row_of_D"] = p.versions(next(k for k, v in names.items() if v == "D"))[-1]
        before = p.snapshot()
        p.run(full_refresh=True)
        record["full refresh unchanged"] = p.snapshot() == before
        record["data tests pass"] = p.data_tests_pass()
        record["snapshot"] = before
        results[path] = record
    return results


@pytest.mark.parametrize("path", FULL_PATHS)
def test_full_extract_histories(full_lifecycle, path):
    for day, histories in full_lifecycle[path]["histories"].items():
        assert histories == FULL_EXPECTED[day], f"{path}, after day {day}"


@pytest.mark.parametrize("path", FULL_PATHS)
def test_full_extract_invariants(full_lifecycle, path):
    for day, violations in full_lifecycle[path]["violations"].items():
        assert not violations, f"{path}, after day {day}: {violations}"


def test_full_extract_delete_row_takes_the_file_without_the_key(full_lifecycle):
    record = full_lifecycle["one run per file"]
    d_row = record["d_row_of_D"]
    assert d_row["op"] == "D" and d_row["day"] == 2
    assert d_row["filename"] == f"{record['entity']}_20260102_000000.json"


def test_full_extract_cv_shows_deleted_keys(full_lifecycle):
    record = full_lifecycle["one run per file"]
    deleted = {k for k, v in record["names"].items() if v == "D"}
    assert {k: record["cv"][k] for k in deleted} == {k: "D" for k in deleted}


def test_full_extract_repeat_run_and_full_refresh_change_nothing(full_lifecycle):
    assert full_lifecycle["one run per file"]["repeat run unchanged"]
    for path in FULL_PATHS:
        assert full_lifecycle[path]["full refresh unchanged"], path


def test_full_extract_paths_give_identical_tables(full_lifecycle):
    reference = full_lifecycle[FULL_PATHS[0]]["snapshot"]
    for path in FULL_PATHS[1:]:
        assert full_lifecycle[path]["snapshot"] == reference, path


@pytest.mark.parametrize("path", FULL_PATHS)
def test_full_extract_data_tests_pass(full_lifecycle, path):
    assert full_lifecycle[path]["data tests pass"]


# ═════════════════════════════════════════════════════════════════════════════════
# Incremental entities: lifecycle (§3.3, §3.4, §3.5)
# ═════════════════════════════════════════════════════════════════════════════════
#
# Day 1 complete; day 2, 4 and 5 are full extractions (file name ends in _full); day 3 is an
# incremental file.  Every file is processed on its own, as a full file must be; only a full
# file deletes.
#
#   A  missing on day 2, back unchanged on day 3          I1 D2 I3
#   B  missing on day 2, back changed on day 3            I1 D2 I3
#   C  missing on day 2, back on day 3, missing on day 4  I1 D2 I3 D4
#   D  missing from day 2 on                              I1 D2
#   E  changed on day 3                                   I1 U3
#   G  missing on day 2, back on day 3, changed on day 5  I1 D2 I3 U5
#   H  never changes                                      I1
#   J  two delete/insert cycles                           I1 D2 I3 D4 I5

INCR_EXPECTED = {
    "day 2": {"A": "I1 D2", "B": "I1 D2", "C": "I1 D2", "D": "I1 D2",
              "E": "I1", "G": "I1 D2", "H": "I1", "J": "I1 D2"},
    "day 3": {"A": "I1 D2 I3", "B": "I1 D2 I3", "C": "I1 D2 I3", "D": "I1 D2",
              "E": "I1 U3", "G": "I1 D2 I3", "H": "I1", "J": "I1 D2 I3"},
    "day 4": {"A": "I1 D2 I3", "B": "I1 D2 I3", "C": "I1 D2 I3 D4", "D": "I1 D2",
              "E": "I1 U3", "G": "I1 D2 I3", "H": "I1", "J": "I1 D2 I3 D4"},
    "day 5": {"A": "I1 D2 I3", "B": "I1 D2 I3", "C": "I1 D2 I3 D4", "D": "I1 D2",
              "E": "I1 U3", "G": "I1 D2 I3 U5", "H": "I1", "J": "I1 D2 I3 D4 I5"},
}  # fmt: skip

# The last two key on two columns: Silver keys on LKHS_primary_key_1_column.
INCR_COMBINATIONS = [("duckdb", "stemme"), ("ducklake", "stemme"), ("duckdb", "family"),
                     ("duckdb", "stemme", "afstemningid+aktoerid"),
                     ("ducklake", "stemme", "afstemningid+aktoerid")]  # fmt: skip

FULL_DAYS = {2, 4, 5}  # the full extractions of the incremental lifecycle


def _incremental_files(p: SilverProject) -> tuple[dict[int, list[dict]], dict[Any, str]]:
    rows = p.base_rows()
    keys = [p.key(r) for r in rows[:8]]
    names = dict(zip(keys, "ABCDEGHJ", strict=True))
    a, b, c, d, e, g, _h, j = keys
    state = {p.key(r): r for r in rows}
    order = [p.key(r) for r in rows]

    def complete(missing: set) -> list[dict]:
        return [state[k] for k in order if k not in missing]

    files = {2: complete({a, b, c, d, g, j})}
    state[b], state[e] = p.change(state[b]), p.change(state[e])
    files[3] = [state[k] for k in (a, b, c, e, g, j)]
    files[4] = complete({c, d, j})
    state[g] = p.change(state[g])
    files[5] = complete({c, d})
    return files, names


def _run_incremental_lifecycle(p: SilverProject) -> dict[str, Any]:
    files, names = _incremental_files(p)
    record: dict[str, Any] = {"names": names}

    def observe(label: str) -> None:
        record[f"histories {label}"] = p.histories(names)
        record[f"violations {label}"] = _violations(p)

    p.build()
    for day in (2, 3, 4, 5):
        p.write(day, files[day], full=day in FULL_DAYS)
        p.run()
        observe(f"day {day}")
        if day == 3:
            before = p.snapshot()
            p.run()
            record["repeat run unchanged"] = p.snapshot() == before
    everything = "SELECT * FROM {t} ORDER BY ALL"
    before_rows = p.sql(everything)
    p.run(full_refresh=True)  # ignored: the delete history cannot be rebuilt from Bronze
    record["full refresh ignored"] = p.sql(everything) == before_rows
    observe("day 5 after full refresh")
    record["data tests pass"] = p.data_tests_pass()
    return record


@pytest.fixture(scope="module", params=INCR_COMBINATIONS, ids="-".join)
def incremental_lifecycle(request, tmp_path_factory) -> dict[str, Any]:
    root = tmp_path_factory.mktemp("incr_" + "_".join(request.param).replace("+", "_"))
    return _run_incremental_lifecycle(_lifecycle_project(root, request.param))


def test_incremental_histories(incremental_lifecycle):
    for label, histories in incremental_lifecycle.items():
        if label.startswith("histories "):
            day = label.removeprefix("histories ").removesuffix(" after full refresh")
            assert histories == INCR_EXPECTED[day], label


def test_incremental_invariants(incremental_lifecycle):
    for label, violations in incremental_lifecycle.items():
        if label.startswith("violations "):
            assert not violations, f"{label}: {violations}"


def test_incremental_repeat_run_changes_nothing_and_full_refresh_is_ignored(incremental_lifecycle):
    assert incremental_lifecycle["repeat run unchanged"]
    assert incremental_lifecycle["full refresh ignored"]


def test_incremental_data_tests_pass(incremental_lifecycle):
    assert incremental_lifecycle["data tests pass"]


# ═════════════════════════════════════════════════════════════════════════════════
# Incremental entities: deletes come from _full files (§3.4)
# ═════════════════════════════════════════════════════════════════════════════════


def test_a_first_build_writes_no_deletes(project):
    """Deletes compare a full file with Silver, so a first build writes none, also with a
    _full file among the files."""
    p = project("stemme")
    rows = p.base_rows()
    p.write(2, rows[1:], full=True)  # key 0 missing from a full file
    p.build()
    assert p.histories({rows[0]["id"]: "k"}) == {"k": "I1"}


def test_a_plain_file_never_deletes_even_when_it_is_complete(project):
    p = project("stemme")
    rows = p.base_rows()
    p.write(2, rows[1:])  # key 0 missing, but the file is not marked _full
    p.build()
    p.write(3, rows[2:])
    p.run()
    assert p.histories({rows[0]["id"]: "k0", rows[1]["id"]: "k1"}) == {"k0": "I1", "k1": "I1"}


def test_new_delete_has_the_full_file_and_the_deleted_content(project):
    p = project("stemme")
    rows = p.base_rows()
    k = rows[0]["id"]
    p.build()
    p.write(2, [p.change(rows[0])])  # incremental file: k changed
    p.run()
    p.write(3, rows[1:], full=True)  # full file without k, found by a normal run
    p.run()
    u, d = p.versions(k)[1:]
    assert (u["op"], d["op"], d["day"]) == ("U", "D", 3)
    assert d["filename"] == p.file_name(3, full=True)
    assert d["hash"] == u["hash"]


def test_a_key_that_is_back_before_the_next_full_file_is_not_deleted(project):
    p = project("stemme")
    rows = p.base_rows()
    k = rows[0]["id"]
    p.build()
    p.write(2, rows[1:])  # plain file without k
    p.run()
    p.write(3, rows, full=True)  # full file with k
    p.run()
    assert p.histories({k: "k"}) == {"k": "I1"}


@pytest.mark.parametrize("together", [False, True], ids=["one run per file", "one run"])
def test_several_plain_files_in_one_run(project, together):
    """Only a full file must be processed on its own: plain files after a delete can be
    processed together and give the same history, including a deleted key that comes back."""
    p = project("stemme")
    rows = p.base_rows()
    back, changed = rows[0], rows[1]
    names = {back["id"]: "back", changed["id"]: "changed"}
    p.build()
    p.write(2, rows[2:], full=True)  # both keys deleted
    p.run()
    p.write(3, [back])  # back with its old content
    if not together:
        p.run()
    p.write(4, [back, p.change(changed)])  # the other key back, changed
    p.run()
    assert p.histories(names) == {"back": "I1 D2 I3", "changed": "I1 D2 I4"}
    assert not _violations(p)


def test_a_full_file_with_other_files_to_process_stops_the_run(project):
    """A full file must be the only file to process: the run fails and writes nothing.
    Processed one at a time, the same files give the delete."""
    p = project("stemme")
    rows = p.base_rows()
    k = rows[0]["id"]
    p.build()
    p.write(2, rows)
    p.write(3, rows[1:], full=True)  # full file without k, pending together with day 2
    before = p.snapshot()
    result = p.dbt("run", "--select", f"+{p.model}")
    assert result.returncode != 0
    assert "must be the only Bronze file to process" in result.stdout
    assert p.snapshot() == before
    p.remove(3)
    p.run()
    p.write(3, rows[1:], full=True)
    p.run()
    assert p.histories({k: "k"}) == {"k": "I1 D3"}


def test_a_full_file_marked_by_mistake_is_stopped_by_the_delete_guard(project):
    """A _full file that is not complete would delete every key it lacks, so the delete guard
    stops the run and nothing is written.  Forced through, the next real full extraction brings
    the keys back as I; the false D rows stay in the history (runbook §7)."""
    p = project("stemme")
    rows = p.base_rows()
    kept, gone = rows[0]["id"], rows[100]["id"]
    names = {kept: "kept", gone: "gone"}
    p.build()
    p.write(2, rows[:5], full=True)  # incremental file with five keys, marked _full by mistake
    before = p.snapshot()
    p.run_fails(f"would delete {len(rows) - 5} of {len(rows)} live keys")
    assert p.snapshot() == before
    assert p.sql("SELECT LKHS_filename FROM {lf}") == [(p.file_name(1),)]
    p.run(dbt_vars="{silver_delete_guard: false}")
    assert p.histories(names) == {"kept": "I1", "gone": "I1 D2"}
    assert p.sql("SELECT COUNT(*) FROM {cv} WHERE LKHS_cdc_operation = 'D'")[0][0] == len(rows) - 5
    p.write(3, rows, full=True)  # the real full extraction
    p.run()
    assert p.histories(names) == {"kept": "I1", "gone": "I1 D2 I3"}
    assert p.sql("SELECT COUNT(*) FROM {cv} WHERE LKHS_cdc_operation = 'D'")[0][0] == 0
    assert not _violations(p)


# ═════════════════════════════════════════════════════════════════════════════════
# Files that arrive late (§3.8)
# ═════════════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize("entity", ["sagskategori", "stemme"])
def test_a_late_file_stops_the_run_until_it_is_skipped(project, entity):
    """A run reads only the files newer than the bookmark, so a file with an older timestamp
    that arrives later can never be merged into the history.  It is not in the _files log, so
    the run stops; with silver_skip_late_files it is logged as skipped and never read."""
    p = project(entity)
    rows = p.base_rows()
    k = rows[0][p.pk]
    p.build()
    p.write(3, rows)
    p.run()  # bookmark: day 3
    p.write(2, [p.change(rows[0]), *rows[1:]])  # day 2 arrives late
    changed = [rows[0], p.change(rows[1]), *rows[2:]]
    p.write(4, changed if entity == "sagskategori" else changed[1:2])
    before = p.snapshot()
    p.run_fails(f"never processed: {p.file_name(2)}")
    assert p.snapshot() == before
    p.run(dbt_vars="{silver_skip_late_files: true}")
    assert p.histories({k: "k", rows[1][p.pk]: "next"}) == {"k": "I1", "next": "I1 U4"}
    assert p.sql("SELECT LKHS_filename, LKHS_status FROM {fl} ORDER BY 1") == [
        (p.file_name(1), "processed"),
        (p.file_name(2), "skipped"),
        (p.file_name(3), "processed"),
        (p.file_name(4), "processed"),
    ]
    p.run()  # a skipped file is known: later runs go on
    assert not _violations(p)


def test_a_silver_table_from_an_older_version_is_migrated(project):
    """A Silver table built by an older version has a _last_file table with the old columns
    and no _files log.  The next run converts the bookmark in place (the new column comes last,
    so the bookmark is written by column name) and seeds the log with the files then in the
    folder up to the bookmark: they are not late files."""
    p = project("sagskategori")
    rows = p.base_rows()
    p.build()
    p.write(2, rows)
    p.run()
    con = duckdb.connect(str(p.root / "test.duckdb"))
    schema = "test.main_silver"
    con.execute(f"DROP TABLE {schema}.{p.model}_files")
    con.execute(f"DROP TABLE {schema}.{p.model}_last_file")
    con.execute(
        f"CREATE TABLE {schema}.{p.model}_last_file (LKHS_filename VARCHAR, "
        "LKHS_date_valid_from TIMESTAMP, LKHS_filename_previous VARCHAR, "
        "LKHS_date_valid_from_previous TIMESTAMP, LKHS_date_inserted TIMESTAMP)"
    )
    con.execute(
        f"INSERT INTO {schema}.{p.model}_last_file VALUES (?, '2026-01-02', ?, '2026-01-01', NULL)",
        [p.file_name(2), p.file_name(1)],
    )
    con.close()
    p.write(3, [p.change(rows[0]), *rows[1:]])
    p.run()
    assert [c[0] for c in p.sql("DESCRIBE {lf}")] == [
        "LKHS_filename", "LKHS_date_inserted", "LKHS_filename_pending"
    ]  # fmt: skip
    assert p.sql("SELECT LKHS_filename, LKHS_filename_pending FROM {lf}") == [
        (p.file_name(3), p.file_name(3))
    ]
    assert p.sql("SELECT LKHS_filename, LKHS_status FROM {fl} ORDER BY 1") == [
        (p.file_name(1), "seeded"),
        (p.file_name(2), "seeded"),
        (p.file_name(3), "processed"),
    ]
    assert p.histories({rows[0]["id"]: "k"}) == {"k": "I1 U3"}


# ═════════════════════════════════════════════════════════════════════════════════
# The delete guard
# ═════════════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize("together", [False, True], ids=["alone", "with a good file first"])
def test_a_truncated_snapshot_stops_the_run(project, together):
    """A full-extract file that lacks most keys looks like a failed extraction: the run stops
    and writes nothing, also when the file comes after a good one in the same run.  When the
    deletes are real, one run with the guard turned off writes them."""
    p = project("sagskategori")
    rows = p.base_rows()
    p.build()
    if together:
        p.write(2, [p.change(rows[0]), *rows[1:]])
    p.write(3, rows[:5])  # 26 of 31 keys missing
    before = p.snapshot()
    p.run_fails(f"{p.file_name(3)} would delete {len(rows) - 5} of {len(rows)} live keys")
    assert p.snapshot() == before
    assert p.sql("SELECT LKHS_filename FROM {lf}") == [(p.file_name(1),)]
    p.run(dbt_vars="{silver_delete_guard: false}")
    expected = {"D": len(rows) - 5, "U": 1} if together else {"D": len(rows) - 5}  # key 0 is back
    assert p.operations_on(3) == expected
    assert not _violations(p)


# ═════════════════════════════════════════════════════════════════════════════════
# Column types that Bronze infers differently later
# ═════════════════════════════════════════════════════════════════════════════════


def test_a_type_bronze_infers_differently_after_archiving_gives_no_false_updates(project):
    """read_json_auto infers types from the files in the folder.  A decimal value makes typeid
    DOUBLE in Silver; once that file is archived Bronze reads BIGINT.  Bronze is cast to Silver's
    type before hashing, so an unchanged key hashes 1.0 as before and gets no false U."""
    p = project("stemme")
    rows = p.base_rows()
    k, other = rows[0], rows[1]
    p.write(1, [{**k, "typeid": 1.5}, *rows[1:]])
    p.build()
    p.write(2, [k])  # typeid back to an integer: U2
    p.run()
    p.remove(1)  # archived: Bronze now reads typeid as BIGINT
    p.write(3, [k, p.change(other)])  # k unchanged, the other key changed
    p.run()
    bronze_type = p.sql(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = 'bronze_ddd_stemme' AND column_name = 'typeid'"
    )[0][0]
    silver_type = dict((c[0], c[1]) for c in p.sql("DESCRIBE {t}"))["typeid"]
    assert (bronze_type, silver_type) == ("BIGINT", "DOUBLE")
    assert p.histories({k["id"]: "k", other["id"]: "other"}) == {"k": "I1 U2", "other": "I1 U3"}
    assert not _violations(p)


def test_a_key_column_bronze_infers_differently_keeps_its_key(project):
    """A key of several columns: a decimal value makes aktoerid DOUBLE in Silver, so the key
    text has '206.0'.  After archiving Bronze reads aktoerid as BIGINT; the key is built from
    the columns cast to Silver's type, so an unchanged key keeps its key text and gets no row."""
    p = project("stemme", primary_key=["afstemningid", "aktoerid"])
    rows = p.base_rows()
    k, other = rows[0], rows[2]
    p.write(1, [k, {**rows[1], "aktoerid": 7.5}, *rows[2:]])
    p.build()
    stored = p.sql(f"SELECT {p.pk} FROM {{t}} WHERE id = ?", [k["id"]])[0][0]
    assert stored == f"{k['afstemningid']}]##[{k['aktoerid']}.0]##["
    p.remove(1)  # archived: Bronze now reads aktoerid as BIGINT
    p.write(2, [k, p.change(other)])  # k unchanged, the other key changed
    p.run()
    assert p.sql(f"SELECT DISTINCT {p.pk} FROM {{t}} WHERE id = ?", [k["id"]]) == [(stored,)]
    assert p.operations_on(2) == {"U": 1}
    assert not _violations(p)


def test_a_value_that_does_not_fit_silvers_type_stops_the_run(project):
    """A value the Silver column cannot hold is a real type change: the run stops and Silver's
    column must be altered by hand (runbook)."""
    p = project("stemme")
    rows = p.base_rows()
    p.build()
    p.write(2, [{**rows[0], "typeid": "not a number"}])
    before = p.snapshot()
    p.run_fails("not a number")
    assert p.snapshot() == before


# ═════════════════════════════════════════════════════════════════════════════════
# Pruning Bronze (§3.6)
# ═════════════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    ("entity", "storage_format"),
    [("sagskategori", "duckdb"), ("stemme", "duckdb"), ("stemme", "ducklake")],
)
def test_processed_files_can_be_archived(project, entity, storage_format):
    """Silver is the state, so processed Bronze files can go (the newest processed file stays,
    so that the Bronze views have a file to read).  Later files are compared with Silver, and
    a full refresh is ignored: nothing of the history is lost."""
    full = entity == "stemme"  # the snapshots of an incremental entity are full extractions
    p = project(entity, storage_format)
    rows = p.base_rows()
    x, gone, deleted = rows[0], rows[1], rows[2]
    others = rows[3:]
    names = {x[p.pk]: "x", gone[p.pk]: "gone", deleted[p.pk]: "deleted", others[0][p.pk]: "other"}
    p.build()
    p.write(2, [p.change(x), gone, *others], full=full)  # `deleted` missing: D2
    p.run()
    p.write(3, [p.change(p.change(x)), *others], full=full)  # `gone` also missing: D3
    p.run()
    history = {"x": "I1 U2 U3", "gone": "I1 D3", "deleted": "I1 D2", "other": "I1"}
    assert p.histories(names) == history
    p.remove(1, 2)  # archived; day 3 is the newest processed file
    p.run(full_refresh=True)
    assert p.histories(names) == history
    # Both deleted keys come back, one with its old content, one changed; x changes again.
    p.write(4, [p.change(p.change(p.change(x))), deleted, p.change(gone), *others], full=full)
    p.run()
    assert p.histories(names) == {
        "x": "I1 U2 U3 U4", "gone": "I1 D3 I4", "deleted": "I1 D2 I4", "other": "I1"
    }  # fmt: skip
    assert not _violations(p)


def test_a_silver_run_after_archiving_reads_the_current_bronze_columns(project):
    """dlt writes a column only into files where a row has a value, so archiving can take a
    column out of what the Bronze view reads.  information_schema still lists it until the view
    is created again; a Silver run without the Bronze views (as dbt_silver_job runs) must work
    all the same, because the Silver macros read the columns with DESCRIBE."""
    p = project("stemme")
    rows = p.base_rows()
    other = rows[1]
    p.write(1, [{**rows[0], "_test_marker": "x"}, *rows[1:]])  # the only file with the column
    p.build()
    p.write(2, [p.change(other)])
    p.run()
    p.remove(1)  # archived: no file in the folder has _test_marker any more
    p.write(3, [p.change(p.change(other))])
    p._must("run", "--select", p.model)  # Silver only, the Bronze view is not created again
    assert p.histories({other["id"]: "other"}) == {"other": "I1 U2 U3"}
    assert not _violations(p)


def test_a_delete_lines_up_with_a_new_source_column(project):
    """The deleted version is read from Silver, where a new source column sits at the end of
    the table.  The D row must still have the right value in every column."""
    p = project("stemme")
    rows = p.base_rows()
    k, others = rows[0], rows[1:]

    def mark(r: dict) -> dict:
        return {**r, "_test_marker": "x"}

    p.build()
    p.write(2, [mark(k)])  # incremental file: a new field, appended at the end of Silver
    p.run()
    p.write(3, [mark(r) for r in others], full=True)  # full file without k: D3
    p.run()
    marker, typeid, day = p.sql(
        f"SELECT _test_marker, typeid, day(LKHS_date_valid_from) FROM {{t}} "
        f"WHERE {p.pk} = ? AND LKHS_cdc_operation = 'D'",
        [k[p.pk]],
    )[0]
    assert (marker, typeid, day) == ("x", k["typeid"], 3)
    assert not _violations(p)


# ═════════════════════════════════════════════════════════════════════════════════
# Changes to the source columns (§3.9)
# ═════════════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    ("entity", "storage_format"),
    [("stemme", "duckdb"), ("stemme", "ducklake"), ("sagskategori", "duckdb")],
)
def test_new_source_column_then_full_refresh(project, entity, storage_format):
    """A new field gives a U only for keys that have a value in it: Silver's latest version is
    hashed again with the current columns, the new one counting as NULL.  A normal run adds the
    column at the end of the Silver table; a full refresh is ignored."""
    p = project(entity, storage_format)
    pk = p.pk
    rows = p.base_rows()
    marked = {r[pk] for r in rows[:5]}
    missing = rows[7][pk]

    def mark(r: dict) -> dict:
        return {**r, "_test_marker": "x"} if r[pk] in marked else r

    p.build()
    first_hash = p.versions(rows[6][pk])[0]["hash"]
    p.write(2, [mark(r) for r in rows])
    p.run()
    assert p.sql("DESCRIBE {t}")[-1][0] == "_test_marker"
    names = {rows[0][pk]: "marked", rows[6][pk]: "unmarked", missing: "missing"}
    assert p.histories(names) == {"marked": "I1 U2", "unmarked": "I1", "missing": "I1"}
    p.write(3, [mark(r) for r in rows if r[pk] != missing], full=True)  # complete, one key missing
    p.run()
    expected = {"marked": "I1 U2", "unmarked": "I1", "missing": "I1 D3"}
    assert p.histories(names) == expected
    before = p.snapshot()
    p.run(full_refresh=True)
    assert p.snapshot() == before
    assert p.sql("DESCRIBE {t}")[-1][0] == "_test_marker"
    assert p.versions(rows[6][pk])[0]["hash"] == first_hash
    assert not _violations(p)
    assert p.data_tests_pass()


@pytest.mark.parametrize("entity", ["sagskategori", "stemme"])
def test_dropped_source_field(project, entity):
    """A field the source stops sending stays in Bronze as NULL, so every key that had a
    value gets a U; a full refresh afterwards is ignored."""
    p = project(entity)
    rows = p.base_rows()
    field = {"sagskategori": "kategori", "stemme": "typeid"}[entity]
    p.build()
    p.write(2, [{k: v for k, v in r.items() if k != field} for r in rows])
    p.run()
    assert p.operations_on(2) == {"U": len(rows)}
    before = p.snapshot()
    p.run(full_refresh=True)
    assert p.snapshot() == before
    assert not _violations(p)


# ═════════════════════════════════════════════════════════════════════════════════
# The same key twice in one file (§3.10)
# ═════════════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    ("entity", "both_changed", "duplicates"),
    [("stemme", True, 1), ("sagskategori", False, 0), ("sagskategori", True, 1)],
)
def test_same_key_twice_in_one_file(project, entity, both_changed, duplicates):
    """Not handled, only detected: the result can contain two rows for the same
    (key, LKHS_date_valid_from), which the grain tests report.  Two changed copies always
    give a duplicate; with one unchanged copy it depends on the order of the rows for the
    incremental macro, so that case is only tested for the full-extract macro, where the
    unchanged copy matches the previous file and gives no row."""
    p = project(entity)
    rows = p.base_rows()
    first = p.change(rows[0]) if both_changed else rows[0]
    p.build()
    p.write(2, [first, p.change(p.change(rows[0])), *rows[1:]])
    p.run()
    assert p.violations()["duplicate (key, valid_from)"] == duplicates
    if entity == "stemme":
        assert not p.data_tests_pass()  # silver_ddd_stemme has a generic grain test
