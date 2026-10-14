"""Real dbt runs of the Silver layer.

Runs dbt-oss on copies of the CI sample (``tests/fixtures/bronze_sample``) in pytest's
tmp directories and checks the Silver tables that both Silver macros produce:
``generate_model_silver_full_extraction`` and ``generate_model_silver_incr_extraction``
with their pre- and post-hooks.  Every case in the table of
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

import pytest

import duckdb

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


class SilverProject:
    """dbt on an isolated copy of the sample Bronze files, for one Silver model."""

    def __init__(self, root: Path, storage_format: str, entity: str) -> None:
        self.root = root
        self.format = storage_format
        self.entity = entity
        e = ENTITIES[entity]
        self.pk: str = e["pk"]
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
    def file_name(self, day: int, suffix: str = ".json") -> str:
        return f"{self.entity}_202601{day:02d}_000000{suffix}"

    def base_rows(self) -> list[dict]:
        text = (self.entity_dir / self.file_name(1)).read_text()
        return [json.loads(line) for line in text.splitlines() if line.strip()]

    def write(self, day: int, rows: list[dict], suffix: str = ".json") -> None:
        text = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows)
        (self.entity_dir / self.file_name(day, suffix)).write_text(text)

    def remove(self, *days: int) -> None:
        for day in days:
            (self.entity_dir / self.file_name(day)).unlink()

    # -- dbt -----------------------------------------------------------------------
    def dbt(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["dbt", *args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR),
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

    def run(self, full_refresh: bool = False) -> None:
        """Bronze views + Silver, as the pipeline runs them."""
        args = ["run", "--select", f"+{self.model}"]
        if full_refresh:
            args.append("--full-refresh")
        self._must(*args)

    def data_tests_pass(self) -> bool:
        """The model's generic data tests (the singular grain test needs all DDD tables)."""
        result = self.dbt(
            "test", "--select", self.model, f"{self.model}_cv", "--exclude", "test_type:singular"
        )
        return result.returncode == 0

    # -- reading the database ------------------------------------------------------
    def sql(self, query: str, params: list | None = None) -> list[tuple]:
        """Run a query; {t}, {cv} and {lf} are the Silver table, its _cv view and its
        _last_file table, {bronze} is the Bronze view."""
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
            "LKHS_date_inserted, LKHS_date_inserted_src, LKHS_source_system_code "
            f"FROM {{t}} WHERE {self.pk} = ? ORDER BY LKHS_date_valid_from",
            [key],
        )
        names = ["op", "day", "filename", "hash", "inserted", "inserted_src", "code"]
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
    """Factory: project("stemme") or project("sagskategori", "ducklake")."""
    counter = iter(range(100))

    def make(entity: str, storage_format: str = "duckdb") -> SilverProject:
        root = tmp_path / f"{entity}_{storage_format}_{next(counter)}"
        root.mkdir()
        return SilverProject(root, storage_format, entity)

    return make


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


def test_file_name_variants_and_bookmark(project):
    """Files named .json, .jsonl and .json.jsonl (dlt versions) are all read; valid_from
    comes from the timestamp in the name; _last_file holds the newest processed file and
    its predecessor."""
    p = project("sagskategori")
    rows = p.base_rows()
    p.build()
    assert p.sql("SELECT LKHS_filename, LKHS_filename_previous FROM {lf}") == [
        (p.file_name(1), None)
    ]
    p.write(2, [p.change(rows[0]), *rows[1:]], suffix=".jsonl")
    p.run()
    p.write(3, [p.change(rows[0]), p.change(rows[1]), *rows[2:]], suffix=".json.jsonl")
    p.run()
    assert p.histories({rows[0]["id"]: "a", rows[1]["id"]: "b"}) == {"a": "I1 U2", "b": "I1 U3"}
    assert [v["filename"] for v in p.versions(rows[1]["id"])] == [
        p.file_name(1),
        p.file_name(3, ".json.jsonl"),
    ]
    assert p.sql("SELECT LKHS_filename, LKHS_filename_previous FROM {lf}") == [
        (p.file_name(3, ".json.jsonl"), p.file_name(2, ".jsonl"))
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


def test_date_inserted_src_with_a_date_column(project):
    """With a date column, LKHS_date_inserted_src is the earliest value of that column for
    the key over all Bronze files."""
    p = project("sagskategori")
    rows = p.base_rows()
    p.build()
    original_text = rows[0]["opdateringsdato"]
    later = {**p.change(rows[0]), "opdateringsdato": "2099" + original_text[4:]}  # same format
    p.write(2, [later, *rows[1:]])
    p.run()
    versions = p.versions(rows[0]["id"])
    original = p.sql(
        "SELECT CAST(opdateringsdato AS DATETIME) FROM {bronze} WHERE id = ? AND LKHS_filename = ?",
        [rows[0]["id"], p.file_name(1)],
    )[0][0]
    assert [v["op"] for v in versions] == ["I", "U"]
    assert {v["inserted_src"] for v in versions} == {original}


def test_date_inserted_src_without_a_date_column(project):
    """Without a date column (here Rfam author), LKHS_date_inserted_src is the timestamp of
    the first file that contains the key."""
    p = project("author")
    rows = p.base_rows()
    late_key = rows[-1]
    p.write(1, rows[:-1])
    p.build()
    p.write(2, rows)
    p.run()
    assert p.versions(rows[0]["author_id"])[0]["inserted_src"].day == 1
    assert [(v["op"], v["inserted_src"].day) for v in p.versions(late_key["author_id"])] == [
        ("I", 2)
    ]


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


def _full_extract_files(p: SilverProject) -> tuple[dict[int, list[dict]], dict[Any, str]]:
    rows = p.base_rows()
    keys = [r[p.pk] for r in rows[:8]]
    names = dict(zip(keys, "ABCDEGJN", strict=True))
    a, b, c, d, e, g, j, n = keys
    state = {r[p.pk]: r for r in rows}
    order = [r[p.pk] for r in rows]

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


@pytest.fixture(scope="module", params=["duckdb", "ducklake"])
def full_lifecycle(request, tmp_path_factory) -> dict[str, dict[str, Any]]:
    results = {}
    for path in FULL_PATHS:
        root = tmp_path_factory.mktemp(f"full_{request.param}_{path.replace(' ', '_')}")
        p = SilverProject(root, request.param, "sagskategori")
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
        record["cv"] = dict(p.sql("SELECT id, LKHS_cdc_operation FROM {cv}"))
        record["names"] = names
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
    d_row = full_lifecycle["one run per file"]["d_row_of_D"]
    assert d_row["op"] == "D" and d_row["day"] == 2
    assert d_row["filename"] == "sagskategori_20260102_000000.json"


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
# Day 1 complete; day 2 complete + full refresh; day 3 incremental file + normal run;
# day 4 complete + full refresh; day 5 complete + normal run.
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

INCR_PATHS = ["one run per file", "days 3+4 in one full refresh", "days 3+4 in one normal run"]

INCR_COMBINATIONS = [("duckdb", "stemme"), ("ducklake", "stemme"), ("duckdb", "family")]


def _incremental_files(p: SilverProject) -> tuple[dict[int, list[dict]], dict[Any, str]]:
    rows = p.base_rows()
    keys = [r[p.pk] for r in rows[:8]]
    names = dict(zip(keys, "ABCDEGHJ", strict=True))
    a, b, c, d, e, g, _h, j = keys
    state = {r[p.pk]: r for r in rows}
    order = [r[p.pk] for r in rows]

    def complete(missing: set) -> list[dict]:
        return [state[k] for k in order if k not in missing]

    files = {2: complete({a, b, c, d, g, j})}
    state[b], state[e] = p.change(state[b]), p.change(state[e])
    files[3] = [state[k] for k in (a, b, c, e, g, j)]
    files[4] = complete({c, d, j})
    state[g] = p.change(state[g])
    files[5] = complete({c, d})
    return files, names


def _run_incremental_path(p: SilverProject, path: str) -> dict[str, Any]:
    files, names = _incremental_files(p)
    record: dict[str, Any] = {"names": names}

    def observe(label: str) -> None:
        record[f"histories {label}"] = p.histories(names)
        record[f"violations {label}"] = _violations(p)

    p.build()
    p.write(2, files[2])
    p.run(full_refresh=True)
    observe("day 2")
    record["first D inserted"] = {
        k: p.versions(k)[1]["inserted"] for k, v in names.items() if v in ("D", "J")
    }
    if path == "one run per file":
        p.write(3, files[3])
        p.run()
        observe("day 3")
        before = p.snapshot()
        p.run()
        record["repeat run unchanged"] = p.snapshot() == before
        p.write(4, files[4])
        p.run(full_refresh=True)
    elif path == "days 3+4 in one full refresh":
        p.write(3, files[3])
        p.write(4, files[4])
        p.run(full_refresh=True)
    else:
        p.write(3, files[3])
        p.write(4, files[4])
        p.run()
        observe("day 3")
        p.run(full_refresh=True)
    observe("day 4")
    p.write(5, files[5])
    p.run()
    observe("day 5")
    before = p.snapshot()
    p.run(full_refresh=True)
    record["full refresh unchanged"] = p.snapshot() == before
    observe("day 5 after full refresh")
    record["carried D inserted"] = {
        k: p.versions(k)[1]["inserted"] for k, v in names.items() if v in ("D", "J")
    }
    record["data tests pass"] = p.data_tests_pass()
    record["snapshot"] = p.snapshot()
    return record


@pytest.fixture(scope="module", params=INCR_COMBINATIONS, ids=lambda c: f"{c[0]}-{c[1]}")
def incremental_lifecycle(request, tmp_path_factory) -> dict[str, dict[str, Any]]:
    storage_format, entity = request.param
    results = {}
    for path in INCR_PATHS:
        root = tmp_path_factory.mktemp(f"incr_{storage_format}_{entity}_{path.replace(' ', '_')}")
        results[path] = _run_incremental_path(SilverProject(root, storage_format, entity), path)
    return results


@pytest.mark.parametrize("path", INCR_PATHS)
def test_incremental_histories(incremental_lifecycle, path):
    record = incremental_lifecycle[path]
    for label, histories in record.items():
        if label.startswith("histories "):
            day = label.removeprefix("histories ").removesuffix(" after full refresh")
            assert histories == INCR_EXPECTED[day], f"{path}, {label}"


@pytest.mark.parametrize("path", INCR_PATHS)
def test_incremental_invariants(incremental_lifecycle, path):
    for label, violations in incremental_lifecycle[path].items():
        if label.startswith("violations "):
            assert not violations, f"{path}, {label}: {violations}"


def test_incremental_carried_deletes_keep_their_date_inserted(incremental_lifecycle):
    """A full refresh rewrites LKHS_date_inserted of every rebuilt row, but a carried
    D row keeps the value of the run that wrote it."""
    record = incremental_lifecycle["one run per file"]
    assert record["carried D inserted"] == record["first D inserted"]


def test_incremental_repeat_run_and_full_refresh_change_nothing(incremental_lifecycle):
    assert incremental_lifecycle["one run per file"]["repeat run unchanged"]
    for path in INCR_PATHS:
        assert incremental_lifecycle[path]["full refresh unchanged"], path


def test_incremental_paths_give_identical_tables(incremental_lifecycle):
    reference = incremental_lifecycle[INCR_PATHS[0]]["snapshot"]
    for path in INCR_PATHS[1:]:
        assert incremental_lifecycle[path]["snapshot"] == reference, path


@pytest.mark.parametrize("path", INCR_PATHS)
def test_incremental_data_tests_pass(incremental_lifecycle, path):
    assert incremental_lifecycle[path]["data tests pass"]


# ═════════════════════════════════════════════════════════════════════════════════
# Incremental entities: full refresh details (§3.4)
# ═════════════════════════════════════════════════════════════════════════════════


def test_full_refresh_of_a_new_table_writes_no_deletes(project):
    p = project("stemme")
    rows = p.base_rows()
    p.write(2, rows[1:])  # key 0 missing from the newest file
    p.build(full_refresh=True)
    assert p.histories({rows[0]["id"]: "k"}) == {"k": "I1"}


def test_new_delete_has_the_newest_file_and_the_deleted_content(project):
    p = project("stemme")
    rows = p.base_rows()
    k = rows[0]["id"]
    p.build()
    p.write(2, [p.change(rows[0])])  # incremental file: k changed
    p.run()
    p.write(3, rows[1:])  # complete file without k
    p.run(full_refresh=True)
    u, d = p.versions(k)[1:]
    assert (u["op"], d["op"], d["day"]) == ("U", "D", 3)
    assert d["filename"] == p.file_name(3)
    assert d["hash"] == u["hash"]


def test_false_deletes_and_their_repair(project):
    """Runbook §7: a full refresh on an incremental newest file deletes every key that is
    not in it.  A full extraction plus a normal run brings them back as I; deleting the
    false D rows and running a full refresh removes the pair from the history."""
    p = project("stemme")
    rows = p.base_rows()
    kept, gone = rows[0]["id"], rows[100]["id"]
    names = {kept: "kept", gone: "gone"}
    p.build()
    p.write(2, rows[:5])  # incremental file with five keys
    p.run(full_refresh=True)
    assert p.histories(names) == {"kept": "I1", "gone": "I1 D2"}
    assert p.sql("SELECT COUNT(*) FROM {cv} WHERE LKHS_cdc_operation = 'D'")[0][0] == len(rows) - 5
    p.write(3, rows)  # full extraction
    p.run()
    assert p.histories(names) == {"kept": "I1", "gone": "I1 D2 I3"}
    assert p.sql("SELECT COUNT(*) FROM {cv} WHERE LKHS_cdc_operation = 'D'")[0][0] == 0
    false_run = p.sql("SELECT DISTINCT LKHS_date_inserted FROM {t} WHERE LKHS_cdc_operation = 'D'")
    assert len(false_run) == 1
    con = duckdb.connect(str(p.root / "test.duckdb"))
    con.execute(
        f"DELETE FROM test.main_silver.{p.model} WHERE LKHS_cdc_operation = 'D' AND LKHS_date_inserted = ?",
        [false_run[0][0]],
    )
    con.close()
    p.run(full_refresh=True)
    assert p.histories(names) == {"kept": "I1", "gone": "I1"}
    assert p.operations_on(3) == {}
    assert not _violations(p)


# ═════════════════════════════════════════════════════════════════════════════════
# Files that arrive late (§3.8)
# ═════════════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize("entity", ["sagskategori", "stemme"])
def test_late_file_newer_than_the_bookmarks_predecessor(project, entity):
    p = project(entity)
    rows = p.base_rows()
    k = rows[0][p.pk]
    p.build()
    p.write(3, rows)
    p.run()  # _last_file: day 3, predecessor day 1
    p.write(2, [p.change(rows[0]), *rows[1:]])  # day 2 arrives late
    p.run()
    assert p.histories({k: "k"}) == {"k": "I1 U2 U3"}
    assert not _violations(p)


@pytest.mark.parametrize("entity", ["sagskategori", "stemme"])
def test_late_file_older_than_the_bookmarks_predecessor(project, entity):
    p = project(entity)
    rows = p.base_rows()
    k = rows[0][p.pk]
    p.build()
    p.write(3, rows)
    p.run()
    p.write(5, rows)
    p.run()  # _last_file: day 5, predecessor day 3
    p.write(2, [p.change(rows[0]), *rows[1:]])  # day 2 arrives very late
    p.run()
    assert p.histories({k: "k"}) == {"k": "I1 U3"}  # day 2 skipped, day 3 compared with it
    assert p.violations()["U row with the hash of the version before it"] == 1
    p.run(full_refresh=True)  # day 5 is complete, so this is allowed for incremental too
    assert p.histories({k: "k"}) == {"k": "I1 U2 U3"}
    assert not _violations(p)


# ═════════════════════════════════════════════════════════════════════════════════
# Pruning Bronze (§3.6)
# ═════════════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(("prune", "expected"), [((1,), "I1 U5"), ((1, 2), "I1 I3 U5")])
def test_full_extract_pruning(project, prune, expected):
    """Keeping the newest processed file, LKHS_filename_previous and the file before it
    is enough; deleting that file too gives every key an extra I."""
    p = project("sagskategori")
    rows = p.base_rows()
    p.build()
    for day in (2, 3, 4):
        p.write(day, rows)
        p.run()  # _last_file: day 4, predecessor day 3
    p.remove(*prune)
    p.write(5, [p.change(rows[0]), *rows[1:]])
    p.run()
    assert p.histories({rows[0]["id"]: "k"}) == {"k": expected}


@pytest.mark.parametrize(
    ("days_after_full_extraction", "expected_k", "extra_i_on_day_3"),
    [((4, 5), "I1 U2 U6", False), ((4,), "I1 U2 I3 U6", True)],
    ids=["full extraction older than predecessor", "full extraction is the predecessor"],
)
def test_incremental_pruning_down_to_a_full_extraction(
    project, days_after_full_extraction, expected_k, extra_i_on_day_3
):
    """Remaining files start with the full extraction of day 3.  Safe when it is older
    than LKHS_filename_previous; when it is the predecessor, every key gets an extra I."""
    p = project("stemme")
    rows = p.base_rows()
    k, w = rows[0], rows[2]
    state = {r["id"]: r for r in rows}
    p.build()
    state[k["id"]] = p.change(k)
    p.write(2, [state[k["id"]]])
    p.run()
    p.write(3, list(state.values()))  # full extraction
    p.run()
    for day in days_after_full_extraction:
        p.write(day, [state[rows[1]["id"]]])  # incremental files, nothing changed
        p.run()
    p.remove(1, 2)
    state[k["id"]], state[w["id"]] = p.change(state[k["id"]]), p.change(w)
    p.write(6, [state[k["id"]], state[w["id"]]])
    p.run()
    histories = p.histories({k["id"]: "k", w["id"]: "w"})
    assert histories["k"] == expected_k
    assert histories["w"] == ("I1 U6" if not extra_i_on_day_3 else "I1 I3 U6")
    assert (p.operations_on(3).get("I", 0) == len(rows)) == extra_i_on_day_3


def test_incremental_pruning_without_a_full_extraction(project):
    """The oldest remaining file is incremental: a key whose previous version was pruned
    gets I instead of U, and a later full extraction gives unchanged keys a second I."""
    p = project("stemme")
    rows = p.base_rows()
    x, z, w = rows[0], rows[1], rows[2]
    p.build()
    p.write(2, [p.change(x)])
    p.run()
    p.write(3, [p.change(p.change(x))])
    p.run()  # _last_file: day 3, predecessor day 2
    p.remove(1)
    p.write(4, [p.change(z)])
    p.run()
    assert p.histories({z["id"]: "z"}) == {"z": "I1 I4"}
    p.write(5, [p.change(p.change(x)), p.change(z), *rows[2:]])  # full extraction
    p.run()
    assert p.histories({w["id"]: "w"}) == {"w": "I1 I5"}
    assert p.operations_on(5) == {"I": len(rows) - 2}


@pytest.mark.parametrize("entity", ["sagskategori", "stemme"])
def test_full_refresh_after_pruning_keeps_only_the_remaining_history(project, entity):
    """A full refresh rebuilds from the remaining files: history before them is lost,
    a key that only exists in pruned files keeps only a D (incremental) or nothing
    (full-extract), and incremental D rows are carried forward."""
    p = project(entity)
    rows = p.base_rows()
    x, gone, deleted = rows[0], rows[1], rows[2]
    others = rows[3:]
    names = {x[p.pk]: "x", gone[p.pk]: "gone", deleted[p.pk]: "deleted", others[0][p.pk]: "other"}
    p.build()
    p.write(2, [p.change(x), gone, *others])  # complete, `deleted` missing
    p.run(full_refresh=True)
    p.write(3, [p.change(p.change(x)), *others])  # complete, `gone` also missing
    p.write(4, [p.change(p.change(x)), *others])
    p.run()
    p.remove(1, 2)
    p.run(full_refresh=True)
    histories = p.histories(names)
    assert histories["x"] == "I3" and histories["other"] == "I3"
    if entity == "stemme":
        # Incremental: the existing D rows are carried forward; `gone` (only in pruned
        # files) is missing from the newest file, so it keeps only a new D.
        assert histories["deleted"] == "D2"
        assert histories["gone"] == "D4"
    else:
        # Full-extract: deletes are derived from the remaining file pairs only.
        assert histories["deleted"] == ""
        assert histories["gone"] == ""


# ═════════════════════════════════════════════════════════════════════════════════
# Changes to the source columns (§3.9)
# ═════════════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize(
    ("entity", "storage_format"),
    [("stemme", "duckdb"), ("stemme", "ducklake"), ("sagskategori", "duckdb")],
)
def test_new_source_column_then_full_refresh(project, entity, storage_format):
    """A new field gives a U for keys that have a value in it.  A normal run adds the
    column at the end of the Silver table; the following full refresh still writes
    correctly aligned rows and recomputes the hashes of all versions."""
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
    p.write(3, [mark(r) for r in rows if r[pk] != missing])  # complete, one key missing
    p.run(full_refresh=True)
    assert p.histories(names) == {"marked": "I1 U2", "unmarked": "I1", "missing": "I1 D3"}
    columns = [c[0] for c in p.sql("DESCRIBE {t}")]
    assert columns.index("_test_marker") < columns.index("LKHS_filename")  # Bronze order again
    assert not _violations(p)
    assert p.versions(rows[6][pk])[0]["hash"] != first_hash  # hashes recomputed
    before = p.snapshot()
    p.run(full_refresh=True)
    assert p.snapshot() == before
    assert p.data_tests_pass()


@pytest.mark.parametrize("entity", ["sagskategori", "stemme"])
def test_dropped_source_field(project, entity):
    """A field the source stops sending stays in Bronze as NULL, so every key that had a
    value gets a U; a full refresh afterwards keeps that history."""
    p = project(entity)
    rows = p.base_rows()
    field = {"sagskategori": "kategori", "stemme": "typeid"}[entity]
    p.build()
    p.write(2, [{k: v for k, v in r.items() if k != field} for r in rows])
    p.run()
    assert p.operations_on(2) == {"U": len(rows)}
    before = p.snapshot()
    p.run(full_refresh=True)  # day 2 is complete, so this is allowed for both
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
