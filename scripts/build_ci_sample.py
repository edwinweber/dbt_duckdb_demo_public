#!/usr/bin/env python3
"""Cut a small, referentially coherent Bronze sample from a full extraction.

The sample is committed under ``tests/fixtures/bronze_sample`` and lets CI run a
real ``dbt build`` (Bronze -> Silver -> Gold, including every dbt test) without
network access.  Run it again only when the sample needs refreshing::

    python scripts/build_ci_sample.py /data/local/Files/Bronze tests/fixtures/bronze_sample

Selection: a few dozen votes (afstemning) are chosen, then every row they
reference is pulled in (meetings, case steps, cases, actors, individual votes);
lookup tables are copied in full.  Rows are copied verbatim, one NDJSON file per
entity named ``<entity>_<YYYYMMDD_HHMMSS>.json`` like the dlt extraction writes.
"""

from __future__ import annotations

import json
import shutil
import sys
from collections.abc import Iterator
from pathlib import Path

TIMESTAMP = "20260101_000000"
VOTES_TO_KEEP = 25
SAGSTRINAKTOER_TO_KEEP = 400

DDD_LOOKUPS = [
    "afstemningstype",
    "aktoertype",
    "moedestatus",
    "moedetype",
    "periode",
    "sagskategori",
    "sagsstatus",
    "sagstrinsstatus",
    "sagstrinstype",
    "sagstype",
    "stemmetype",
]
RFAM_LIMITS = {
    "family": 40,
    "genome": 40,
    "clan": 20,
    "clan_membership": 60,
    "author": 20,
    "literature_reference": 20,
    "dead_family": 20,
}


def _rows(entity_dir: Path) -> Iterator[dict]:
    """Yield every row of an entity, oldest file first (later rows win on dedupe)."""
    for path in sorted(entity_dir.glob("*.json")):
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield json.loads(line)


def _latest_by(entity_dir: Path, key: str) -> dict:
    return {row[key]: row for row in _rows(entity_dir)}


def _cover_columns(selected: list[dict], universe: dict) -> list[dict]:
    """Add rows until every column seen anywhere in *universe* appears in *selected*.

    The API omits empty fields, so a small sample can lack a column that Gold
    models select; Silver would then have no such column and the build fails.
    """
    result = list(selected)
    present = {key for row in result for key in row}
    for row in universe.values():
        if set(row) - present:
            result.append(row)
            present |= set(row)
    return result


def _write(out_dir: Path, entity: str, rows: list[dict]) -> None:
    target = out_dir / entity
    target.mkdir(parents=True, exist_ok=True)
    with (target / f"{entity}_{TIMESTAMP}.json").open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    print(f"  {entity}: {len(rows)} rows")


def build_ddd(source: Path, out: Path) -> None:
    afstemning = _latest_by(source / "afstemning", "id")
    chosen = sorted(
        (v for v in afstemning.values() if v.get("sagstrinid") and v.get("moedeid")),
        key=lambda v: v["id"],
    )[-VOTES_TO_KEEP:]
    vote_ids = {v["id"] for v in chosen}
    moede_ids = {v["moedeid"] for v in chosen}
    sagstrin_ids = {v["sagstrinid"] for v in chosen}

    stemmer = [r for r in _rows(source / "stemme") if r["afstemningid"] in vote_ids]
    stemmer = list({r["id"]: r for r in stemmer}.values())

    sagstrin_all = _latest_by(source / "sagstrin", "id")
    sagstrin = [sagstrin_all[i] for i in sorted(sagstrin_ids) if i in sagstrin_all]
    sag_ids = {s["sagid"] for s in sagstrin}
    sag_all = _latest_by(source / "sag", "id")
    sag = [sag_all[i] for i in sorted(sag_ids) if i in sag_all]
    sta_all = _latest_by(source / "sagstrinaktoer", "id")
    sagstrinaktoer = [r for r in sta_all.values() if r["sagstrinid"] in sagstrin_ids]
    sagstrinaktoer = sagstrinaktoer[:SAGSTRINAKTOER_TO_KEEP]

    actor_ids = {r["aktoerid"] for r in stemmer} | {r["aktoerid"] for r in sagstrinaktoer}
    aktoer_all = _latest_by(source / "aktoer", "id")
    aktoer = _cover_columns(
        [aktoer_all[i] for i in sorted(actor_ids) if i in aktoer_all], aktoer_all
    )

    moede_all = _latest_by(source / "moede", "id")
    moede = _cover_columns([moede_all[i] for i in sorted(moede_ids) if i in moede_all], moede_all)

    sag = _cover_columns(sag, sag_all)
    sagstrin = _cover_columns(sagstrin, sagstrin_all)

    selected = {
        "afstemning": chosen,
        "stemme": stemmer,
        "sagstrin": sagstrin,
        "sag": sag,
        "sagstrinaktoer": sagstrinaktoer,
        "aktoer": aktoer,
        "moede": moede,
    }
    for entity, rows in selected.items():
        _write(out, entity, rows)
    for entity in DDD_LOOKUPS:
        _write(out, entity, list(_latest_by(source / entity, "id").values()))


def build_rfam(source: Path, out: Path) -> None:
    keys = {
        "family": "rfam_acc",
        "genome": "upid",
        "clan": "clan_acc",
        "author": "author_id",
        "literature_reference": "pmid",
        "dead_family": "rfam_acc",
    }
    for entity, limit in RFAM_LIMITS.items():
        if entity in keys:
            rows = list(_latest_by(source / entity, keys[entity]).values())
        else:  # clan_membership has no primary key
            rows = list(_rows(source / entity))
        _write(out, entity, rows[:limit])


def main() -> None:
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    source, out = Path(sys.argv[1]), Path(sys.argv[2])
    if out.exists():
        shutil.rmtree(out)
    print("DDD")
    build_ddd(source / "DDD", out / "DDD")
    print("RFAM")
    build_rfam(source / "RFAM", out / "RFAM")


if __name__ == "__main__":
    main()
