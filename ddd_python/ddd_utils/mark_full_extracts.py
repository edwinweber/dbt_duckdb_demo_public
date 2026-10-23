"""Add the ``_full`` marker to the full extractions already in Bronze.

Silver derives the deletes of incremental entities from Bronze files named
``<entity>_<YYYYMMDD_HHMMSS>_full.json`` (``dbt_transformations.md`` §3.4).  The extraction
adds the marker itself since that naming was introduced; files written before are plain.
This script finds the full extractions among them and renames them.  It changes nothing
but file names.

Only the 6 DDD and 2 Rfam incremental entities are looked at.  Every file of a
full-extract entity is complete by definition and never gets the marker.

How a full extraction is recognised, per file
---------------------------------------------
1. **The extraction run log.**  Every extraction wrote a record with the file name and
   the start date it ran from.  A file whose record says ``1900-01-01`` or earlier
   (``FULL_EXTRACT_DATE``) is full; a file whose record says a later date is not,
   whatever its size.
2. **Size, when the log has no record of the file** (rotated or lost logs).  A file is
   taken as full when it is at least ``--ratio`` times the median size of the entity's
   files (default 3) *and* at least ``--min-share`` of the entity's largest file (default
   0.9).  The second test rejects a large partial extraction next to a much larger full
   one: in production a month of daily incremental files after a source-wide update was
   half the size of the full extraction and 180 times the median.  It needs at least 3
   files.

The evidence is printed for every file that is renamed or that nearly was, so the plan can
be checked before ``--apply``.  Size is a heuristic: review the plan.  An entity without any
full extraction (for example Rfam tables that were always extracted with the one-year
window) gets no marker and detects no deletes until a real full extraction is run
(``--date_to_load_from 1900-01-01``).

The default is a **dry run**.  Stop the extraction and dbt jobs before ``--apply``; the
rename is not atomic across files.  Rename before the first Silver run of the version that
logs Bronze files in ``<model>_files``: that run records the names it finds, and a file
renamed afterwards looks like a late file.  Rows already in Silver keep the old
``LKHS_filename``.

Usage
-----
    python -m ddd_python.ddd_utils.mark_full_extracts                   # plan only
    python -m ddd_python.ddd_utils.mark_full_extracts --apply
    python -m ddd_python.ddd_utils.mark_full_extracts --source DDD --entity stemme
    python -m ddd_python.ddd_utils.mark_full_extracts --log-dir /data/local/logs --ratio 4

Local files only (no ``s3://``).
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import statistics
import sys
from dataclasses import dataclass
from pathlib import Path

from ddd_python.ddd_utils import get_variables_from_env
from ddd_python.ddd_utils.archive_bronze import Entity, bronze_files, is_full_file, list_entities
from ddd_python.ddd_utils.string_utils import FULL_EXTRACT_DATE

logger = logging.getLogger(__name__)

DEFAULT_RATIO = 3.0
DEFAULT_MIN_SHARE = 0.9
MIN_FILES_FOR_SIZE = 3
_FILE_NAME = re.compile(r"^(?P<stem>.+_\d{8}_\d{6})(?P<ext>\..+)$")


@dataclass
class Decision:
    file: Path
    full: bool
    evidence: str

    @property
    def new_name(self) -> str:
        return full_name(self.file.name)


def full_name(name: str) -> str:
    """``aktoer_20260516_214617.json`` -> ``aktoer_20260516_214617_full.json``."""
    match = _FILE_NAME.match(name)
    if not match:
        raise ValueError(f"Not a Bronze file name: {name!r}")
    return f"{match['stem']}_full{match['ext']}"


def read_run_log(log_dir: Path, entity: Entity) -> dict[str, str]:
    """File name -> start date of the successful extraction that wrote it, from the run log."""
    path = log_dir / entity.source / entity.name / f"{entity.name}_log.ndjson"
    dates: dict[str, str] = {}
    if not path.exists():
        return dates
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if "success" not in str(record.get("message", "")):
            continue
        parameters = record.get("parameters") or {}
        name = parameters.get("destination_file_name")
        date = parameters.get("source_api_date_to_load_from") or (
            parameters.get("sql_params") or {}
        ).get("updated_from")
        if name and date:
            dates[name] = str(date)
    return dates


def decide(
    files: list[Path],
    logged: dict[str, str],
    ratio: float = DEFAULT_RATIO,
    min_share: float = DEFAULT_MIN_SHARE,
    use_logs: bool = True,
    use_size: bool = True,
) -> list[Decision]:
    """One decision per file of an entity that is not marked yet."""
    sizes = {f: f.stat().st_size for f in files}
    median = statistics.median(sizes.values()) if sizes else 0
    largest = max(sizes.values(), default=0)
    decisions = []
    for file in files:
        if is_full_file(file.name):
            continue
        date = logged.get(file.name) if use_logs else None
        if date is not None:
            full = date <= FULL_EXTRACT_DATE
            decisions.append(Decision(file, full, f"log: extraction from {date}"))
        elif use_size and len(files) >= MIN_FILES_FOR_SIZE and median > 0:
            size = sizes[file]
            full = size >= ratio * median and size >= min_share * largest
            share = f"{size / median:.1f}x the median, {100 * size / largest:.0f}% of the largest"
            decisions.append(Decision(file, full, f"size (no log record): {share}"))
        else:
            decisions.append(Decision(file, False, "no evidence"))
    return decisions


def apply_decisions(decisions: list[Decision]) -> int:
    """Rename the files decided as full; returns how many were renamed."""
    renamed = 0
    for decision in decisions:
        if not decision.full:
            continue
        target = decision.file.with_name(decision.new_name)
        if target.exists():
            raise FileExistsError(f"{target} already exists; not overwriting")
        decision.file.rename(target)
        renamed += 1
    return renamed


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="mark_full_extracts",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source", choices=["DDD", "RFAM", "all"], default="all")
    parser.add_argument("--entity", action="append", help="Only this normalised entity.")
    parser.add_argument("--ddd-dir", type=Path, help="Bronze DDD directory (default from env).")
    parser.add_argument("--rfam-dir", type=Path, help="Bronze RFAM directory (default from env).")
    parser.add_argument("--log-dir", type=Path, help="Run log directory (default from env).")
    parser.add_argument("--ratio", type=float, default=DEFAULT_RATIO)
    parser.add_argument("--min-share", type=float, default=DEFAULT_MIN_SHARE)
    parser.add_argument("--no-logs", action="store_true", help="Decide on size only.")
    parser.add_argument("--no-size", action="store_true", help="Decide on the logs only.")
    parser.add_argument("--apply", action="store_true", help="Rename (default: plan only).")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S"
    )

    configured = {
        "DDD": args.ddd_dir or get_variables_from_env.DANISH_DEMOCRACY_DATA_SOURCE,
        "RFAM": args.rfam_dir or get_variables_from_env.RFAM_DATA_SOURCE,
    }
    sources = ["DDD", "RFAM"] if args.source == "all" else [args.source]
    directories: dict[str, Path] = {}
    for source in sources:
        directory = configured[source]
        if not directory or str(directory).startswith("s3://"):
            logger.error("%s: a local Bronze directory is required (got %r)", source, directory)
            sys.exit(1)
        directories[source] = Path(directory)
    log_dir = args.log_dir or Path(get_variables_from_env.DLT_PIPELINE_RUN_LOG_DIR)

    total = 0
    for entity in list_entities(sources):
        if not entity.incremental or (args.entity and entity.name not in args.entity):
            continue
        files = bronze_files(directories[entity.source] / entity.name, entity)
        decisions = decide(
            files,
            read_run_log(log_dir, entity),
            args.ratio,
            args.min_share,
            use_logs=not args.no_logs,
            use_size=not args.no_size,
        )
        marked = [d for d in decisions if d.full]
        if not marked:
            logger.info(
                "%s/%-15s %3d files: no full extraction found",
                entity.source,
                entity.name,
                len(files),
            )
        for decision in marked:
            logger.info(
                "%s/%-15s %s -> %s  [%.1f MB; %s]",
                entity.source,
                entity.name,
                decision.file.name,
                decision.new_name,
                decision.file.stat().st_size / 1e6,
                decision.evidence,
            )
        if args.apply:
            total += apply_decisions(decisions)
    if args.apply:
        logger.info("%d file(s) renamed", total)
    else:
        logger.info("Dry run: nothing renamed (add --apply)")


if __name__ == "__main__":
    main()
