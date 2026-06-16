#!/usr/bin/env python3
"""
upgrade_check.py — Find the newest compatible package versions for dbt_duckdb_demo.

The script:
  1. Queries PyPI for the latest available version of every key package.
  2. Creates an isolated temp venv and asks pip to resolve all packages with
     their upper-bound caps removed (floor constraints only).
  3. Compares resolved versions against what is currently installed.
  4. Warns about known inter-package coupling constraints that pip's resolver
     cannot enforce on its own (dagster ↔ dagster-dbt, dbt-core cap from
     dagster-dbt, DuckDB Azure-regression note).
  5. Optionally runs pytest against the resolved environment.
  6. Optionally patches pyproject.toml with updated floor constraints and
     reinstalls the real venv.

Usage:
    python upgrade_check.py             # resolve + report only
    python upgrade_check.py --test      # also run pytest in the temp venv
    python upgrade_check.py --apply     # patch pyproject.toml + reinstall real venv

Requirements: Python >=3.12, internet access, pip in PATH.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

# ── Repo root ─────────────────────────────────────────────────────────────────
REPO = Path(__file__).resolve().parent
PYPROJECT = REPO / "pyproject.toml"

# ── Current constraints from pyproject.toml (keep in sync manually) ───────────
# These are the *combined* install extras: .[dagster,dev]
CURRENT_CONSTRAINTS: dict[str, str] = {
    # Core dependencies
    "dlt": ">=1.24,<2",
    "requests": ">=2.33",
    "python-dotenv": ">=1.0",
    "adlfs": ">=2026.2",
    "azure-identity": ">=1.25",
    "azure-storage-file-datalake": ">=12.23",
    "dbt-core": ">=1.11,<1.12",
    "dbt-duckdb": ">=1.10,<2",
    "duckdb": ">=1.5.1,<1.6",
    "deltalake": ">=1.5",
    "pyarrow": ">=17",
    "sqlalchemy": ">=2.0",
    "pymysql": ">=1.1",
    # Dagster extras
    "dagster": ">=1.12,<2",
    "dagster-webserver": ">=1.12,<2",
    "dagster-dbt": ">=0.29,<1",
    # Dev extras
    "pytest": ">=8.0",
    "ruff": ">=0.9",
    "mypy": ">=1.10",
    "types-requests": ">=0",
    "pandas": ">=2.0",
}

# ── Relaxed constraints: drop upper caps so pip can go as high as possible ───
# We keep floors so pip respects the minimum API surface we actually use.
# Note: dlt<2, dagster<2, dagster-dbt<1 are *major* caps — kept intentionally
# because 2.0 releases typically break APIs. Remove them here to probe.
RELAXED_CONSTRAINTS: list[str] = [
    "dlt>=1.24",
    "requests>=2.33",
    "python-dotenv>=1.0",
    "adlfs>=2026.2",
    "azure-identity>=1.25",
    "azure-storage-file-datalake>=12.23",
    "dbt-core>=1.11",  # cap removed — let dagster-dbt constrain it
    "dbt-duckdb>=1.10",
    "duckdb>=1.5.1",  # cap removed — see Azure-regression note below
    "deltalake>=1.5",
    "pyarrow>=17",
    "sqlalchemy>=2.0",
    "pymysql>=1.1",
    "dagster>=1.12",
    "dagster-webserver>=1.12",
    "dagster-dbt>=0.29",
    "pytest>=8.0",
    "ruff>=0.9",
    "mypy>=1.10",
    "types-requests",
    "pandas>=2.0",
]

# ── Known coupling constraints (informational — not enforced by pip alone) ────
COUPLING_NOTES = [
    (
        "dagster-dbt ↔ dagster",
        "dagster-dbt pins dagster to an *exact* version (e.g. dagster-dbt 0.29.x "
        "requires dagster==1.13.x). Upgrading dagster-dbt's minor bumps dagster's "
        "minor in lockstep. Check the dagster-dbt release notes before bumping.",
    ),
    (
        "dagster-dbt ↔ dbt-core",
        "dagster-dbt caps dbt-core at <(next minor). E.g. 0.29.x requires "
        "dbt-core<1.12. To use dbt-core 1.12+ you must upgrade dagster-dbt "
        "to a version that supports it.",
    ),
    (
        "dbt-duckdb ↔ dbt-core + duckdb",
        "dbt-duckdb requires dbt-core>=1.8 (no upper cap) and duckdb>=1.0 "
        "(no upper cap as of 1.10.1) — so dbt-duckdb itself is not the blocker.",
    ),
    (
        "duckdb >=1.6 — Azure/OneLake regression",
        "DuckDB's built-in Delta writer has an Azure/OneLake regression in "
        "versions >=1.6 (at the time this note was written). We export via "
        "the 'deltalake' Python library (not DuckDB's writer), so the regression "
        "does NOT block a duckdb bump — but verify with a real OneLake run before "
        "shipping >=1.6 to production.",
    ),
]

# ─────────────────────────────────────────────────────────────────────────────


def pypi_latest(package: str) -> str | None:
    """Return the latest non-pre-release version of *package* from PyPI."""
    url = f"https://pypi.org/pypi/{package}/json"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.load(resp)
        return data["info"]["version"]
    except Exception as exc:
        print(f"  [warn] PyPI lookup failed for {package}: {exc}")
        return None


def pypi_requires_dist(package: str, version: str) -> list[str]:
    """Return requires_dist list for *package*==*version* from PyPI."""
    url = f"https://pypi.org/pypi/{package}/{version}/json"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.load(resp)
        return data["info"]["requires_dist"] or []
    except Exception:
        return []


def run(
    cmd: list[str], cwd: Path | None = None, capture: bool = False
) -> subprocess.CompletedProcess:
    kwargs: dict = {"cwd": cwd, "check": False}
    if capture:
        kwargs["capture_output"] = True
        kwargs["text"] = True
    return subprocess.run(cmd, **kwargs)


def pip_in(venv: Path) -> list[str]:
    """Return the pip executable inside *venv*."""
    return [str(venv / "bin" / "pip")]


def python_in(venv: Path) -> str:
    return str(venv / "bin" / "python")


def installed_versions(venv: Path) -> dict[str, str]:
    """Return {package_name_lower: version} for every package in *venv*."""
    result = run(
        pip_in(venv) + ["list", "--format=json"],
        capture=True,
    )
    if result.returncode != 0:
        return {}
    pkgs = json.loads(result.stdout)
    return {p["name"].lower().replace("_", "-"): p["version"] for p in pkgs}


def current_installed_versions() -> dict[str, str]:
    """Return installed versions in the *active* Python environment."""
    result = subprocess.run(
        [sys.executable, "-m", "pip", "list", "--format=json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return {}
    pkgs = json.loads(result.stdout)
    return {p["name"].lower().replace("_", "-"): p["version"] for p in pkgs}


def create_venv(path: Path) -> None:
    subprocess.run([sys.executable, "-m", "venv", str(path)], check=True, capture_output=True)


def install_relaxed(venv: Path) -> tuple[bool, str]:
    """Install RELAXED_CONSTRAINTS into *venv*. Returns (success, stderr)."""
    args = (
        pip_in(venv)
        + [
            "install",
            "--quiet",
            "--upgrade",
            # Use a cache dir inside the system cache to avoid re-downloading
            "--cache-dir",
            os.path.expanduser("~/.cache/pip"),
        ]
        + RELAXED_CONSTRAINTS
    )
    result = run(args, capture=True)
    return result.returncode == 0, result.stderr or ""


def smoke_import(venv: Path) -> tuple[bool, str]:
    """Run a quick import of the key packages inside *venv*."""
    code = (
        "import dlt, dbt.version, duckdb, dagster, deltalake, sqlalchemy; "
        "print('dbt', dbt.version.__version__); "
        "print('duckdb', duckdb.__version__); "
        "print('dagster', dagster.__version__); "
        "print('dlt', dlt.__version__); "
        "print('deltalake', deltalake.__version__); "
        "print('sqlalchemy', sqlalchemy.__version__)"
    )
    result = subprocess.run(
        [python_in(venv), "-c", code],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0, (result.stdout + result.stderr).strip()


def run_pytest(venv: Path) -> bool:
    env = {
        **os.environ,
        "STORAGE_TARGET": "local",
        "SILVER_STORAGE_FORMAT": "duckdb",
        "LOCAL_STORAGE_PATH": "/tmp/ddd_upgrade_check",
        "DANISH_DEMOCRACY_DATA_SOURCE": "/tmp/ddd_upgrade_check/Files/Bronze/DDD",
        "RFAM_DATA_SOURCE": "/tmp/ddd_upgrade_check/Files/Bronze/RFAM",
        "DUCKDB_DATABASE_LOCATION": "/tmp/ddd_upgrade_check/test.duckdb",
        "DUCKDB_DATABASE": "danish_democracy_data",
        "DLT_PIPELINES_DIR": "/tmp/ddd_upgrade_check/pipelines_dir",
        "DLT_PIPELINE_RUN_LOG_DIR": "/tmp/ddd_upgrade_check/logs",
        "DBT_PROJECT_DIRECTORY": str(REPO / "dbt"),
        "DBT_MODELS_DIRECTORY": str(REPO / "dbt" / "models"),
        "DBT_LOGS_DIRECTORY": "/tmp/ddd_upgrade_check/dbt/logs",
        "DBT_FRESHNESS_WARN_AFTER_DAYS": "2",
        "DBT_FRESHNESS_ERROR_AFTER_DAYS": "7",
        "DAGSTER_HOME": "/tmp/ddd_upgrade_check/dagster",
        "DANISH_DEMOCRACY_BASE_URL": "https://oda.ft.dk/api",
        "DANISH_DEMOCRACY_DEFAULT_DAYS_TO_LOAD": "31",
        "RFAM_CONNECTION_STRING": "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam",
        "RFAM_DEFAULT_DAYS_TO_LOAD": "365",
    }
    # Install the project itself (editable) into the temp venv first
    result = subprocess.run(
        pip_in(venv) + ["install", "--quiet", "-e", str(REPO)],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        print("  [warn] editable install into temp venv failed — skipping pytest")
        return False

    result = subprocess.run(
        [python_in(venv), "-m", "pytest", str(REPO / "tests"), "-v", "--tb=short", "-q"],
        env=env,
        cwd=REPO,
    )
    return result.returncode == 0


def colour(text: str, code: str) -> str:
    if not sys.stdout.isatty():
        return text
    return f"\033[{code}m{text}\033[0m"


def green(t: str) -> str:
    return colour(t, "32")


def yellow(t: str) -> str:
    return colour(t, "33")


def red(t: str) -> str:
    return colour(t, "31")


def bold(t: str) -> str:
    return colour(t, "1")


def cyan(t: str) -> str:
    return colour(t, "36")


def print_section(title: str) -> None:
    print()
    print(bold(f"{'─' * 60}"))
    print(bold(f"  {title}"))
    print(bold(f"{'─' * 60}"))


def _ver(v: str) -> tuple[int, ...]:
    """Parse a version string into a comparable tuple of ints."""
    return tuple(int(x) for x in re.split(r"[.\-]", v) if x.isdigit())


def suggest_pyproject_patch(
    resolved: dict[str, str],
    currently_installed: dict[str, str],
) -> dict[str, tuple[str, str]]:
    """
    Return {package: (old_floor, new_floor)} for packages where the resolved
    version is strictly *higher* than both the current install and the current
    floor, so pyproject.toml floors can be bumped without regressions.
    Packages where the resolved version is *lower* than what is installed
    (meaning something in the dependency graph pulls it down) are skipped.
    """
    patches: dict[str, tuple[str, str]] = {}
    floor_re = re.compile(r">=([0-9][^,\s]*)")

    for pkg, constraint in CURRENT_CONSTRAINTS.items():
        key = pkg.lower()
        res_ver = resolved.get(key)
        cur_ver = currently_installed.get(key)
        if not res_ver or not cur_ver:
            continue
        m = floor_re.search(constraint)
        if not m:
            continue
        old_floor = m.group(1)
        # Only suggest bumping when the resolver picked a *higher* version
        # than what is currently installed — never suggest a downgrade.
        if _ver(res_ver) > _ver(cur_ver) and res_ver != old_floor:
            patches[pkg] = (old_floor, res_ver)
    return patches


def apply_pyproject_patches(patches: dict[str, tuple[str, str]]) -> None:
    """Bump >= floors in pyproject.toml for all packages in *patches*."""
    text = PYPROJECT.read_text()
    for pkg, (old_floor, new_ver) in patches.items():
        # Match e.g. "dlt>=1.24" or "dlt>=1.24,<2" anywhere in the file
        pattern = re.compile(
            r'("' + re.escape(pkg) + r'[^"]*">=' + re.escape(old_floor) + r")",
            re.IGNORECASE,
        )
        old_f, new_v = old_floor, new_ver
        text = pattern.sub(
            lambda m, o=old_f, n=new_v: m.group(0).replace(f">={o}", f">={n}"),
            text,
        )
    PYPROJECT.write_text(text)
    print(f"  Patched {PYPROJECT}")


# ─────────────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--test", action="store_true", help="Run pytest in the temp venv after resolving"
    )
    parser.add_argument(
        "--apply", action="store_true", help="Patch pyproject.toml floors + reinstall real venv"
    )
    args = parser.parse_args()

    # ── Step 1: Query PyPI for latest available versions ─────────────────────
    print_section("Step 1 — Query PyPI for latest versions")
    key_packages = [
        "dlt",
        "dbt-core",
        "dbt-duckdb",
        "duckdb",
        "deltalake",
        "pyarrow",
        "dagster",
        "dagster-webserver",
        "dagster-dbt",
        "requests",
        "python-dotenv",
        "adlfs",
        "azure-identity",
        "azure-storage-file-datalake",
        "sqlalchemy",
        "pymysql",
        "pandas",
        "ruff",
        "mypy",
        "pytest",
    ]
    pypi_latest_map: dict[str, str | None] = {}
    for pkg in key_packages:
        v = pypi_latest(pkg)
        pypi_latest_map[pkg] = v
        status = green(f"  latest: {v}") if v else red("  LOOKUP FAILED")
        print(f"  {pkg:<40} {status}")

    # ── Step 2: Check key coupling constraints via PyPI metadata ─────────────
    print_section("Step 2 — Inspect cross-package coupling constraints")
    dbt_latest = pypi_latest_map.get("dagster-dbt")
    if dbt_latest:
        reqs = pypi_requires_dist("dagster-dbt", dbt_latest)
        relevant = [
            r for r in reqs if any(k in r.lower() for k in ["dagster", "dbt-core", "dbt-common"])
        ]
        print(f"  dagster-dbt {dbt_latest} requires:")
        for r in relevant:
            print(f"    {r}")

    dbt_duckdb_latest = pypi_latest_map.get("dbt-duckdb")
    if dbt_duckdb_latest:
        reqs = pypi_requires_dist("dbt-duckdb", dbt_duckdb_latest)
        relevant = [
            r for r in reqs if any(k in r.lower() for k in ["dbt-core", "duckdb", "dbt-common"])
        ]
        print(f"  dbt-duckdb {dbt_duckdb_latest} requires:")
        for r in relevant:
            print(f"    {r}")

    # ── Step 3: Resolve in a temp venv ───────────────────────────────────────
    print_section("Step 3 — Resolve relaxed constraints in isolated venv")
    tmpdir = tempfile.mkdtemp(prefix="ddd_upgrade_")
    venv_path = Path(tmpdir) / "venv"
    print(f"  Creating venv at {venv_path} …")
    try:
        create_venv(venv_path)
        print("  Installing packages (this takes a minute) …")
        ok, stderr = install_relaxed(venv_path)
        if not ok:
            print(red("  pip install FAILED — see output above"))
            print(stderr[-3000:] if len(stderr) > 3000 else stderr)
            return 1
        print(green("  Install succeeded"))

        resolved = installed_versions(venv_path)
        currently_installed = current_installed_versions()

        # ── Step 4: Smoke import ─────────────────────────────────────────────
        print_section("Step 4 — Smoke import test")
        ok, output = smoke_import(venv_path)
        if ok:
            print(green("  All key packages import successfully"))
            print("  " + output.replace("\n", "\n  "))
        else:
            print(red("  Import FAILED"))
            print("  " + output.replace("\n", "\n  "))

        # ── Step 5: Version diff table ───────────────────────────────────────
        print_section("Step 5 — Version comparison")
        col_w = (34, 14, 14, 14)
        header = (
            f"  {'Package':<{col_w[0]}}"
            f"{'Installed':<{col_w[1]}}"
            f"{'Resolved':<{col_w[2]}}"
            f"{'PyPI latest':<{col_w[3]}}"
            f"  Change?"
        )
        print(header)
        print("  " + "─" * (sum(col_w) + 12))

        for pkg in key_packages:
            key = pkg.lower()
            cur = currently_installed.get(key, "—")
            res = resolved.get(key, "—")
            pypi = pypi_latest_map.get(pkg) or "—"
            if res == "—":
                change = yellow("not resolved")
            elif cur == "—":
                change = green(f"new: {res}")
            elif _ver(res) > _ver(cur):
                change = green(f"↑ {cur} → {res}")
            elif _ver(res) < _ver(cur):
                # Resolved lower than installed: a transitive dep is capping it
                change = yellow(f"↓ capped by dep (have {cur})")
            else:
                change = "same"
            print(
                f"  {pkg:<{col_w[0]}}{cur:<{col_w[1]}}{res:<{col_w[2]}}{pypi:<{col_w[3]}}  {change}"
            )

        # ── Step 6: Coupling notes ───────────────────────────────────────────
        print_section("Step 6 — Known coupling constraints")
        for title, note in COUPLING_NOTES:
            print(f"  {bold(cyan(title))}")
            # Word-wrap the note at 70 chars
            words = note.split()
            line = "    "
            for word in words:
                if len(line) + len(word) + 1 > 74:
                    print(line)
                    line = "    " + word + " "
                else:
                    line += word + " "
            print(line.rstrip())
            print()

        # ── Step 7: Optional pytest ──────────────────────────────────────────
        if args.test:
            print_section("Step 7 — pytest in resolved venv")
            ok = run_pytest(venv_path)
            if ok:
                print(green("  All tests passed"))
            else:
                print(red("  Tests FAILED — see output above"))
                return 1

        # ── Step 8: Suggest / apply pyproject.toml patches ──────────────────
        patches = suggest_pyproject_patch(resolved, currently_installed)
        print_section("Step 8 — Suggested pyproject.toml floor updates")
        if not patches:
            print(
                green(
                    "  No floor updates needed — all packages are already at their resolved versions"
                )
            )
        else:
            for pkg, (old, new) in sorted(patches.items()):
                print(f"  {pkg:<40} >={old}  →  >={new}")

        if args.apply and patches:
            print()
            print("  Patching pyproject.toml …")
            apply_pyproject_patches(patches)
            print("  Reinstalling real venv …")
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "-e",
                    str(REPO) + "[dagster,dev]",
                    "--upgrade",
                ],
            )
            if result.returncode == 0:
                print(green("  Real venv updated"))
            else:
                print(red("  Reinstall failed — check output above"))
                return 1

        elif args.apply and not patches:
            print()
            print(green("  pyproject.toml already up to date — nothing to patch"))

        if not args.apply and patches:
            print()
            print(yellow("  Run with --apply to patch pyproject.toml and reinstall."))

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    print()
    print(bold(green("Done.")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
