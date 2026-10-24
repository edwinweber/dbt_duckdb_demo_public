# CLAUDE.md

The project reference lives in the tracked, public file below so it cannot drift
from a private copy. Edit it there, not here.

@documentation/project_reference.md

## Notes for AI assistants

- Run `pytest tests/`, `ruff check .`, `ruff format --check .` and `mypy --package ddd_python` before declaring work done.
- The DuckDB 2.0 quack RPC server (`ddd_python/ddd_dlt/duckdb_quack_server.py`) is the sole read-write owner of the database in Docker. dbt runs via `dbt-quack` (`ddd_python/ddd_dbt/dbt_quack.py`). Local/CI runs use direct file access (no quack).
- dbt uses `dbt-core>=1.12.6,<1.13` + `dbt-duckdb>=1.11.0,<2`. dbt-oss is no longer used: it ships its own DuckDB 1.5, which cannot work with DuckDB 2.0 (ADR-11).
