"""dbt-quack — dbt wrapper that patches dbt-duckdb for quack CONNECT mode.

Entry point for the ``dbt-quack`` console script.  Applies three patches to
dbt-duckdb then delegates to dbt's own CLI runner (``dbtRunner``).

Patches are active only when ``QUACK_SERVER_HOST`` is set; without it the
script falls through to a plain ``dbt`` invocation, which is the correct
behaviour in CI where the profile selects the direct-file ``local`` target.

Three patches (all required as of duckdb 2.0.0 + quack, dbt-duckdb 1.11.1):

1. CONNECT on every cursor (patch name: ``connect``).
   A cursor opened by dbt-duckdb's ``Environment.initialize_cursor`` inherits
   the Python connection object but *not* its quack session.  Each cursor
   starts on the local in-memory database, so without an explicit
   ``CONNECT <alias>`` every dbt statement resolves names locally (to nothing).
   Fix: monkey-patch ``initialize_cursor`` to run ``CONNECT`` immediately after
   the original implementation.
   Remove when: dbt-duckdb adds per-cursor init SQL, or quack propagates
   ``CONNECT`` to cursors automatically.

2. DuckLake detection (patch name: ``is_ducklake``).
   ``DuckDBAdapter.is_ducklake()`` only inspects databases ATTACHed in the dbt
   profile; server-side catalogs (e.g. ``ducklake_catalog``) are invisible to
   it.  Without this patch dbt issues ``DROP … CASCADE`` on ducklake relations,
   which DuckLake refuses with "Cascade Drop not supported in DuckLake".
   Fix: extend ``is_ducklake`` to treat any database name listed in
   ``QUACK_DUCKLAKE_DATABASES`` as a DuckLake catalog.
   Remove when: dbt-duckdb introspects server-side DuckLake catalogs.

3. Per-thread connection (patch name: ``per_thread``).
   dbt-duckdb normally shares one ``duckdb.connect()`` across all dbt threads,
   giving each thread a cursor on that shared connection.  Under quack CONNECT
   those cursors share the same server session, so concurrent threads cancel
   each other with "INTERRUPT Error: superseded by a new query".  A thread-local
   ``conn`` property gives every thread its own ``duckdb.connect()`` call and
   quack ``ATTACH``, after which patch 1 (CONNECT) makes it server-side.
   Remove when: quack or dbt-duckdb serialises concurrent cursor calls on a
   shared connection.

To test individual patches after an upgrade:
    QUACK_SKIP_PATCHES=connect,is_ducklake,per_thread dbt-quack build ...
"""

import atexit
import contextlib
import os
import sys
import threading

_SKIP = {p.strip() for p in os.environ.get("QUACK_SKIP_PATCHES", "").split(",") if p.strip()}


def _apply_patches() -> None:
    """Apply the three quack CONNECT-mode patches to dbt-duckdb internals."""
    connect_db = os.environ.get("QUACK_CONNECT_DATABASE") or os.environ.get(
        "DUCKDB_DATABASE", "danish_democracy_data"
    )
    ducklake_dbs = {
        name.strip().lower()
        for name in os.environ.get("QUACK_DUCKLAKE_DATABASES", "ducklake_catalog").split(",")
        if name.strip()
    }

    from dbt.adapters.duckdb.environments import Environment, local
    from dbt.adapters.duckdb.impl import DuckDBAdapter

    _orig_initialize_cursor = Environment.initialize_cursor.__func__  # type: ignore[attr-defined]
    _orig_is_ducklake = DuckDBAdapter.is_ducklake

    # ── Patch 1: CONNECT on every cursor ─────────────────────────────────────
    if "connect" not in _SKIP:

        def _initialize_cursor(cls, creds, cursor, *args, **kwargs):
            cursor = _orig_initialize_cursor(cls, creds, cursor, *args, **kwargs)
            cursor.execute(f"CONNECT {connect_db}")
            return cursor

        Environment.initialize_cursor = classmethod(_initialize_cursor)  # type: ignore[method-assign,assignment]

    # ── Patch 2: DuckLake detection for server-side catalogs ─────────────────
    if "is_ducklake" not in _SKIP:

        def _is_ducklake(self, relation):
            if (
                relation is not None
                and relation.database
                and relation.database.lower() in ducklake_dbs
            ):
                return True
            return _orig_is_ducklake(self, relation)

        _is_ducklake._is_available_ = True  # type: ignore[attr-defined]
        DuckDBAdapter.is_ducklake = _is_ducklake  # type: ignore[method-assign]

    # ── Patch 3: Per-thread DuckDB connection ─────────────────────────────────
    if "per_thread" not in _SKIP:
        _tls = threading.local()
        _all_conns: list = []
        _all_lock = threading.Lock()

        def _get_conn(self):
            return getattr(_tls, "conns", {}).get(id(self))

        def _set_conn(self, value):
            if not hasattr(_tls, "conns"):
                _tls.conns = {}
            _tls.conns[id(self)] = value
            if value is not None:
                with _all_lock:
                    _all_conns.append(value)

        local.LocalEnvironment.conn = property(_get_conn, _set_conn)  # type: ignore[assignment]

        @atexit.register
        def _close_all():
            with _all_lock:
                for c in _all_conns:
                    with contextlib.suppress(Exception):
                        c.close()


def main() -> None:
    """Entry point for the ``dbt-quack`` console script."""
    if not os.environ.get("QUACK_SERVER_HOST"):
        import warnings

        warnings.warn(
            "QUACK_SERVER_HOST is not set — dbt-quack running without quack patches. "
            "Use DBT_EXECUTABLE=dbt for non-quack environments.",
            stacklevel=1,
        )
    else:
        _apply_patches()

    from dbt.cli.main import dbtRunner

    res = dbtRunner().invoke(sys.argv[1:])
    sys.exit(0 if res.success else 1)


if __name__ == "__main__":
    main()
