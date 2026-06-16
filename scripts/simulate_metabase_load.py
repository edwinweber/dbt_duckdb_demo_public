#!/usr/bin/env python3
"""
Simulate concurrent Metabase users querying the DuckDB Gold layer.

Default mode runs a concurrency sweep across multiple user counts and produces
a single HTML report comparing how the database behaves at each level.

Usage
-----
    python scripts/simulate_metabase_load.py                        # sweep 1,5,10,25 users
    python scripts/simulate_metabase_load.py --sweep 1,10,50        # custom sweep
    python scripts/simulate_metabase_load.py --sweep 10             # single run
    python scripts/simulate_metabase_load.py --rounds 5 --jitter 0
    python scripts/simulate_metabase_load.py --output report.html --no-open
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import NamedTuple

from dotenv import load_dotenv

import duckdb

_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_ROOT / ".env", override=False)

# ---------------------------------------------------------------------------
# Queries — one per typical Metabase dashboard panel, Gold views only
# ---------------------------------------------------------------------------

QUERIES: list[tuple[str, str]] = [
    (
        "kpi_counts",
        """
        SELECT
            (SELECT COUNT(*) FROM main_gold.actor_cv   WHERE actor_bk   != 'Unknown') AS total_actors,
            (SELECT COUNT(*) FROM main_gold.case_cv    WHERE case_bk    != 'Unknown') AS total_cases,
            (SELECT COUNT(*) FROM main_gold.vote_cv    WHERE vote_bk    != 'Unknown') AS total_votes,
            (SELECT COUNT(*) FROM main_gold.meeting_cv WHERE meeting_bk != 'Unknown') AS total_meetings
        """,
    ),
    (
        "actors_by_type",
        """
        SELECT actor_type_english, COUNT(*) AS actor_count
        FROM   main_gold.actor_cv
        WHERE  actor_bk != 'Unknown'
        GROUP BY actor_type_english
        ORDER BY actor_count DESC
        """,
    ),
    (
        "meetings_by_type_and_status",
        """
        SELECT meeting_type_english, meeting_status_english, COUNT(*) AS cnt
        FROM   main_gold.meeting_cv
        WHERE  meeting_bk != 'Unknown'
        GROUP BY meeting_type_english, meeting_status_english
        ORDER BY cnt DESC
        LIMIT 30
        """,
    ),
    (
        "meetings_per_year",
        """
        SELECT YEAR(CAST(meeting_date AS DATE)) AS year, COUNT(*) AS meetings
        FROM   main_gold.meeting_cv
        WHERE  meeting_bk != 'Unknown' AND meeting_date IS NOT NULL
        GROUP BY year
        ORDER BY year
        """,
    ),
    (
        "cases_by_type_and_status",
        """
        SELECT case_type_danish, case_status_danish, COUNT(*) AS cnt
        FROM   main_gold.case_cv
        WHERE  case_bk != 'Unknown'
        GROUP BY case_type_danish, case_status_danish
        ORDER BY cnt DESC
        LIMIT 20
        """,
    ),
    (
        "votes_by_type",
        """
        SELECT vt.vote_type_english, COUNT(*) AS vote_count
        FROM   main_gold.vote_cv v
        JOIN   main_gold.vote_type_cv vt ON v.LKHS_vote_type_id = vt.LKHS_vote_type_id
        WHERE  v.vote_bk != 'Unknown'
        GROUP BY vt.vote_type_english
        ORDER BY vote_count DESC
        """,
    ),
    (
        "individual_votes_by_actor_type",
        """
        SELECT a.actor_type_english, iv.individual_voting_type, COUNT(*) AS vote_count
        FROM   main_gold.individual_votes iv
        JOIN   main_gold.actor_cv a ON iv.LKHS_actor_id = a.LKHS_actor_id
        WHERE  iv.LKHS_actor_id != 0
        GROUP BY a.actor_type_english, iv.individual_voting_type
        ORDER BY vote_count DESC
        LIMIT 50
        """,
    ),
    (
        "top_mps_by_participation",
        """
        SELECT a.full_name, a.party_short_name, COUNT(*) AS total_individual_votes
        FROM   main_gold.individual_votes iv
        JOIN   main_gold.actor_cv a ON iv.LKHS_actor_id = a.LKHS_actor_id
        WHERE  a.actor_type_english = 'Member of Parliament'
          AND  iv.LKHS_actor_id != 0
        GROUP BY a.full_name, a.party_short_name
        ORDER BY total_individual_votes DESC
        LIMIT 20
        """,
    ),
    (
        "votes_per_quarter",
        """
        SELECT d.date_year, EXTRACT(QUARTER FROM d.date_day) AS quarter, COUNT(*) AS vote_count
        FROM   main_gold.individual_votes iv
        JOIN   main_gold.date d ON iv.date_day = d.date_day
        WHERE  iv.date_day IS NOT NULL AND d.date_year >= 2010
        GROUP BY d.date_year, quarter
        ORDER BY d.date_year, quarter
        """,
    ),
    (
        "mps_by_party_and_gender",
        """
        SELECT party_short_name, gender_english, COUNT(*) AS mp_count
        FROM   main_gold.actor_cv
        WHERE  actor_type_english = 'Member of Parliament'
          AND  actor_bk != 'Unknown'
          AND  party_short_name IS NOT NULL
        GROUP BY party_short_name, gender_english
        ORDER BY mp_count DESC
        LIMIT 30
        """,
    ),
]

# ---------------------------------------------------------------------------
# DuckDB connection
# ---------------------------------------------------------------------------

# ATTACH modifies shared catalog state across all read-only connections to the
# same file.  A module-level lock plus an "already attached" check avoids the
# "database with name ducklake_catalog already exists" race when N threads open
# connections at roughly the same time.
_ATTACH_LOCK = threading.Lock()


def _open_connection(db_path: str, threads: int = 0) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(db_path, read_only=True)
    # Limit per-connection thread count so concurrent queries share the CPU pool
    # rather than all fighting for all available cores simultaneously.
    if threads > 0:
        conn.execute(f"SET threads = {threads}")
    if os.getenv("SILVER_STORAGE_FORMAT", "duckdb").lower() == "ducklake":
        with _ATTACH_LOCK:
            already = {row[0] for row in conn.execute("SHOW DATABASES").fetchall()}
            if "ducklake_catalog" not in already:
                catalog = os.environ["DUCKLAKE_CATALOG_LOCATION"]
                data_path = os.environ["DUCKLAKE_DATA_PATH"]
                conn.execute("INSTALL ducklake; LOAD ducklake;")
                conn.execute(
                    f"ATTACH 'ducklake:{catalog}' AS ducklake_catalog"
                    f" (DATA_PATH '{data_path}', READ_ONLY)"
                )
    return conn


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------


class Result(NamedTuple):
    query_name: str
    elapsed_ms: float
    is_error: bool


def _user_worker(
    user_id: int,
    db_path: str,
    rounds: int,
    max_jitter: float,
    results: list[Result],
    lock: threading.Lock,
    done_counter: list[int],
    threads: int = 0,
) -> None:
    try:
        conn = _open_connection(db_path, threads=threads)
    except Exception as exc:
        with lock:
            results.append(Result("connect", 0.0, is_error=True))
        print(f"  [user {user_id:03d}] connection failed: {exc}", file=sys.stderr)
        return

    rng = random.Random(user_id)
    try:
        for _ in range(rounds):
            for query_name, sql in QUERIES:
                if max_jitter > 0:
                    time.sleep(rng.uniform(0, max_jitter))
                t0 = time.perf_counter()
                is_error = False
                try:
                    conn.execute(sql).fetchall()
                except Exception as exc:
                    is_error = True
                    print(f"\n  [user {user_id:03d}] {query_name}: {exc}", file=sys.stderr)
                elapsed_ms = (time.perf_counter() - t0) * 1000
                with lock:
                    results.append(Result(query_name, elapsed_ms, is_error))
                    done_counter[0] += 1
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Simulation runner
# ---------------------------------------------------------------------------


def _run_simulation(
    n_users: int,
    rounds: int,
    db_path: str,
    max_jitter: float,
    duckdb_threads: int = 0,
) -> tuple[list[Result], float]:
    """Spin up n_users threads, run all queries, return (results, wall_time_s)."""
    total_queries = n_users * rounds * len(QUERIES)
    results: list[Result] = []
    done_counter = [0]
    lock = threading.Lock()

    threads = [
        threading.Thread(
            target=_user_worker,
            args=(i, db_path, rounds, max_jitter, results, lock, done_counter),
            kwargs={"threads": duckdb_threads},
            daemon=True,
            name=f"user-{i:03d}",
        )
        for i in range(n_users)
    ]

    wall_start = time.perf_counter()
    for t in threads:
        t.start()

    try:
        while any(t.is_alive() for t in threads):
            time.sleep(1.0)
            done = done_counter[0]
            elapsed = time.perf_counter() - wall_start
            pct = done / total_queries * 100 if total_queries else 0
            print(f"\r  {done}/{total_queries} ({pct:.0f}%) in {elapsed:.0f}s", end="", flush=True)
    except KeyboardInterrupt:
        print("\nInterrupted — partial results follow.")

    for t in threads:
        t.join()

    wall_time = time.perf_counter() - wall_start
    print(f"\r  {done_counter[0]}/{total_queries} in {wall_time:.1f}s              ")
    return results, wall_time


# ---------------------------------------------------------------------------
# Shared stats helpers
# ---------------------------------------------------------------------------


def _pct(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = max(0, min(len(sorted_vals) - 1, int(p / 100.0 * len(sorted_vals))))
    return sorted_vals[idx]


# Latency tier thresholds in ms — overridden by --green / --orange CLI flags.
_THRESH_GREEN: float = 1000.0
_THRESH_ORANGE: float = 2000.0


def _tier(ms: float) -> tuple[str, str]:
    """Return (badge_css_class, hex_fill_color) for a given latency in ms."""
    if ms < _THRESH_GREEN:
        return "badge-green", "#22c55e"
    elif ms < _THRESH_ORANGE:
        return "badge-orange", "#f97316"
    return "badge-red", "#ef4444"


def _aggregate(results: list[Result]) -> tuple[dict, dict, list[float], int]:
    """Return (by_query_times, errors_per_query, all_ok_times, total_errors)."""
    by_query: dict[str, list[float]] = defaultdict(list)
    errors_per_query: dict[str, int] = defaultdict(int)
    for r in results:
        if r.is_error:
            errors_per_query[r.query_name] += 1
        else:
            by_query[r.query_name].append(r.elapsed_ms)
    all_ok: list[float] = []
    total_errors = 0
    for name in by_query:
        all_ok.extend(by_query[name])
        total_errors += errors_per_query.get(name, 0)
    return by_query, errors_per_query, all_ok, total_errors


# ---------------------------------------------------------------------------
# Terminal report
# ---------------------------------------------------------------------------


def _print_report(results: list[Result], wall_time: float, n_users: int, n_rounds: int) -> None:
    by_query, errors_per_query, all_ok, total_errors = _aggregate(results)
    col_w = 36
    hdr = (
        f"{'Query':<{col_w}}  {'n':>5}  {'err':>4}  "
        f"{'min':>7}  {'p50':>7}  {'p95':>7}  {'p99':>7}  {'max':>7}  (ms)"
    )
    sep = "─" * len(hdr)
    print(f"\n{'═' * len(hdr)}")
    print(
        f"METABASE LOAD SIMULATION — {n_users} users × {n_rounds} rounds × {len(QUERIES)} queries"
        f" | wall time: {wall_time:.1f}s"
    )
    print("═" * len(hdr))
    print(hdr)
    print(sep)

    for query_name, _ in QUERIES:
        times = sorted(by_query.get(query_name, []))
        n_err = errors_per_query.get(query_name, 0)
        if times:
            print(
                f"{query_name:<{col_w}}  {len(times):>5}  {n_err:>4}  "
                f"{times[0]:>7.0f}  {_pct(times, 50):>7.0f}  "
                f"{_pct(times, 95):>7.0f}  {_pct(times, 99):>7.0f}  {times[-1]:>7.0f}"
            )
        else:
            print(f"{query_name:<{col_w}}  {'—':>5}  {n_err:>4}  " + "  ".join([f"{'—':>7}"] * 5))

    print(sep)
    if all_ok:
        s = sorted(all_ok)
        qps = len(s) / wall_time if wall_time > 0 else 0
        print(
            f"{'TOTAL / AGGREGATE':<{col_w}}  {len(s):>5}  {total_errors:>4}  "
            f"{s[0]:>7.0f}  {_pct(s, 50):>7.0f}  {_pct(s, 95):>7.0f}  "
            f"{_pct(s, 99):>7.0f}  {s[-1]:>7.0f}"
        )
        print(f"\nThroughput: {qps:.1f} queries/s | Concurrency: {n_users} users")
    if total_errors:
        print(f"\nTotal errors: {total_errors} (see stderr)")


# ---------------------------------------------------------------------------
# HTML helpers
# ---------------------------------------------------------------------------

_CSS = """
:root{--bg:#0f1117;--card:#181c28;--border:#252a3d;--text:#e2e8f0;--muted:#7c8799;--accent:#6366f1;}
*{box-sizing:border-box;margin:0;padding:0;}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
  background:var(--bg);color:var(--text);padding:2rem 2.5rem;line-height:1.5;}

.hdr{margin-bottom:1.75rem;}
.hdr h1{font-size:1.55rem;font-weight:700;letter-spacing:-.02em;}
.hdr .sub{color:var(--muted);font-size:.8rem;margin-top:.3rem;}
.hdr .sub code{font-family:"SF Mono",Consolas,monospace;background:rgba(255,255,255,.07);
  padding:1px 5px;border-radius:3px;font-size:.75rem;}

/* Single-run KPI grid */
.kpi-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(155px,1fr));
  gap:.75rem;margin-bottom:2rem;}
.kpi{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:1rem 1.1rem;}
.kpi .lbl{font-size:.65rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;
  color:var(--muted);margin-bottom:.35rem;}
.kpi .val{font-size:1.6rem;font-weight:700;color:var(--accent);
  font-variant-numeric:tabular-nums;line-height:1.1;}
.kpi .unit{font-size:.75rem;color:var(--muted);margin-left:3px;font-weight:400;}

/* Sweep scaling cards */
.sweep-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(185px,1fr));
  gap:.75rem;margin-bottom:2rem;}
.sweep-card{background:var(--card);border:1px solid var(--border);border-left-width:3px;
  border-radius:8px;padding:1rem 1.1rem;}
.sweep-card .users{font-size:1.25rem;font-weight:700;margin-bottom:.6rem;}
.sweep-card .metric{display:flex;justify-content:space-between;align-items:baseline;
  font-size:.775rem;color:var(--muted);margin-top:.25rem;}
.sweep-card .metric .v{font-weight:600;color:var(--text);font-variant-numeric:tabular-nums;}

.sec{font-size:.65rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;
  color:var(--muted);margin-bottom:.6rem;}

/* Tables */
.tbl-wrap{background:var(--card);border:1px solid var(--border);
  border-radius:8px;overflow:hidden;margin-bottom:1.75rem;}
table{width:100%;border-collapse:collapse;font-size:.8rem;}
thead th{background:rgba(99,102,241,.07);color:var(--muted);font-size:.65rem;font-weight:700;
  text-transform:uppercase;letter-spacing:.06em;padding:.6rem .875rem;
  text-align:right;white-space:nowrap;border-bottom:1px solid var(--border);}
thead th:first-child,thead th.left{text-align:left;}
thead th:last-child{text-align:left;min-width:130px;}
tbody td{padding:.7rem .875rem;text-align:right;font-variant-numeric:tabular-nums;
  border-bottom:1px solid rgba(37,42,61,.7);}
tbody td:first-child{text-align:left;}
tbody td:last-child{text-align:left;}
tbody tr:last-child td{border-bottom:none;}
tbody tr:hover td{background:rgba(255,255,255,.018);}
.name{font-family:"SF Mono",Consolas,monospace;font-size:.76rem;}
.agg td{border-top:2px solid var(--border) !important;font-weight:600;}

/* Heatmap table — cells are centered */
.heatmap td:not(:first-child){text-align:center;}
.heatmap thead th:not(:first-child){text-align:center;}

/* Badges */
.badge{display:inline-block;padding:2px 9px;border-radius:9999px;
  font-size:.72rem;font-weight:700;font-variant-numeric:tabular-nums;}
.badge-green {background:rgba(34,197,94,.15); color:#22c55e;}
.badge-orange{background:rgba(249,115,22,.15);color:#f97316;}
.badge-red   {background:rgba(239,68,68,.15); color:#ef4444;}

/* Legend */
.legend{display:flex;gap:1.25rem;flex-wrap:wrap;margin-bottom:1.75rem;align-items:center;}
.legend .lbl{font-size:.65rem;font-weight:700;text-transform:uppercase;
  letter-spacing:.08em;color:var(--muted);margin-right:.25rem;}
.legend-item{display:flex;align-items:center;gap:.35rem;font-size:.78rem;color:var(--muted);}
.dot{width:9px;height:9px;border-radius:50%;flex-shrink:0;}

/* Chart grid */
.chart-row{display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:2rem;}
.chart-card{background:var(--card);border:1px solid var(--border);
  border-radius:8px;padding:1.1rem 1.25rem;overflow:hidden;}
.chart-card h3{font-size:.65rem;font-weight:700;text-transform:uppercase;
  letter-spacing:.08em;color:var(--muted);margin-bottom:.85rem;}

/* Collapsible per-run details */
details{margin-bottom:.5rem;}
summary{cursor:pointer;list-style:none;padding:.55rem .875rem;font-size:.8rem;
  font-weight:600;color:var(--muted);background:var(--card);
  border:1px solid var(--border);border-radius:6px;user-select:none;}
summary::-webkit-details-marker{display:none;}
summary::before{content:"▶ ";font-size:.65rem;}
details[open] summary::before{content:"▼ ";}
summary:hover{color:var(--text);}
details[open] summary{border-bottom-left-radius:0;border-bottom-right-radius:0;
  border-bottom-color:transparent;}
details[open] .tbl-wrap{border-top-left-radius:0;border-top-right-radius:0;margin-top:0;}

/* Footer */
.footer{margin-top:2rem;padding-top:1rem;border-top:1px solid var(--border);
  color:var(--muted);font-size:.75rem;display:flex;gap:2rem;flex-wrap:wrap;}
.footer code{font-family:"SF Mono",Consolas,monospace;background:rgba(255,255,255,.06);
  padding:1px 5px;border-radius:3px;}
"""


def _svg_hbar_chart(
    rows: list[tuple[str, float]],
    label_w: int = 175,
    bar_w: int = 165,
    val_w: int = 72,
    row_h: int = 26,
    row_gap: int = 5,
    pad_v: int = 6,
    fixed_color: str | None = None,
    val_suffix: str = "",
) -> str:
    """Horizontal SVG bar chart.  Pass fixed_color to override tier-based colouring."""
    max_val = max((v for _, v in rows), default=1) or 1
    total_w = label_w + bar_w + val_w
    total_h = pad_v + len(rows) * (row_h + row_gap) - row_gap + pad_v

    parts: list[str] = [
        f'<svg width="{total_w}" height="{total_h}" xmlns="http://www.w3.org/2000/svg">'
        '<style>.mono{font-family:"SF Mono",Consolas,monospace;}</style>'
    ]
    for i, (label, value) in enumerate(rows):
        y = pad_v + i * (row_h + row_gap)
        cy = y + row_h // 2 + 1
        fill_px = max(0, int(bar_w * value / max_val))
        hex_color = fixed_color if fixed_color else _tier(value)[1]
        disp = label if len(label) <= 23 else label[:20] + "…"

        parts.append(
            f'<text x="{label_w - 8}" y="{cy + 4}" text-anchor="end" '
            f'fill="#8892a4" font-size="11.5" class="mono">{disp}</text>'
            f'<rect x="{label_w}" y="{y + 5}" width="{bar_w}" height="{row_h - 10}" '
            f'fill="rgba(255,255,255,0.05)" rx="3"/>'
        )
        if fill_px > 0:
            parts.append(
                f'<rect x="{label_w}" y="{y + 5}" width="{fill_px}" height="{row_h - 10}" '
                f'fill="{hex_color}" opacity="0.82" rx="3"/>'
            )
        parts.append(
            f'<text x="{label_w + bar_w + 6}" y="{cy + 4}" '
            f'fill="{hex_color}" font-size="11" font-weight="700" class="mono">'
            f"{value:.1f}{val_suffix}</text>"
        )
    parts.append("</svg>")
    return "".join(parts)


def _legend_html() -> str:
    return f"""
<div class="legend">
  <span class="lbl">Latency tiers</span>
  <div class="legend-item"><div class="dot" style="background:#22c55e;"></div>&lt; {int(_THRESH_GREEN)} ms — fast</div>
  <div class="legend-item"><div class="dot" style="background:#f97316;"></div>{int(_THRESH_GREEN)}–{int(_THRESH_ORANGE)} ms — slow</div>
  <div class="legend-item"><div class="dot" style="background:#ef4444;"></div>&gt; {int(_THRESH_ORANGE)} ms — very slow</div>
</div>"""


def _detail_table_html(results: list[Result], wall_time: float) -> str:
    """Full per-run breakdown table (used inside <details>)."""
    by_query, errors_per_query, all_ok, total_errors = _aggregate(results)
    max_all = max(all_ok) if all_ok else 1
    rows: list[str] = []

    for q_name, _ in QUERIES:
        times = sorted(by_query.get(q_name, []))
        n_err = errors_per_query.get(q_name, 0)
        if not times:
            rows.append(
                f"<tr><td class='name'>{q_name}</td><td>—</td>"
                + "<td>—</td>" * 5
                + "<td></td></tr>"
            )
            continue
        p50 = _pct(times, 50)
        p99 = _pct(times, 99)
        badge_cls, _ = _tier(p99)
        p50_w = p50 / max_all * 100
        p99_w = p99 / max_all * 100
        _, p50_hex = _tier(p50)
        _, p99_hex = _tier(p99)
        bar = (
            f'<div style="position:relative;height:7px;background:rgba(255,255,255,.05);'
            f'border-radius:3px;min-width:100px;">'
            f'<div style="position:absolute;inset:0;width:{p99_w:.1f}%;'
            f'background:{p99_hex};opacity:.28;border-radius:3px;"></div>'
            f'<div style="position:absolute;inset:0;width:{p50_w:.1f}%;'
            f'background:{p50_hex};opacity:.9;border-radius:3px;"></div>'
            f"</div>"
        )
        err_td = (
            f'<td style="color:#ef4444;font-weight:600;">{n_err}</td>'
            if n_err
            else '<td style="color:#374151;">0</td>'
        )
        rows.append(
            f"<tr><td class='name'>{q_name}</td><td>{len(times)}</td>{err_td}"
            f"<td>{times[0]:.0f}</td><td>{p50:.0f}</td>"
            f"<td>{_pct(times, 95):.0f}</td>"
            f"<td><span class='badge {badge_cls}'>{p99:.0f}</span></td>"
            f"<td>{times[-1]:.0f}</td><td>{bar}</td></tr>"
        )

    if all_ok:
        s = sorted(all_ok)
        agg_p99 = _pct(s, 99)
        badge_cls, _ = _tier(agg_p99)
        qps = len(s) / wall_time if wall_time > 0 else 0
        err_td = (
            f'<td style="color:#ef4444;font-weight:600;">{total_errors}</td>'
            if total_errors
            else '<td style="color:#374151;">0</td>'
        )
        rows.append(
            f"<tr class='agg'><td class='name' style='color:#e2e8f0;'>All queries"
            f"<span style='font-weight:400;color:var(--muted);margin-left:.5rem;'>"
            f"{qps:.1f} q/s</span></td>"
            f"<td>{len(s)}</td>{err_td}"
            f"<td>{s[0]:.0f}</td><td>{_pct(s, 50):.0f}</td><td>{_pct(s, 95):.0f}</td>"
            f"<td><span class='badge {badge_cls}'>{agg_p99:.0f}</span></td>"
            f"<td>{s[-1]:.0f}</td><td></td></tr>"
        )

    return (
        "<div class='tbl-wrap'><table>"
        "<thead><tr><th>Query</th><th>n</th><th>Err</th>"
        "<th>Min</th><th>P50</th><th>P95</th><th>P99</th><th>Max</th>"
        "<th class='left'>P50 / P99</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table></div>"
    )


# ---------------------------------------------------------------------------
# Sweep HTML report
# ---------------------------------------------------------------------------

# Cell bg colours for heatmap (very subtle rgba)
_TIER_BG = {
    "badge-green": "rgba(34,197,94,.06)",
    "badge-orange": "rgba(249,115,22,.08)",
    "badge-red": "rgba(239,68,68,.10)",
}


def _generate_sweep_html_report(
    sweep: list[tuple[int, list[Result], float]],
    rounds: int,
    db_path: str,
    duckdb_threads: int = 0,
) -> str:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    db_name = Path(db_path).stem
    silver_fmt = os.getenv("SILVER_STORAGE_FORMAT", "duckdb")

    # Pre-compute per-run stats
    run_stats: list[dict] = []
    for n_users, results, wall_time in sweep:
        by_query, errors, all_ok, total_errors = _aggregate(results)
        s = sorted(all_ok)
        qps = len(s) / wall_time if wall_time > 0 else 0
        p50 = _pct(s, 50)
        p99 = _pct(s, 99)
        run_stats.append(
            dict(
                n_users=n_users,
                by_query=by_query,
                errors=errors,
                all_ok=s,
                qps=qps,
                p50=p50,
                p99=p99,
                total_errors=total_errors,
                wall_time=wall_time,
            )
        )

    # ── Scaling KPI cards ──
    sweep_counts_label = ", ".join(str(s[0]) for s in sweep)
    cards_html: list[str] = []
    for rs in run_stats:
        badge_cls, hex_color = _tier(rs["p99"])
        label = "1 user" if rs["n_users"] == 1 else f"{rs['n_users']} users"
        cards_html.append(
            f"<div class='sweep-card' style='border-left-color:{hex_color};'>"
            f"<div class='users'>{label}</div>"
            f"<div class='metric'><span>Throughput</span><span class='v'>{rs['qps']:.1f} q/s</span></div>"
            f"<div class='metric'><span>Median (p50)</span><span class='v'>{rs['p50']:.0f} ms</span></div>"
            f"<div class='metric'><span>Tail (p99)</span>"
            f"<span class='v' style='color:{hex_color};'>{rs['p99']:.0f} ms</span></div>"
            f"<div class='metric'><span>Errors</span>"
            f"<span class='v' style='color:{'#ef4444' if rs['total_errors'] else 'var(--muted)'};'>"
            f"{rs['total_errors']}</span></div>"
            f"</div>"
        )

    # ── Scaling charts ──
    user_labels = ["1 user" if s[0] == 1 else f"{s[0]} users" for s in sweep]
    svg_throughput = _svg_hbar_chart(
        list(zip(user_labels, [rs["qps"] for rs in run_stats], strict=True)),
        fixed_color="#6366f1",
        val_suffix=" q/s",
    )
    svg_p99_scale = _svg_hbar_chart(
        list(zip(user_labels, [rs["p99"] for rs in run_stats], strict=True)),
        val_suffix=" ms",
    )

    # ── Heatmap table ──
    col_headers = "".join(
        f"<th>{'1 user' if rs['n_users'] == 1 else str(rs['n_users']) + ' users'}</th>"
        for rs in run_stats
    )
    heatmap_rows: list[str] = []
    for q_name, _ in QUERIES:
        cells = [f"<td class='name'>{q_name}</td>"]
        for rs in run_stats:
            times = sorted(rs["by_query"].get(q_name, []))
            if times:
                p99 = _pct(times, 99)
                badge_cls, _ = _tier(p99)
                bg = _TIER_BG[badge_cls]
                cells.append(
                    f"<td style='background:{bg};'>"
                    f"<span class='badge {badge_cls}'>{p99:.0f}</span></td>"
                )
            else:
                cells.append("<td>—</td>")
        heatmap_rows.append(f"<tr>{''.join(cells)}</tr>")

    # Aggregate row in heatmap
    agg_cells = ["<td class='name' style='color:#e2e8f0;font-weight:600;'>Aggregate p99</td>"]
    for rs in run_stats:
        p99 = _pct(rs["all_ok"], 99)
        badge_cls, _ = _tier(p99)
        bg = _TIER_BG[badge_cls]
        agg_cells.append(
            f"<td style='background:{bg};'><span class='badge {badge_cls}'>{p99:.0f}</span></td>"
        )
    heatmap_rows.append(f"<tr class='agg'>{''.join(agg_cells)}</tr>")

    # ── Per-run collapsible details ──
    details_html: list[str] = []
    for (n_users, results, wall_time), rs in zip(sweep, run_stats, strict=True):
        label = "1 user" if n_users == 1 else f"{n_users} users"
        badge_cls, hex_color = _tier(rs["p99"])
        summary = (
            f"<summary>{label} &nbsp;—&nbsp; "
            f"{rs['qps']:.1f} q/s &nbsp;·&nbsp; "
            f"p50 {rs['p50']:.0f} ms &nbsp;·&nbsp; "
            f"<span style='color:{hex_color};'>p99 {rs['p99']:.0f} ms</span>"
            f"</summary>"
        )
        details_html.append(f"<details>{summary}{_detail_table_html(results, wall_time)}</details>")

    counts_str = " → ".join(
        ("1 user" if rs["n_users"] == 1 else f"{rs['n_users']} users") for rs in run_stats
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Load Sweep Report — {db_name}</title>
  <style>{_CSS}</style>
</head>
<body>

<div class="hdr">
  <h1>Metabase Concurrency Sweep Report</h1>
  <div class="sub">
    Generated <code>{ts}</code> &nbsp;·&nbsp;
    Database <code>{db_name}</code> &nbsp;·&nbsp;
    Silver storage <code>{silver_fmt}</code> &nbsp;·&nbsp;
    {rounds} rounds × {len(QUERIES)} queries per user count
  </div>
</div>

<div class="sec">Scaling overview — {counts_str}</div>
<div class="sweep-grid">{"".join(cards_html)}</div>

<div class="chart-row">
  <div class="chart-card">
    <h3>Throughput Scaling (queries / second)</h3>
    {svg_throughput}
  </div>
  <div class="chart-card">
    <h3>Overall Tail Latency Scaling (p99 ms)</h3>
    {svg_p99_scale}
  </div>
</div>

<div class="sec">P99 latency heatmap — queries × concurrency level</div>
<div class="tbl-wrap">
  <table class="heatmap">
    <thead><tr><th>Query</th>{col_headers}</tr></thead>
    <tbody>{"".join(heatmap_rows)}</tbody>
  </table>
</div>

{_legend_html()}

<div class="sec">Per-run detail (click to expand)</div>
{"".join(details_html)}

<div class="footer">
  <span><strong>Database:</strong> <code>{db_path}</code></span>
  <span><strong>Silver format:</strong> <code>{silver_fmt}</code></span>
  <span><strong>Sweep:</strong> {sweep_counts_label} concurrent users</span>
  <span><strong>Threads per conn:</strong> <code>{"all cores (default)" if duckdb_threads == 0 else str(duckdb_threads)}</code></span>
  <span><strong>Concurrency model:</strong> 1 DuckDB read-only connection per thread</span>
</div>

</body>
</html>"""


# ---------------------------------------------------------------------------
# Single-run HTML report (used when sweep has exactly one entry)
# ---------------------------------------------------------------------------


def _generate_single_html_report(
    results: list[Result],
    wall_time: float,
    n_users: int,
    n_rounds: int,
    db_path: str,
) -> str:
    by_query, errors_per_query, all_ok, total_errors = _aggregate(results)
    s = sorted(all_ok)
    qps = len(s) / wall_time if wall_time > 0 else 0
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    db_name = Path(db_path).stem
    silver_fmt = os.getenv("SILVER_STORAGE_FORMAT", "duckdb")
    max_all = max(all_ok) if all_ok else 1

    p50_data: list[tuple[str, float]] = []
    p99_data: list[tuple[str, float]] = []
    table_rows: list[str] = []

    for q_name, _ in QUERIES:
        times = sorted(by_query.get(q_name, []))
        n_err = errors_per_query.get(q_name, 0)
        if not times:
            table_rows.append(
                f"<tr><td class='name'>{q_name}</td><td>—</td>"
                + "<td>—</td>" * 5
                + "<td></td></tr>"
            )
            continue
        p50 = _pct(times, 50)
        p99 = _pct(times, 99)
        badge_cls, _ = _tier(p99)
        p50_data.append((q_name, p50))
        p99_data.append((q_name, p99))
        p50_w = p50 / max_all * 100
        p99_w = p99 / max_all * 100
        _, p50_hex = _tier(p50)
        _, p99_hex = _tier(p99)
        bar = (
            f'<div style="position:relative;height:7px;background:rgba(255,255,255,.05);'
            f'border-radius:3px;min-width:100px;">'
            f'<div style="position:absolute;inset:0;width:{p99_w:.1f}%;'
            f'background:{p99_hex};opacity:.28;border-radius:3px;"></div>'
            f'<div style="position:absolute;inset:0;width:{p50_w:.1f}%;'
            f'background:{p50_hex};opacity:.9;border-radius:3px;"></div></div>'
        )
        err_td = (
            f'<td style="color:#ef4444;font-weight:600;">{n_err}</td>'
            if n_err
            else '<td style="color:#374151;">0</td>'
        )
        table_rows.append(
            f"<tr><td class='name'>{q_name}</td><td>{len(times)}</td>{err_td}"
            f"<td>{times[0]:.0f}</td><td>{p50:.0f}</td>"
            f"<td>{_pct(times, 95):.0f}</td>"
            f"<td><span class='badge {badge_cls}'>{p99:.0f}</span></td>"
            f"<td>{times[-1]:.0f}</td><td>{bar}</td></tr>"
        )

    if s:
        agg_p99 = _pct(s, 99)
        badge_cls, _ = _tier(agg_p99)
        err_td = (
            f'<td style="color:#ef4444;font-weight:600;">{total_errors}</td>'
            if total_errors
            else '<td style="color:#374151;">0</td>'
        )
        table_rows.append(
            f"<tr class='agg'><td class='name' style='color:#e2e8f0;'>All queries</td>"
            f"<td>{len(s)}</td>{err_td}"
            f"<td>{s[0]:.0f}</td><td>{_pct(s, 50):.0f}</td><td>{_pct(s, 95):.0f}</td>"
            f"<td><span class='badge {badge_cls}'>{agg_p99:.0f}</span></td>"
            f"<td>{s[-1]:.0f}</td><td></td></tr>"
        )

    error_style = "color:#ef4444;" if total_errors else ""
    total_planned = n_users * n_rounds * len(QUERIES)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Load Test Report — {db_name}</title>
  <style>{_CSS}</style>
</head>
<body>
<div class="hdr">
  <h1>Metabase Load Simulation Report</h1>
  <div class="sub">Generated <code>{ts}</code> &nbsp;·&nbsp;
    Database <code>{db_name}</code> &nbsp;·&nbsp; Silver storage <code>{silver_fmt}</code></div>
</div>
<div class="kpi-grid">
  <div class="kpi"><div class="lbl">Concurrent Users</div><div class="val">{n_users}</div></div>
  <div class="kpi"><div class="lbl">Rounds × Queries</div>
    <div class="val">{n_rounds}<span class="unit">× {len(QUERIES)} = {total_planned}</span></div></div>
  <div class="kpi"><div class="lbl">Completed OK</div>
    <div class="val">{len(s)}<span class="unit">queries</span></div></div>
  <div class="kpi"><div class="lbl">Errors</div>
    <div class="val" style="{error_style}">{total_errors}</div></div>
  <div class="kpi"><div class="lbl">Wall Time</div>
    <div class="val">{wall_time:.1f}<span class="unit">s</span></div></div>
  <div class="kpi"><div class="lbl">Throughput</div>
    <div class="val">{qps:.1f}<span class="unit">q/s</span></div></div>
  <div class="kpi"><div class="lbl">Median (p50)</div>
    <div class="val">{_pct(s, 50):.0f}<span class="unit">ms</span></div></div>
  <div class="kpi"><div class="lbl">Tail (p99)</div>
    <div class="val">{_pct(s, 99):.0f}<span class="unit">ms</span></div></div>
</div>
<div class="sec">Query performance — all latencies in ms</div>
<div class="tbl-wrap"><table>
  <thead><tr><th>Query</th><th>n</th><th>Err</th>
    <th>Min</th><th>P50</th><th>P95</th><th>P99</th><th>Max</th>
    <th class="left">P50 / P99 bar</th></tr></thead>
  <tbody>{"".join(table_rows)}</tbody>
</table></div>
{_legend_html()}
<div class="chart-row">
  <div class="chart-card"><h3>Median Latency (p50) per Query</h3>
    {_svg_hbar_chart(p50_data, val_suffix=" ms") if p50_data else "<em>no data</em>"}</div>
  <div class="chart-card"><h3>Tail Latency (p99) per Query</h3>
    {_svg_hbar_chart(p99_data, val_suffix=" ms") if p99_data else "<em>no data</em>"}</div>
</div>
<div class="footer">
  <span><strong>Database:</strong> <code>{db_path}</code></span>
  <span><strong>Silver format:</strong> <code>{silver_fmt}</code></span>
  <span><strong>Concurrency:</strong> {n_users} threads, 1 DuckDB read-only connection each</span>
</div>
</body></html>"""


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _resolve_db_path(cli_path: str | None) -> str:
    if cli_path:
        return cli_path
    env_path = os.getenv("DUCKDB_DATABASE_LOCATION")
    if env_path:
        return env_path
    default = str(_ROOT / "duckdb" / "danish_democracy_data.duckdb")
    print(f"Warning: using fallback DB path {default}", file=sys.stderr)
    return default


def _parse_sweep(raw: str) -> list[int]:
    try:
        counts = [int(x.strip()) for x in raw.split(",") if x.strip()]
    except ValueError:
        print(f"Error: --sweep must be comma-separated integers, got: {raw!r}", file=sys.stderr)
        sys.exit(1)
    if not counts:
        print("Error: --sweep must contain at least one value.", file=sys.stderr)
        sys.exit(1)
    return sorted(set(counts))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Simulate concurrent Metabase users querying the DuckDB Gold layer.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--sweep",
        type=str,
        default="1,5,10,25",
        metavar="N[,N...]",
        help="Comma-separated user counts to simulate (runs sequentially)",
    )
    parser.add_argument(
        "--rounds", type=int, default=3, help="Full query-set runs per user per sweep step"
    )
    parser.add_argument("--db", type=str, default=None, help="Path to the DuckDB database file")
    parser.add_argument(
        "--jitter",
        type=float,
        default=0.3,
        help="Max random pause in seconds between queries per user (0 = no pause)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        help="HTML report output path (default: reports/load_sweep_YYYYMMDD_HHMMSS.html)",
    )
    parser.add_argument(
        "--no-open", action="store_true", help="Skip opening the report in a browser"
    )
    parser.add_argument(
        "--green",
        type=float,
        default=1000.0,
        metavar="MS",
        help="Upper bound (ms) for the green latency tier",
    )
    parser.add_argument(
        "--orange",
        type=float,
        default=2000.0,
        metavar="MS",
        help="Upper bound (ms) for the orange latency tier; above this is red",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        metavar="N",
        help="DuckDB threads per connection (0 = DuckDB default = all cores). "
        "Limiting threads lets concurrent connections share the CPU pool "
        "without thrashing. 4 is a good baseline for 5 concurrent users on "
        "a 22-core host; tune up for fewer users or down for more.",
    )
    args = parser.parse_args()

    global _THRESH_GREEN, _THRESH_ORANGE
    _THRESH_GREEN = args.green
    _THRESH_ORANGE = args.orange

    db_path = _resolve_db_path(args.db)
    if not Path(db_path).exists():
        print(f"Error: DuckDB file not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    sweep_counts = _parse_sweep(args.sweep)

    threads_label = (
        f"{args.threads} threads/conn" if args.threads > 0 else "DuckDB default (all cores)"
    )
    print(f"Database : {db_path}")
    print(f"Sweep    : {sweep_counts} users")
    print(
        f"Rounds   : {args.rounds}  |  Jitter: up to {args.jitter:.2f}s  |  Queries: {len(QUERIES)}"
    )
    print(f"Threads  : {threads_label}")
    print()

    sweep_results: list[tuple[int, list[Result], float]] = []

    for idx, n_users in enumerate(sweep_counts, 1):
        total = n_users * args.rounds * len(QUERIES)
        label = "1 user" if n_users == 1 else f"{n_users} users"
        print(f"── Run {idx}/{len(sweep_counts)}: {label} × {args.rounds} rounds = {total} queries")
        results, wall_time = _run_simulation(
            n_users, args.rounds, db_path, args.jitter, args.threads
        )
        sweep_results.append((n_users, results, wall_time))
        _print_report(results, wall_time, n_users, args.rounds)
        print()

    # ── Write HTML report ──
    report_dir = _ROOT / "reports"
    report_dir.mkdir(exist_ok=True)
    ts_file = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = Path(args.output) if args.output else report_dir / f"load_sweep_{ts_file}.html"

    if len(sweep_counts) == 1:
        n_users, results, wall_time = sweep_results[0]
        html = _generate_single_html_report(results, wall_time, n_users, args.rounds, db_path)
    else:
        html = _generate_sweep_html_report(sweep_results, args.rounds, db_path, args.threads)

    out_path.write_text(html, encoding="utf-8")
    print(f"HTML report: {out_path}")

    if not args.no_open:
        import webbrowser

        webbrowser.open(out_path.as_uri())


if __name__ == "__main__":
    main()
