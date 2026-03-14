"""
switch-point-finder/query2.py
Finds where the PostgreSQL optimizer switches execution plans as
production_year threshold changes — for IMDB JOB Query 1.

Sweeps the year range in coarse steps, detects plan hash changes,
then binary-searches to find the exact switch year.

Usage:
    python query2.py
"""

import psycopg2
import json
import os
import sys
import hashlib
from datetime import datetime

# ─────────────────────────────────────────────
# Import structural_hash from comparator
# ─────────────────────────────────────────────

SCRIPT_DIR     = os.path.dirname(os.path.realpath(__file__))
COMPARATOR_DIR = os.path.realpath(os.path.join(SCRIPT_DIR, "..", "query-comparision"))

if not os.path.isdir(COMPARATOR_DIR):
    raise RuntimeError(f"Cannot find query-comparision/ at: {COMPARATOR_DIR}")

if COMPARATOR_DIR not in sys.path:
    sys.path.insert(0, COMPARATOR_DIR)

from comparator import structural_hash   # noqa: E402


# ═════════════════════════════════════════════
# Configuration
# ═════════════════════════════════════════════

CONFIG = {
    "db": {
        "host":     "localhost",
        "port":     5432,
        "database": "imdb_job",
        "user":     "postgres",
        "password": "postgrespassword",
    },

    # ── JOB Query 2 — title + cast_info + name + movie_info + info_type ──
    # Based on JOB query 18b family. Joins 5 tables including the three
    # largest (cast_info 36M, movie_info 14M, name 4M) with production_year
    # as the sweep parameter. Row counts go from 34 at year=1880 to 9M at
    # year=2000, forcing multiple plan switches across the full range.
    "query": """
        EXPLAIN (FORMAT JSON)
        SELECT MIN(t.title)    AS movie_title,
               MIN(n.name)     AS actor_name,
               MIN(mi.info)    AS genre
        FROM cast_info  AS ci,
             info_type  AS it,
             movie_info AS mi,
             name       AS n,
             title      AS t
        WHERE it.info          = 'genres'
          AND t.id             = ci.movie_id
          AND t.id             = mi.movie_id
          AND ci.movie_id      = mi.movie_id
          AND n.id             = ci.person_id
          AND it.id            = mi.info_type_id
          AND t.production_year > %s;
    """,

    # ── Year sweep range ────────────────────────────────────────────────
    "range": {
        "start": 1880,
        "end":   2020,
    },

    # Coarse probe step (years between each initial sample)
    "step": 1,

    "output_dir": os.path.join(SCRIPT_DIR, "output"),
}


# ═════════════════════════════════════════════
# Database helpers
# ═════════════════════════════════════════════

def connect():
    cfg = CONFIG["db"]
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        database=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
    )


def get_plan(cursor, year: int) -> dict:
    """Execute EXPLAIN and return the parsed plan dict."""
    cursor.execute(CONFIG["query"], (year,))
    result = cursor.fetchone()
    # psycopg2 returns the JSON already parsed when the column is JSON type
    raw = result[0]
    if isinstance(raw, list):
        raw = raw[0]
    return raw.get("Plan", raw)


def get_hash(plan: dict) -> str:
    return structural_hash(plan)


# ═════════════════════════════════════════════
# Binary search for exact switch year
# ═════════════════════════════════════════════

def refine_switch(cursor, lo: int, hi: int, lo_hash: str) -> tuple[int, dict, int]:
    """
    Binary-search between lo and hi (exclusive) to find the first year
    whose plan hash differs from lo_hash.

    Returns (switch_year, new_plan, probe_count).
    """
    iterations = 0

    while hi - lo > 1:
        mid = (lo + hi) // 2
        plan = get_plan(cursor, mid)
        h    = get_hash(plan)
        iterations += 1

        if h == lo_hash:
            lo = mid
        else:
            hi = mid

    new_plan = get_plan(cursor, hi)
    return hi, new_plan, iterations


# ═════════════════════════════════════════════
# Persist helpers
# ═════════════════════════════════════════════

def save_plan(plan: dict, filename: str) -> None:
    out_dir = CONFIG["output_dir"]
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, filename)
    with open(path, "w") as f:
        json.dump([{"Plan": plan}], f, indent=2)


def save_report(switches: list, phases: list, total_probes: int) -> str:
    out_dir = CONFIG["output_dir"]
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "query2_switch_report.txt")

    with open(path, "w") as f:
        f.write("PLAN SWITCH REPORT — IMDB JOB Query 2\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 50 + "\n\n")

        f.write(f"Switch points found: {len(switches)}\n\n")

        for i, sw in enumerate(switches, 1):
            f.write(f"Switch #{i}\n")
            f.write(f"  Year          : {sw['year']}\n")
            f.write(f"  From hash     : {sw['from_hash']}\n")
            f.write(f"  To hash       : {sw['to_hash']}\n")
            f.write(f"  Binary-search : {sw['iterations']} probes\n\n")

        f.write(f"Total probes: {total_probes}\n\n")
        f.write("─" * 50 + "\n")
        f.write("Stable plan phases\n\n")

        for i, ph in enumerate(phases, 1):
            f.write(f"Phase {i}: year {ph['start']} → {ph.get('end', '?')}  "
                    f"hash={ph['hash'][:10]}\n")

    return path


# ═════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════

def main():
    cfg   = CONFIG
    start = cfg["range"]["start"]
    end   = cfg["range"]["end"]
    step  = cfg["step"]

    conn = connect()
    cur  = conn.cursor()

    print("\n" + "=" * 50)
    print("PLAN SWITCH FINDER — IMDB JOB Query 2")
    print("=" * 50 + "\n")
    print(f"Sweeping production_year {start} → {end}  (step={step})\n")

    prev_hash  = None
    prev_year  = None
    phases     = []
    switches   = []
    total_probes = 0

    year = start

    while year <= end:

        plan = get_plan(cur, year)
        h    = get_hash(plan)
        total_probes += 1

        print(f"  year={year:4d}  hash={h[:10]}")

        if prev_hash is None:
            # first probe — start first phase
            phases.append({"start": year, "hash": h, "plan": plan})

        elif h != prev_hash:
            print(f"\n  ↳ Switch detected between {prev_year} and {year}")

            sw_year, new_plan, iters = refine_switch(cur, prev_year, year, prev_hash)
            total_probes += iters

            print(f"  ↳ Exact switch at year={sw_year}\n")

            # close previous phase
            phases[-1]["end"] = sw_year - 1

            switches.append({
                "year":       sw_year,
                "from_hash":  prev_hash,
                "to_hash":    get_hash(new_plan),
                "iterations": iters,
            })

            phases.append({
                "start": sw_year,
                "hash":  get_hash(new_plan),
                "plan":  new_plan,
            })

        prev_hash = h
        prev_year = year
        year += step

    # close last phase
    phases[-1]["end"] = prev_year

    cur.close()
    conn.close()

    # ── Summary ──────────────────────────────────────────────────────────
    print("\n" + "=" * 50)
    print(f"Switch points found : {len(switches)}")
    for i, sw in enumerate(switches, 1):
        print(f"  Switch #{i} at production_year = {sw['year']}")
    print(f"Total probes        : {total_probes}")
    print("=" * 50 + "\n")

    # ── Save plans & report ──────────────────────────────────────────────
    for i, ph in enumerate(phases, 1):
        save_plan(ph["plan"], f"query2_phase_{i}_plan.json")

    report_path = save_report(switches, phases, total_probes)

    print(f"Plans  → {CONFIG['output_dir']}/query2_phase_*_plan.json")
    print(f"Report → {report_path}\n")


if __name__ == "__main__":
    main()
