"""
true-switch-point-finder/query3.py

For each switch point S found by switch-point-finder:
  - Auto-extracts pg_hint_plan hints from phase_before and phase_after plan JSONs
  - At year S:   forces Plan A (the old plan) → checks if optimizer would have used it
  - At year S-1: forces Plan B (the new plan) → checks if optimizer would have used it
  - Compares forced plan vs natural plan using structural hashing
  - Saves report showing what plan was chosen vs what was forced at each switch

Usage:
    python query3.py                 # reads switch-point-finder output/ directly
"""

import psycopg2
import json
import os
import sys
import re
from datetime import datetime

# ─────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────

SCRIPT_DIR     = os.path.dirname(os.path.realpath(__file__))
COMPARATOR_DIR = os.path.realpath(os.path.join(SCRIPT_DIR, "..", "query-comparision"))
SWITCH_OUT     = os.path.realpath(os.path.join(SCRIPT_DIR, "..", "switch-point-finder", "output"))

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

    # ── JOB Query 3 — title + movie_info + movie_keyword + keyword ────────
    # Based on JOB query 3c family. Joins title (2.5M), movie_info (14M),
    # movie_keyword (4.5M), keyword (134K). All four tables now have
    # indexes on movie_id and keyword_id. No cast_info so the optimizer
    # has more varied join order choices across the production_year range.
    "query": """
        {hint}
        EXPLAIN (FORMAT JSON)
        SELECT MIN(t.title)    AS movie_title,
               MIN(mi.info)   AS movie_info,
               MIN(k.keyword) AS movie_keyword
        FROM keyword       AS k,
             movie_info    AS mi,
             movie_keyword AS mk,
             title         AS t
        WHERE k.keyword LIKE '%%sequel%%'
          AND mi.info IN ('Sweden', 'Norway', 'Germany',
                          'Denmark', 'Swedish', 'Norwegian',
                          'German', 'USA', 'American')
          AND t.id            = mi.movie_id
          AND t.id            = mk.movie_id
          AND mk.movie_id     = mi.movie_id
          AND k.id            = mk.keyword_id
          AND t.production_year > %s;
    """,

    "output_dir": os.path.join(SCRIPT_DIR, "output"),
}


# ═════════════════════════════════════════════
# Database
# ═════════════════════════════════════════════

def connect():
    cfg = CONFIG["db"]
    return psycopg2.connect(
        host=cfg["host"], port=cfg["port"],
        database=cfg["database"],
        user=cfg["user"], password=cfg["password"],
    )


def get_plan(cursor, year: int, hint: str = "") -> dict:
    query = CONFIG["query"].format(hint=hint)
    cursor.execute(query, (year,))
    raw = cursor.fetchone()[0]
    if isinstance(raw, list):
        raw = raw[0]
    return raw.get("Plan", raw)


# ═════════════════════════════════════════════
# Hint extraction from EXPLAIN JSON
# ═════════════════════════════════════════════

SCAN_MAP = {
    "Seq Scan":         "SeqScan",
    "Index Scan":       "IndexScan",
    "Index Only Scan":  "IndexOnlyScan",
    "Bitmap Heap Scan": "BitmapScan",
    "Tid Scan":         "TidScan",
}

JOIN_MAP = {
    "Hash Join":   "HashJoin",
    "Nested Loop": "NestLoop",
    "Merge Join":  "MergeJoin",
}


def _aliases_under(node: dict) -> list:
    result = []
    if node.get("Node Type") in SCAN_MAP:
        alias = node.get("Alias") or node.get("Relation Name", "")
        if alias:
            result.append(alias)
    for child in node.get("Plans", []):
        result.extend(_aliases_under(child))
    return result


def _collect_hints(node: dict) -> list:
    hints = []
    for child in node.get("Plans", []):
        hints.extend(_collect_hints(child))
    ntype = node.get("Node Type", "")
    if ntype in JOIN_MAP:
        aliases = _aliases_under(node)
        if len(aliases) >= 2:
            hints.append(f"{JOIN_MAP[ntype]}({' '.join(aliases)})")
    if ntype in SCAN_MAP:
        alias = node.get("Alias") or node.get("Relation Name", "")
        if alias:
            hints.append(f"{SCAN_MAP[ntype]}({alias})")
    return hints


def extract_hint(plan: dict) -> str:
    parts, seen = [], set()
    for h in _collect_hints(plan):
        if h not in seen:
            parts.append(h)
            seen.add(h)
    return ("/*+ " + " ".join(parts) + " */") if parts else ""


# ═════════════════════════════════════════════
# Read switch-point-finder outputs
# ═════════════════════════════════════════════

def load_plan_file(path: str) -> dict:
    with open(path) as f:
        data = json.load(f)
    if isinstance(data, list):
        data = data[0]
    return data.get("Plan", data)


def read_switch_points() -> list[dict]:
    report = os.path.join(SWITCH_OUT, "query3_switch_report.txt")
    if not os.path.exists(report):
        raise FileNotFoundError(
            f"switch_report.txt not found at {SWITCH_OUT}\n"
            "Run switch-point-finder/query3.py first."
        )

    switch_years = []
    with open(report) as f:
        for line in f:
            m = re.search(r"Year\s*:\s*(\d+)", line)
            if m:
                switch_years.append(int(m.group(1)))

    if not switch_years:
        raise ValueError("No switch points found in switch_report.txt")

    switches = []
    for i, sw_year in enumerate(switch_years):
        pb = i + 1
        pa = i + 2
        path_before = os.path.join(SWITCH_OUT, f"query3_phase_{pb}_plan.json")
        path_after  = os.path.join(SWITCH_OUT, f"query3_phase_{pa}_plan.json")
        for p in [path_before, path_after]:
            if not os.path.exists(p):
                raise FileNotFoundError(f"Plan file missing: {p}")
        switches.append({
            "year":         sw_year,
            "phase_before": pb,
            "phase_after":  pa,
            "plan_before":  load_plan_file(path_before),
            "plan_after":   load_plan_file(path_after),
        })

    return switches


# ═════════════════════════════════════════════
# Save outputs
# ═════════════════════════════════════════════

def save_plan(plan: dict, filename: str) -> None:
    out_dir = CONFIG["output_dir"]
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, filename)
    with open(path, "w") as f:
        json.dump([{"Plan": plan}], f, indent=2)


def save_report(results: list[dict], switches: list[dict]) -> str:
    out_dir = CONFIG["output_dir"]
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "query3_forced_plan_report.txt")

    with open(path, "w") as f:
        f.write("FORCED PLAN REPORT — IMDB JOB Query 3\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 55 + "\n\n")

        for r in results:
            f.write(f"Switch #{r['switch_index']}  at  production_year = {r['switch_year']}\n")
            f.write("─" * 55 + "\n")

            f.write(f"  Phase {r['phase_before']} → Phase {r['phase_after']}\n\n")

            # A→ at S
            f.write(f"  [Force Plan A at year S={r['switch_year']}]\n")
            f.write(f"    Hint used     : {r['hint_a']}\n")
            f.write(f"    Natural plan  : {r['natural_hash_at_S'][:10]}\n")
            f.write(f"    Forced plan   : {r['forced_hash_A_at_S'][:10]}\n")
            match_a = "✓ match" if r['natural_hash_at_S'] == r['forced_hash_A_at_S'] else "✗ different"
            f.write(f"    Same as natural? {match_a}\n\n")

            # B→ at S-1
            f.write(f"  [Force Plan B at year S-1={r['switch_year'] - 1}]\n")
            f.write(f"    Hint used     : {r['hint_b']}\n")
            f.write(f"    Natural plan  : {r['natural_hash_at_S1'][:10]}\n")
            f.write(f"    Forced plan   : {r['forced_hash_B_at_S1'][:10]}\n")
            match_b = "✓ match" if r['natural_hash_at_S1'] == r['forced_hash_B_at_S1'] else "✗ different"
            f.write(f"    Same as natural? {match_b}\n\n")

            f.write("=" * 55 + "\n\n")

    return path


# ═════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════

def main():
    out_dir = CONFIG["output_dir"]
    os.makedirs(out_dir, exist_ok=True)

    print("\n" + "=" * 55)
    print("FORCED PLAN CHECKER — IMDB JOB Query 3")
    print("=" * 55 + "\n")

    switches = read_switch_points()
    print(f"Switch points loaded: {len(switches)}")
    for i, sw in enumerate(switches, 1):
        print(f"  Switch #{i} at year={sw['year']}  "
              f"(phase {sw['phase_before']} → {sw['phase_after']})")

    conn = connect()
    cur  = conn.cursor()
    cur.execute("LOAD 'pg_hint_plan';")
    cur.execute("SET pg_hint_plan.enable_hint = on;")

    results = []

    for i, sw in enumerate(switches, 1):
        S      = sw["year"]
        hint_a = extract_hint(sw["plan_before"])
        hint_b = extract_hint(sw["plan_after"])

        print(f"\n{'─'*55}")
        print(f"Switch #{i}  at  production_year = {S}")
        print(f"  Hint A (plan before S) : {hint_a or '(none)'}")
        print(f"  Hint B (plan after  S) : {hint_b or '(none)'}")
        print(f"{'─'*55}")

        # Natural plans at S and S-1
        natural_at_S  = get_plan(cur, S)
        natural_at_S1 = get_plan(cur, S - 1)

        # Force Plan A at S (old plan — optimizer has already switched away from it)
        forced_A_at_S = get_plan(cur, S, hint=hint_a)

        # Force Plan B at S-1 (new plan — optimizer hasn't switched to it yet)
        forced_B_at_S1 = get_plan(cur, S - 1, hint=hint_b)

        nat_hash_S  = structural_hash(natural_at_S)
        nat_hash_S1 = structural_hash(natural_at_S1)
        frc_hash_A  = structural_hash(forced_A_at_S)
        frc_hash_B  = structural_hash(forced_B_at_S1)

        print(f"\n  At year S={S}:")
        print(f"    Natural plan hash  : {nat_hash_S[:10]}")
        print(f"    Forced Plan A hash : {frc_hash_A[:10]}  "
              f"{'✓ same as natural' if nat_hash_S == frc_hash_A else '✗ differs from natural'}")

        print(f"\n  At year S-1={S-1}:")
        print(f"    Natural plan hash  : {nat_hash_S1[:10]}")
        print(f"    Forced Plan B hash : {frc_hash_B[:10]}  "
              f"{'✓ same as natural' if nat_hash_S1 == frc_hash_B else '✗ differs from natural'}")

        # Save forced plan JSONs
        save_plan(forced_A_at_S,  f"query3_switch{i}_forced_A_at_S{S}.json")
        save_plan(forced_B_at_S1, f"query3_switch{i}_forced_B_at_S{S-1}.json")

        results.append({
            "switch_index":        i,
            "switch_year":         S,
            "phase_before":        sw["phase_before"],
            "phase_after":         sw["phase_after"],
            "hint_a":              hint_a,
            "hint_b":              hint_b,
            "natural_hash_at_S":   nat_hash_S,
            "forced_hash_A_at_S":  frc_hash_A,
            "natural_hash_at_S1":  nat_hash_S1,
            "forced_hash_B_at_S1": frc_hash_B,
        })

    cur.close()
    conn.close()

    report_path = save_report(results, switches)

    print("\n" + "=" * 55)
    print("OUTPUTS")
    for fname in sorted(os.listdir(out_dir)):
        print(f"  {out_dir}/{fname}")
    print("=" * 55 + "\n")
    print(f"Report → {report_path}\n")


if __name__ == "__main__":
    main()
