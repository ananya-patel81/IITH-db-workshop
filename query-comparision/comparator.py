"""
comparator.py — PostgreSQL EXPLAIN plan structural comparator
Adapted for IMDB JOB benchmark (originally built for TPC-H).

Usage:
    python comparator.py                              # compares query1.json vs query2.json
    python comparator.py plan_a.json plan_b.json      # custom files
    python comparator.py query1.json query2.json --strict   # treat literal differences as distinct
"""

import json
import sys
import os
import re
import hashlib
import argparse
from datetime import datetime

# ─────────────────────────────────────────────
# Structural keys used for plan comparison
# ─────────────────────────────────────────────

STRUCTURAL_KEYS = {
    "Node Type",
    "Join Type",
    "Index Name",
    "Index Cond",
    "Filter",
    "Hash Cond",
    "Merge Cond",
    "Relation Name",
    "Alias",
    "Scan Direction",
    "Strategy",
    "Parent Relationship",
    "Subplan Name",
    "Sort Key",
    "Group Key",
    "Recheck Cond",
}


# ─────────────────────────────────────────────
# Normalize literal values in condition strings
# e.g.  "t.production_year >= 1990"  →  "t.production_year >= ?"
# ─────────────────────────────────────────────

def normalize_literals(value: str) -> str:
    if not isinstance(value, str):
        return value
    # Replace integer / float literals
    value = re.sub(r"\b\d+(\.\d+)?\b", "?", value)
    # Replace quoted string literals
    value = re.sub(r"'[^']*'", "'?'", value)
    return value


# ─────────────────────────────────────────────
# Strip a plan node down to structural keys only
# ─────────────────────────────────────────────

def strip_node(node: dict, strict: bool = False) -> dict:
    result = {}
    for key in STRUCTURAL_KEYS:
        if key in node:
            val = node[key]
            if not strict and isinstance(val, str):
                val = normalize_literals(val)
            result[key] = val

    if "Plans" in node:
        result["Plans"] = [strip_node(child, strict) for child in node["Plans"]]

    return result


# ─────────────────────────────────────────────
# Load a plan from a JSON file
# Handles both raw list wrapper and bare dict
# ─────────────────────────────────────────────

def load_plan(path: str) -> dict:
    with open(path) as f:
        data = json.load(f)

    # EXPLAIN FORMAT JSON wraps in a list: [{"Plan": {...}}]
    if isinstance(data, list):
        data = data[0]

    return data.get("Plan", data)


# ─────────────────────────────────────────────
# Compute a structural hash of a plan
# ─────────────────────────────────────────────

def structural_hash(plan: dict, strict: bool = False) -> str:
    stripped = strip_node(plan, strict)
    serialized = json.dumps(stripped, sort_keys=True)
    return hashlib.md5(serialized.encode()).hexdigest()


# ─────────────────────────────────────────────
# Pretty-print a plan tree
# ─────────────────────────────────────────────

def plan_summary(node: dict, indent: int = 0) -> list:
    lines = []
    prefix = "  " * indent
    node_type = node.get("Node Type", "Unknown")
    relation = node.get("Relation Name", "")
    index = node.get("Index Name", "")
    join = node.get("Join Type", "")

    desc = node_type
    if relation:
        desc += f" on {relation}"
    if index:
        desc += f" [{index}]"
    if join:
        desc += f" ({join} join)"

    lines.append(f"{prefix}→ {desc}")

    for child in node.get("Plans", []):
        lines.extend(plan_summary(child, indent + 1))

    return lines


# ─────────────────────────────────────────────
# Core comparison logic
# ─────────────────────────────────────────────

def compare_plans(plan1: dict, plan2: dict, strict: bool = False) -> dict:
    h1 = structural_hash(plan1, strict)
    h2 = structural_hash(plan2, strict)
    identical = h1 == h2

    return {
        "identical": identical,
        "hash1": h1,
        "hash2": h2,
        "summary1": plan_summary(plan1),
        "summary2": plan_summary(plan2),
    }


# ─────────────────────────────────────────────
# Format and write the comparison report
# ─────────────────────────────────────────────

def format_report(result: dict, file1: str, file2: str, strict: bool) -> str:
    lines = []
    lines.append("=" * 60)
    lines.append("PLAN COMPARISON REPORT")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Mode: {'strict' if strict else 'normalized (literals ignored)'}")
    lines.append("=" * 60)
    lines.append("")

    lines.append(f"Plan A: {file1}")
    lines.append(f"Plan B: {file2}")
    lines.append("")

    status = "✓ IDENTICAL" if result["identical"] else "✗ DIFFERENT"
    lines.append(f"Result: {status}")
    lines.append("")

    lines.append(f"Hash A: {result['hash1']}")
    lines.append(f"Hash B: {result['hash2']}")
    lines.append("")

    lines.append("─" * 40)
    lines.append("Plan A Structure:")
    lines.extend(result["summary1"])
    lines.append("")

    lines.append("─" * 40)
    lines.append("Plan B Structure:")
    lines.extend(result["summary2"])
    lines.append("")

    if not result["identical"]:
        lines.append("─" * 40)
        lines.append("NOTE: Plans differ structurally.")
        if not strict:
            lines.append("      Run with --strict to also catch literal value changes.")

    return "\n".join(lines)


# ─────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Compare two PostgreSQL EXPLAIN JSON plans")
    parser.add_argument("plan1", nargs="?", default="query1.json")
    parser.add_argument("plan2", nargs="?", default="query2.json")
    parser.add_argument("--strict", action="store_true",
                        help="Treat literal value differences as structural differences")
    args = parser.parse_args()

    for path in [args.plan1, args.plan2]:
        if not os.path.exists(path):
            print(f"Error: file not found: {path}")
            sys.exit(1)

    plan1 = load_plan(args.plan1)
    plan2 = load_plan(args.plan2)

    result = compare_plans(plan1, plan2, strict=args.strict)
    report = format_report(result, args.plan1, args.plan2, args.strict)

    print(report)

    out_path = os.path.join(os.path.dirname(args.plan1), "output.txt")
    with open(out_path, "w") as f:
        f.write(report)
    print(f"\nReport saved to: {out_path}")


if __name__ == "__main__":
    main()
