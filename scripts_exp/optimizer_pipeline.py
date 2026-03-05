import psycopg2
import json
import hashlib
import csv
import os


conn = psycopg2.connect(
    dbname="imdb_job",
    user="postgres",
    password="postgrespassword",
    host="localhost"
)

cursor = conn.cursor()

os.makedirs("results", exist_ok=True)


# -----------------------------
# PLAN STRUCTURE HASH
# -----------------------------

def extract_structure(plan):

    node = {
        "Node Type": plan.get("Node Type"),
        "Join Type": plan.get("Join Type"),
        "Relation": plan.get("Relation Name"),
        "Index": plan.get("Index Name"),
        "Plans": []
    }

    if "Plans" in plan:
        for child in plan["Plans"]:
            node["Plans"].append(extract_structure(child))

    return node


def plan_hash(query):

    cursor.execute(f"EXPLAIN (FORMAT JSON) {query}")

    plan = cursor.fetchone()[0][0]["Plan"]

    structure = extract_structure(plan)

    return hashlib.md5(
        json.dumps(structure, sort_keys=True).encode()
    ).hexdigest(), plan


# -----------------------------
# SWITCH POINT DETECTION
# -----------------------------

def find_optimizer_switch(query_template, start=1900, end=2025):

    prev_hash = None

    for x in range(start, end):

        query = query_template.format(x)

        h, plan = plan_hash(query)

        if prev_hash and h != prev_hash:

            print("Optimizer switch between", x-1, "and", x)

            return x

        prev_hash = h

    return None


# -----------------------------
# HINT EXTRACTION
# -----------------------------

def get_alias(node):

    if "Alias" in node:
        return node["Alias"]

    if "Relation Name" in node:
        return node["Relation Name"]

    if "Plans" in node:
        return get_alias(node["Plans"][0])

    return None


def extract_hints(plan, hints):

    node_type = plan.get("Node Type")

    if node_type in ["Nested Loop", "Hash Join", "Merge Join"]:

        left = plan["Plans"][0]
        right = plan["Plans"][1]

        left_alias = get_alias(left)
        right_alias = get_alias(right)

        if node_type == "Nested Loop":
            hints.append(f"NestLoop({left_alias} {right_alias})")

        elif node_type == "Hash Join":
            hints.append(f"HashJoin({left_alias} {right_alias})")

        elif node_type == "Merge Join":
            hints.append(f"MergeJoin({left_alias} {right_alias})")

    if "Plans" in plan:
        for child in plan["Plans"]:
            extract_hints(child, hints)


def build_hint(plan):

    hints = []

    extract_hints(plan, hints)

    hint_string = "/*+\n"

    for h in hints:
        hint_string += h + "\n"

    hint_string += "*/"

    return hint_string


# -----------------------------
# EXECUTION TIME
# -----------------------------

def runtime(query):

    cursor.execute(f"EXPLAIN (ANALYZE, FORMAT JSON) {query}")

    return cursor.fetchone()[0][0]["Execution Time"]


# -----------------------------
# TRUE SWITCH CHECK
# -----------------------------

def test_forced_plans(query_template, S, hintA, hintB):

    rows = []

    for x in [S-1, S]:

        query = query_template.format(x)

        optimizer_time = runtime(query)

        forcedA = runtime(hintA + query)
        forcedB = runtime(hintB + query)

        rows.append((x, optimizer_time, forcedA, forcedB))

        print("\nPredicate:", x)
        print("Optimizer:", optimizer_time)
        print("PlanA forced:", forcedA)
        print("PlanB forced:", forcedB)

    return rows


# -----------------------------
# MAIN PIPELINE
# -----------------------------

with open("query.sql") as f:
    query_template = f.read()


print("\nFinding optimizer switch...")

S = find_optimizer_switch(query_template)

if not S:
    print("No optimizer switch detected")
    exit()


with open("results/optimizer_switch.txt", "w") as f:
    f.write(str(S))


print("\nExtracting hints...")

_, planA = plan_hash(query_template.format(S-1))
_, planB = plan_hash(query_template.format(S))

hintA = build_hint(planA)
hintB = build_hint(planB)


with open("results/planA_hint.sql", "w") as f:
    f.write(hintA)

with open("results/planB_hint.sql", "w") as f:
    f.write(hintB)


print("\nPlan A hint:\n", hintA)
print("\nPlan B hint:\n", hintB)


print("\nTesting forced plans...")

rows = test_forced_plans(query_template, S, hintA, hintB)


with open("results/forced_results.csv", "w") as f:

    writer = csv.writer(f)

    writer.writerow([
        "predicate",
        "optimizer_runtime",
        "planA_forced",
        "planB_forced"
    ])

    for r in rows:
        writer.writerow(r)


print("\nResults written to results/forced_results.csv")
