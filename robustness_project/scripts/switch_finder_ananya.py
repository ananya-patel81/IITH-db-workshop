import psycopg2
import json
import hashlib

conn = psycopg2.connect(
    dbname="imdb_job",
    user="postgres",
    password="postgrespassword",
    host="localhost"
)

cursor = conn.cursor()


def extract_structure(plan):

    node = {
        "Node Type": plan.get("Node Type"),
        "Join Type": plan.get("Join Type"),
        "Relation Name": plan.get("Relation Name"),
        "Index Name": plan.get("Index Name"),
        "Strategy": plan.get("Strategy"),
        "Plans": []
    }

    if "Plans" in plan:
        for child in plan["Plans"]:
            node["Plans"].append(extract_structure(child))

    return node


def plan_hash(query):

    cursor.execute(f"EXPLAIN (FORMAT JSON) {query}")

    result = cursor.fetchone()[0][0]

    structure = extract_structure(result["Plan"])

    plan_string = json.dumps(structure, sort_keys=True)

    return hashlib.md5(plan_string.encode()).hexdigest()


def binary_switch(query_template, low=1950, high=2025):

    low_hash = plan_hash(query_template.format(low))
    high_hash = plan_hash(query_template.format(high))

    print("Plan at", low, ":", low_hash[:8])
    print("Plan at", high, ":", high_hash[:8])

    if low_hash == high_hash:
        print("No switch detected in range.")
        return None

    while high - low > 1:

        mid = (low + high) // 2

        mid_hash = plan_hash(query_template.format(mid))

        print("Testing", mid, "->", mid_hash[:8])

        if mid_hash == low_hash:
            low = mid
        else:
            high = mid

    return high
