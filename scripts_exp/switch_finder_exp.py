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
    ).hexdigest()


def find_switch(query_template, start=1950, end=2025):

    prev_hash = None

    for x in range(start, end):

        query = query_template.format(x)

        h = plan_hash(query)

        if prev_hash and h != prev_hash:

            print("Optimizer switch detected between:", x-1, "and", x)

            return x

        prev_hash = h

    return None


# load query
with open("query.sql") as f:
    query_template = f.read()


S = find_switch(query_template)

if S:

    with open("results/switch_point.txt", "w") as f:
        f.write(str(S))

    print("Switch point saved:", S)

else:

    print("No switch found")
