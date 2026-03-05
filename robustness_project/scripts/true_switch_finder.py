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


# ---------------------------
# runtime measurement
# ---------------------------

def runtime(query):

    cursor.execute(f"EXPLAIN (ANALYZE, FORMAT JSON) {query}")
    return cursor.fetchone()[0][0]["Execution Time"]


# ---------------------------
# forced plans
# ---------------------------

def force_nested(query):

    cursor.execute("SET enable_hashjoin=off")
    cursor.execute("SET enable_mergejoin=off")
    cursor.execute("SET enable_nestloop=on")

    t = runtime(query)

    cursor.execute("RESET ALL")

    return t


def force_hash(query):

    cursor.execute("SET enable_nestloop=off")
    cursor.execute("SET enable_mergejoin=off")
    cursor.execute("SET enable_hashjoin=on")

    t = runtime(query)

    cursor.execute("RESET ALL")

    return t


# ---------------------------
# plan hashing
# ---------------------------

def plan_hash(query):

    cursor.execute(f"EXPLAIN (FORMAT JSON) {query}")
    plan = cursor.fetchone()[0][0]["Plan"]

    plan_str = json.dumps(plan["Node Type"])

    return hashlib.md5(plan_str.encode()).hexdigest()


# ---------------------------
# find optimizer switch
# ---------------------------

def find_optimizer_switch(query_template, start=1950, end=2025):

    prev_hash = None

    for x in range(start, end):

        query = query_template.format(x)

        h = plan_hash(query)

        if prev_hash and h != prev_hash:

            print("\nOptimizer switch detected between:", x-1, "and", x)

            return x

        prev_hash = h

    return None


# ---------------------------
# query template
# ---------------------------

query_template = """
SELECT COUNT(*)
FROM title t
JOIN cast_info c ON t.tconst = c.tconst
JOIN name n ON c.nconst = n.nconst
WHERE t.startyear >= {};
"""


# ---------------------------
# main logic
# ---------------------------

S = find_optimizer_switch(query_template)

if S is None:

    print("No optimizer switch found.")
    exit()


print("\nTesting around optimizer switch:", S)

for x in [S-1, S]:

    query = query_template.format(x)

    nested = force_nested(query)
    hashj = force_hash(query)

    print("\nPredicate:", x)
    print("Nested runtime:", nested)
    print("Hash runtime:", hashj)
