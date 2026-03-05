
import psycopg2
import csv

conn = psycopg2.connect(
    dbname="imdb_job",
    user="postgres",
    password="postgrespassword",
    host="localhost"
)

cursor = conn.cursor()


def runtime(query):

    cursor.execute(f"EXPLAIN (ANALYZE, FORMAT JSON) {query}")

    return cursor.fetchone()[0][0]["Execution Time"]


def force_nested(query):

    cursor.execute("SET enable_hashjoin=off")
    cursor.execute("SET enable_mergejoin=off")

    t = runtime(query)

    cursor.execute("RESET ALL")

    return t


def force_hash(query):

    cursor.execute("SET enable_nestloop=off")
    cursor.execute("SET enable_mergejoin=off")

    t = runtime(query)

    cursor.execute("RESET ALL")

    return t


# read query
with open("query.sql") as f:
    query_template = f.read()

# read switch point
with open("results/switch_point.txt") as f:
    S = int(f.read())

print("Testing around optimizer switch:", S)

rows = []

for x in [S-1, S]:

    query = query_template.format(x)

    optimizer = runtime(query)

    nested = force_nested(query)

    hashj = force_hash(query)

    rows.append((x, optimizer, nested, hashj))

    print("\nPredicate:", x)
    print("Optimizer:", optimizer)
    print("Nested:", nested)
    print("Hash:", hashj)


with open("results/comparison_results.csv", "w") as f:

    writer = csv.writer(f)

    writer.writerow([
        "predicate",
        "optimizer_runtime",
        "nested_runtime",
        "hash_runtime"
    ])

    for r in rows:
        writer.writerow(r)


print("\nResults saved.")
