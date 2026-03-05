import csv
import psycopg2
from switch_finder_ananya import binary_switch, plan_hash

conn = psycopg2.connect(
    dbname="imdb_job",
    user="postgres",
    password="postgrespassword",
    host="localhost"
)

cursor = conn.cursor()


def runtime(query):

    cursor.execute(f"EXPLAIN (ANALYZE, FORMAT JSON) {query}")
    result = cursor.fetchone()[0][0]

    return result["Execution Time"]


def forced_runtime(query, mode):

    if mode == "nested":

        cursor.execute("SET enable_hashjoin=off")
        cursor.execute("SET enable_mergejoin=off")
        cursor.execute("SET enable_nestloop=on")

    elif mode == "hash":

        cursor.execute("SET enable_nestloop=off")
        cursor.execute("SET enable_mergejoin=off")
        cursor.execute("SET enable_hashjoin=on")

    elif mode == "merge":

        cursor.execute("SET enable_hashjoin=off")
        cursor.execute("SET enable_nestloop=off")
        cursor.execute("SET enable_mergejoin=on")

    t = runtime(query)

    cursor.execute("RESET ALL")

    return t


queries = {

"query_cast_name":

"""
SELECT COUNT(*)
FROM title t
JOIN cast_info c ON t.tconst = c.tconst
JOIN name n ON c.nconst = n.nconst
WHERE t.startyear >= {};
""",

"query_with_ratings":

"""
SELECT COUNT(*)
FROM title t
JOIN cast_info c ON t.tconst = c.tconst
JOIN name n ON c.nconst = n.nconst
JOIN ratings r ON t.tconst = r.tconst
WHERE t.startyear >= {};
""",

"query_votes_selectivity":

"""
SELECT COUNT(*)
FROM title t
JOIN ratings r ON t.tconst = r.tconst
WHERE r.numvotes >= {};
"""
}


results = []

for name, query_template in queries.items():

    print("\nRunning:", name)

    # choose predicate sweep range
    if "votes" in name:
        params = range(0,20000,2000)
    else:
        params = range(1950,2025,5)

    for x in params:

        query = query_template.format(x)

        print("Parameter:", x)

        try:

            opt = runtime(query)
            nested = forced_runtime(query,"nested")
            hashj = forced_runtime(query,"hash")

            best = min(nested,hashj)

            loss = opt - best

            results.append((name,x,opt,nested,hashj,best,loss))

            print(
                "opt:",round(opt,2),
                "nested:",round(nested,2),
                "hash:",round(hashj,2)
            )

        except Exception as e:

            print("Skipped:",e)



with open("results/robustness_data.csv","w") as f:

    writer = csv.writer(f)

    writer.writerow([
        "query",
        "parameter",
        "optimizer_runtime",
        "nested_runtime",
        "hash_runtime",
        "best_runtime",
        "optimizer_loss"
    ])

    for r in results:
        writer.writerow(r)


print("\nExperiment finished. Results saved.")

