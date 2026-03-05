import psycopg2

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


with open("query.sql") as f:
    query_template = f.read()


with open("results/planA_hint.sql") as f:
    hintA = f.read()

with open("results/planB_hint.sql") as f:
    hintB = f.read()


with open("results/optimizer_switch.txt") as f:
    S = int(f.read())


window = 40

low = S - window
high = S + window

true_switch = None


while low <= high:

    mid = (low + high) // 2

    query = query_template.format(mid)

    runtimeA = runtime(hintA + query)
    runtimeB = runtime(hintB + query)

    print(
        "Predicate:", mid,
        "PlanA:", runtimeA,
        "PlanB:", runtimeB
    )

    if abs(runtimeA - runtimeB) < 1:

        true_switch = mid
        break

    if runtimeA < runtimeB:

        low = mid + 1

    else:

        high = mid - 1


print("\nTrue switch point:", true_switch)
