import psycopg2
import matplotlib.pyplot as plt
import csv


conn = psycopg2.connect(
    dbname="imdb_job",
    user="postgres",
    password="postgrespassword",
    host="localhost"
)

cursor = conn.cursor()


# -----------------------------
# runtime helper
# -----------------------------

def runtime(query):

    cursor.execute(f"EXPLAIN (ANALYZE, FORMAT JSON) {query}")

    return cursor.fetchone()[0][0]["Execution Time"]


# -----------------------------
# load query
# -----------------------------

with open("query.sql") as f:
    query_template = f.read()


# -----------------------------
# load hints
# -----------------------------

with open("results/planA_hint.sql") as f:
    hintA = f.read()

with open("results/planB_hint.sql") as f:
    hintB = f.read()


# optimizer switch
with open("results/optimizer_switch.txt") as f:
    S = int(f.read())


# -----------------------------
# sweep predicate range
# -----------------------------

start = S - 40
end = S + 40


predicates = []
opt_runtime = []
planA_runtime = []
planB_runtime = []


for x in range(start, end):

    query = query_template.format(x)

    predicates.append(x)

    opt_runtime.append(runtime(query))

    planA_runtime.append(runtime(hintA + query))

    planB_runtime.append(runtime(hintB + query))


# -----------------------------
# save dataset
# -----------------------------

with open("results/runtime_sweep.csv", "w") as f:

    writer = csv.writer(f)

    writer.writerow([
        "predicate",
        "optimizer",
        "planA",
        "planB"
    ])

    for i in range(len(predicates)):
        writer.writerow([
            predicates[i],
            opt_runtime[i],
            planA_runtime[i],
            planB_runtime[i]
        ])


# -----------------------------
# detect true switch
# -----------------------------

true_switch = None

for i in range(len(predicates)):

    if planA_runtime[i] > planB_runtime[i]:
        true_switch = predicates[i]
        break


# -----------------------------
# plot
# -----------------------------

plt.figure(figsize=(10,6))

plt.plot(predicates, opt_runtime, label="Optimizer plan")
plt.plot(predicates, planA_runtime, label="Plan A forced")
plt.plot(predicates, planB_runtime, label="Plan B forced")


plt.axvline(S, linestyle="--", label="Optimizer switch")

if true_switch:
    plt.axvline(true_switch, linestyle=":", label="True switch")


plt.xlabel("Predicate value")
plt.ylabel("Runtime (ms)")
plt.title("Runtime vs Selectivity with Plan Regions")

plt.legend()

plt.savefig("results/runtime_vs_selectivity.png")

print("Plot saved to results/runtime_vs_selectivity.png")
