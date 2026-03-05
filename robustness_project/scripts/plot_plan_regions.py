import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("results/runtime_data.csv")

queries = df["query"].unique()

for q in queries:

    sub = df[df["query"] == q].copy()

    # convert plan hash to plan IDs
    sub["plan_id"] = sub["plan_hash"].astype("category").cat.codes

    plt.figure()

    for pid in sub["plan_id"].unique():

        region = sub[sub["plan_id"] == pid]

        plt.scatter(
            region["parameter"],
            region["runtime_ms"],
            label=f"Plan {pid}"
        )

    plt.plot(sub["parameter"], sub["runtime_ms"], linestyle="--")

    plt.xlabel("Predicate Value")
    plt.ylabel("Runtime (ms)")
    plt.title(q + " Runtime vs Selectivity")
    plt.legend()

    plt.savefig(f"plots/{q}_runtime_regions.png")

print("Plan region plots saved.")
