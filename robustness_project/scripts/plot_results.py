import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("results/robustness_data.csv")

queries = df["query"].unique()

for q in queries:

    sub = df[df["query"] == q]

    sub = sub.sort_values("parameter")

    plt.figure()

    plt.plot(sub["parameter"],sub["optimizer_runtime"],label="Optimizer")

    plt.plot(sub["parameter"],sub["nested_runtime"],label="Nested Loop")

    plt.plot(sub["parameter"],sub["hash_runtime"],label="Hash Join")

    plt.xlabel("Predicate Value")

    plt.ylabel("Runtime (ms)")

    plt.title(q + " Runtime vs Selectivity")

    plt.legend()

    plt.savefig(f"plots/{q}_plan_comparison.png")



    plt.figure()

    plt.plot(sub["parameter"],sub["optimizer_loss"])

    plt.xlabel("Predicate Value")

    plt.ylabel("Optimizer Loss (ms)")

    plt.title(q + " Optimizer Loss")

    plt.savefig(f"plots/{q}_optimizer_loss.png")


print("Plots generated.")
