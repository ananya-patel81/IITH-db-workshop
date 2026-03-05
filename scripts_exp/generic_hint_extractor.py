import psycopg2
import json


conn = psycopg2.connect(
    dbname="imdb_job",
    user="postgres",
    password="postgrespassword",
    host="localhost"
)

cursor = conn.cursor()


def get_plan(query):

    cursor.execute(f"EXPLAIN (FORMAT JSON) {query}")
    plan = cursor.fetchone()[0][0]["Plan"]

    return plan


def get_alias(node):

    if "Alias" in node:
        return node["Alias"]

    if "Relation Name" in node:
        return node["Relation Name"]

    if "Plans" in node:
        return get_alias(node["Plans"][0])

    return None


def extract_join_hints(plan, hints):

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
            extract_join_hints(child, hints)


def extract_join_order(plan, order):

    if "Plans" not in plan:
        alias = get_alias(plan)
        if alias:
            order.append(alias)
        return

    for child in plan["Plans"]:
        extract_join_order(child, order)


def generate_hint(query):

    plan = get_plan(query)

    hints = []
    join_order = []

    extract_join_hints(plan, hints)
    extract_join_order(plan, join_order)

    hint_string = "/*+\n"

    if join_order:
        hint_string += "Leading(" + " ".join(join_order) + ")\n"

    for h in hints:
        hint_string += h + "\n"

    hint_string += "*/"

    return hint_string


def main():

    with open("query.sql") as f:
        query_template = f.read()

    predicate = input("Enter predicate value: ")

    query = query_template.format(predicate)

    hint = generate_hint(query)

    print("\nGenerated pg_hint_plan hints:\n")
    print(hint)


if __name__ == "__main__":
    main()
