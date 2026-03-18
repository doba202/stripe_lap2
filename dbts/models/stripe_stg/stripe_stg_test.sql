select to_json(struct(
    1 as id,
    "John" as name,
    struct(
        30 as age,
        "Hanoi" as city
    ) as profile,
    ["sql", "dbt", "airflow"] as skills
)) as json_data