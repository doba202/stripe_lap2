from .client import get_client,get_load_job_config
import pandas as pd
from datetime import datetime
import json
def insert_raw2(table_name, records, schema):
    client = get_client()
    rows = []
    for r in records:
        row = {}
        for field in schema:
            name = field["name"]
            field_type = field.get("type")
            mode = field.get("mode", "NULLABLE")

            value = r.get(name)
            # REPEATED
            if mode == "REPEATED":
                if value is None:
                    row[name] = []
                elif isinstance(value, list):
                    row[name] = [json.dumps(v, default=str) for v in value]
                else:

                    row[name] = [json.dumps(value, default=str)]
                continue

            # JSON
            if field_type == "JSON":
                try:
                    row[name] = json.dumps(value, default=str) if value is not None else None
                except Exception as e:
                    print("JSON serialize error:", e)
                    row[name] = None
                continue

            #  default
            row[name] = value if value is not None else None
        rows.append(row)
    df = pd.DataFrame(rows)
    print(f"Loading {len(df)} rows into {table_name}")

    job = client.load_table_from_dataframe(
        df,
        table_name,
        job_config=get_load_job_config("WRITE_APPEND")
    )

    job.result()

    print("Load completed")

    # def write_append(df, table_name):
    #     client = get_client()
    #     job = client.load_table_from_dataframe(
    #         df,
    #         table_name,
    #         job_config=get_query_job_config(table_name,"WRITE_TRUNCATE")
    #     )
    #     job.result()