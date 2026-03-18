from .client import get_client
from datetime import datetime
import json
def insert_raw(table_name, records, schema):
    client = get_client()
    rows_to_insert = []

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

        rows_to_insert.append(row)

    print("rows_to_insert", rows_to_insert)
    print(f"Inserting {len(rows_to_insert)} rows into {table_name}")

    errors = client.insert_rows_json(table_name, rows_to_insert)

    if errors:
        print("Insert errors:", errors)
        raise Exception(errors)