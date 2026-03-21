import pandas as pd
import json

def align_df_with_schema(df, schema):
    for field in schema:
        name = field["name"]
        field_type = field["type"]

        if name not in df.columns:
            continue

        if field_type == "DATETIME":
            df[name] = pd.to_datetime(df[name], errors="coerce")

        elif field_type == "INTEGER":
            df[name] = pd.to_numeric(df[name], errors="coerce").astype("Int64")

        elif field_type == "FLOAT":
            df[name] = pd.to_numeric(df[name], errors="coerce")

        elif field_type == "BOOLEAN":
            df[name] = df[name].astype("boolean")

        elif field_type == "JSON":
            
            df[name] = df[name].apply(
                lambda x: json.dumps(x, default=str) if x is not None else None
            ).astype("string")  # 🔥 FIX CHÍNH
            print('data_json',df["data_json"].map(type).unique())

        else:
            df[name] = df[name].astype("string")

    return df