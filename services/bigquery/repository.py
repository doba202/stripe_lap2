from services.bigquery.client import get_client, get_load_job_config
from services.common.config import DATASET, PROJECT_ID

def insert_raw(table_name, records, schema):
    client = get_client()

    # records = list dict (giữ nguyên)
    rows = [
        {field["name"]: r.get(field["name"]) for field in schema}
        for r in records
    ]

    job = client.load_table_from_json(
        rows,
        table_name,
        job_config=get_load_job_config("WRITE_APPEND", schema)
    )

    job.result()

    print("Load completed")

def get_table_name(resource):
    return f"{PROJECT_ID}.{DATASET}.stripe_raw_{resource}"

def delete_all(table_name, open_ids=None):
    client = get_client()

    if open_ids:
        open_ids_str = ", ".join([f"'{str(oid)}'" for oid in open_ids])
        query = f"DELETE FROM `{table_name}` WHERE open_id IN ({open_ids_str})"
        print(f" [DELETE INIT BY OPEN_IDS] {open_ids}")
    else:
        query = f"DELETE FROM `{table_name}` WHERE TRUE"
        print(" [DELETE ALL]")

    print("[QUERY]", query)

    job = client.query(query)
    job.result()

    print(f"[DELETE DONE] {table_name}")

def delete_by_time_range(table_name, start_ts, end_ts, time_field, open_ids=None):
    client = get_client()

    if start_ts is None or end_ts is None:
        raise ValueError("start_ts and end_ts are required")

    if not isinstance(start_ts, int) or not isinstance(end_ts, int):
        raise TypeError("start_ts and end_ts must be int")

    if start_ts > end_ts:
        raise ValueError("start_ts cannot be greater than end_ts")

    query = f"""
    DELETE FROM `{table_name}`
    WHERE {time_field} >= DATETIME(TIMESTAMP_SECONDS({start_ts}))
      AND {time_field} <= DATETIME(TIMESTAMP_SECONDS({end_ts}))
    """

    if open_ids:
        open_ids_str = ", ".join([f"'{str(oid)}'" for oid in open_ids])
        query += f"  AND open_id IN ({open_ids_str})\n    "
        print(f"[DELETE RANGE BY OPEN_IDS] {open_ids}")

    print(f"[DELETE RANGE] {start_ts} → {end_ts}")
    print(f"[TIME FIELD] {time_field}")
    print("[QUERY]", query)

    job = client.query(query)
    job.result()

    print(f"[DELETE DONE] {table_name}")