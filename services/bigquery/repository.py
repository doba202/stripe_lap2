from services.bigquery.client import get_client
from services.common.config import DATASET, PROJECT_ID

def get_table_name(resource):
    return f"{PROJECT_ID}.{DATASET}.stripe_raw_{resource}"

def delete_all(table_name):
    client = get_client()

    query = f"DELETE FROM `{table_name}` WHERE TRUE"

    print(" [DELETE ALL]")
    print("[QUERY]", query)

    job = client.query(query)
    job.result()

    print(f"[DELETE DONE] {table_name}")

def delete_by_time_range(table_name, start_ts, end_ts, time_field):
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

    print(f"[DELETE RANGE] {start_ts} → {end_ts}")
    print(f"[TIME FIELD] {time_field}")
    print("[QUERY]", query)

    job = client.query(query)
    job.result()

    print(f"[DELETE DONE] {table_name}")