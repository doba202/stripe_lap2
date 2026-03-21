from .client import get_client, get_load_job_config

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