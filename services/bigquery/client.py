from google.cloud import bigquery

def get_client(project_id: str = None):
    return bigquery.Client(project=project_id)

def get_load_job_config(write_mode, schema):
    return bigquery.LoadJobConfig(
        write_disposition=write_mode,
        schema=[
            bigquery.SchemaField(
                field["name"],
                field["type"],
                mode=field.get("mode", "NULLABLE")
            )
            for field in schema
        ]
    )