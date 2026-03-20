from google.cloud import bigquery

def get_client(project_id: str = None):
    return bigquery.Client(project=project_id)

def get_load_job_config(write_mode):
    return bigquery.LoadJobConfig(
        write_disposition=write_mode
    )