from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

def get_client():
    hook = BigQueryHook()
    return hook.get_client()