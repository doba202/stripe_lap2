from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from services.stripe.client import StripeClient
from services.stripe.config import STRIPE_RESOURCES
from services.bigquery.loader import insert_raw
from services.bigquery.config import TABLE_CONFIG,DATASET,PROJECT_ID
import json


client = StripeClient()


def transform_record(resource, record):
    config = TABLE_CONFIG.get(resource)
    if not config:
        return None
    print('record', record)
    schema = config["schema"]
    mode = config["mode"]

    now = datetime.utcnow().isoformat()
    row = {}

    for field in schema:
        field_name = field["name"]
        field_mode = field.get("mode", "NULLABLE")

        #  special field
        if field_name == "call_at":
            row[field_name] = now
            continue

        #  json mode
        if mode == "json" and field_name == "data_json":
            row[field_name] = record
            continue

        # lấy data từ record
        value = record.get(field_name)
        # xử lý REPEATED
        if field_mode == "REPEATED":
            row[field_name] = value if value is not None else []
        else:
            row[field_name] = value

    return row

def start():
    print("Start pipeline...")

def load_data(resource):
    print(f"Loading {resource}...")

    data = client.get_all(resource)
    if not data:
        print(f"No data for {resource}")
        return

    # 🔹 normalize về list
    if isinstance(data, dict):
        data = [data]

    print(f"Fetched {len(data)} records")

    transformed = []

    for record in data:
        print('recordvvvvvvvvvvvvv', record)
        row = transform_record(resource, record)
        print('rowvvvvvvvvvvvvvvvv',row)
        if row:
            transformed.append(row)

    if not transformed:
        print(f"No transformed data for {resource}")
        return

    print(f"Transformed {len(transformed)} records")
    print('transformed',transformed)
    insert_raw(
        table_name=f"{PROJECT_ID}.{DATASET}.stripe_raw_{resource}",
        records=transformed,
        schema=TABLE_CONFIG[resource]["schema"]
    )

    print(f"Inserted {len(transformed)} records into {resource}")



with DAG(
    dag_id="stripe_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    start_task = PythonOperator(
        task_id="start",
        python_callable=start
    )

    tasks = []

    for resource in STRIPE_RESOURCES:
        task = PythonOperator(
            task_id=f"load_{resource}",
            python_callable=load_data,
            op_args=[resource]
        )

        start_task >> task
        tasks.append(task)