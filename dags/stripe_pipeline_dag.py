from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
from services.stripe.client import StripeClient
from services.stripe.config import STRIPE_RESOURCES,STRIPE_ACCOUNTS
from services.bigquery.loader import insert_raw
from services.bigquery.config import TABLE_CONFIG,DATASET,PROJECT_ID
from services.bigquery.repository import delete_by_time_range,delete_all,get_table_name
from services.common.time_window import build_time_window


def get_run_mode(context, default="daily"):
    dag_run = context.get("dag_run")

    if not dag_run:
        print(f"[MODE] No dag_run → default={default}")
        return default

    conf = dag_run.conf or {}

    mode = conf.get("run_mode", default)

    print(f"[MODE] {mode}")

    return mode

def transform_record(resource, record,open_id):
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

        if field_name == "open_id":
            row[field_name] = open_id
            continue

        if field_name == "created_at":
            created_ts = record.get("created")
            if created_ts:
                row[field_name] = datetime.utcfromtimestamp(created_ts).isoformat()
            else:
                row[field_name] = None
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

def start(**context):
    print("Start pipeline...")


def delete_task(resource, **context):


    config = TABLE_CONFIG[resource]

    table_name = get_table_name(resource)
    print('test',table_name,get_run_mode(context))
    print('test2',config)
    if get_run_mode(context) == "init":
        delete_all(table_name)
        return
    elif get_run_mode(context) == "daily":
        time_window = build_time_window(context)
        print('time_window',time_window["start"],time_window["end"])
        delete_by_time_range(
            table_name,
            time_window["start"],
            time_window["end"]
        )



def load_data(resource,**context):
    mode = get_run_mode(context)
    for account in STRIPE_ACCOUNTS:
        print(account)
        client = StripeClient(account["api_key"])
        print(f"Loading {resource}...")
        data = client.fetch(resource,mode)
        print("data_fetch",data)
        if not data:
            print(f"No data for {resource}")
            continue
        # normalize về list
        if isinstance(data, dict):
            data = [data]
        print(f"Fetched {len(data)} records")
        transformed = []
        for record in data:
            row = transform_record(resource, record,account["openid"])
            if row:
                transformed.append(row)
        if not transformed:
            print(f"No transformed data for {resource}")
            continue

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


    for resource in STRIPE_RESOURCES:
        delete = PythonOperator(
            task_id=f"delete_{resource}",
            python_callable=delete_task,
            op_args=[resource]
        )

        load = PythonOperator(
            task_id=f"load_{resource}",
            python_callable=load_data,
            op_args=[resource]
        )

        # flow: start → delete → load
        start_task >> delete >> load
