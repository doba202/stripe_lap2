from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.api.common.trigger_dag import trigger_dag
from services.common.config import STRIPE_RESOURCES
from services.pipeline.stripe_airflow import (
    start,
    process_delete_insert,
    end,
    get_run_mode,
)


def _end(**context):
    end(**context)

    mode = get_run_mode(context, default="daily")
    if mode != "init":
        trigger_dag(dag_id="stripe_dbt_pipeline")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # Retry config
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    # Timeout cho mỗi task
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="stripe_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="30 0 * * *",
    catchup=False,
) as dag:

    start_task = PythonOperator(task_id="start", python_callable=start)
    end_task   = PythonOperator(task_id="end",   python_callable=_end)

    for resource in STRIPE_RESOURCES:
        process = PythonOperator(
            task_id=f"process_{resource}",
            python_callable=process_delete_insert,
            op_args=[resource],
        )
        start_task >> process >> end_task
