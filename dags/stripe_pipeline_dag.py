from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.api.common.trigger_dag import trigger_dag

from services.pipeline.stripe_airflow import (
    start,
    process_all_resources,
    end,
    get_run_mode,
)


def _end(**context):
    end(**context)  # logic end gốc

    # Không trigger dbt khi chạy init (full load)
    mode = get_run_mode(context, default="daily")
    if mode != "init":
        trigger_dag(dag_id="stripe_dbt_pipeline")


with DAG(
    dag_id="stripe_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    start_task   = PythonOperator(task_id="start",   python_callable=start)
    process_task = PythonOperator(task_id="process", python_callable=process_all_resources)
    end_task     = PythonOperator(task_id="end",     python_callable=_end)

    start_task >> process_task >> end_task
