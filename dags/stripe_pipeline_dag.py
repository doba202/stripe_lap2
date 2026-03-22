from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from services.pipeline.stripe_airflow import (
    start,
    delete_all_resources,
    load_all_data,
    end,
)


with DAG(
    dag_id="stripe_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    start_task = PythonOperator(
        task_id="start",
        python_callable=start,
    )

    delete_task = PythonOperator(
        task_id="delete",
        python_callable=delete_all_resources,
    )

    load_task = PythonOperator(
        task_id="loader",
        python_callable=load_all_data,
    )

    end_task = PythonOperator(
        task_id="end",
        python_callable=end,
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="stripe_test_dag",
        wait_for_completion=False,
    )

    start_task >> delete_task >> load_task >> end_task >> trigger_dbt
