from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from services.common.config import STRIPE_RESOURCES
from services.pipeline.stripe_airflow import delete_task, load_data, start


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

    for resource in STRIPE_RESOURCES:
        delete = PythonOperator(
            task_id=f"delete_{resource}",
            python_callable=delete_task,
            op_args=[resource],
        )

        load = PythonOperator(
            task_id=f"load_{resource}",
            python_callable=load_data,
            op_args=[resource],
        )

        start_task >> delete >> load
