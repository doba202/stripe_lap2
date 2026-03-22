from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def test_start(**context):
    print("=== Test DAG triggered successfully ===")
    print(f"Triggered by: {context.get('dag_run').conf if context.get('dag_run') else 'manual'}")


def test_end(**context):
    print("=== Test DAG completed ===")


with DAG(
    dag_id="stripe_test_dag",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    start = PythonOperator(
        task_id="test_start",
        python_callable=test_start,
    )

    end = PythonOperator(
        task_id="test_end",
        python_callable=test_end,
    )

    start >> end
