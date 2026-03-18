from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

task = BigQueryInsertJobOperator(
    task_id="test_bq",
    configuration={
        "query": {
            "query": "SELECT 1",
            "useLegacySql": False,
        }
    },
)
with DAG(
    dag_id="dbt_full_refresh_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbts && dbt deps"
    )

    dbt_run = BashOperator(
        task_id="dbt_full_refresh",
        bash_command="cd /opt/airflow/dbts && dbt run --full-refresh"
    )

    dbt_deps >> dbt_run