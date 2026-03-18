import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.python import PythonOperator



schedule_interval = Variable.get("schedule_interval", default_var='@daily')
execution_timeout_minutes = Variable.get("execution_timeout_minutes", default_var=30)


with DAG(
    dag_id="stripe_dbt_pipeline",
    catchup=False,
    default_args={
        'owner': 'stripe',
        'start_date': datetime(2026, 1, 1),
        "pool": "default_pool",
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        'execution_timeout': timedelta(minutes=int(execution_timeout_minutes))
    },
    schedule_interval=schedule_interval
) as dag:

    # ===== Helper =====
    def _get_project_dir():
        dag_file_path = os.path.dirname(os.path.realpath(__file__))
        return os.path.join(dag_file_path, '../dbts')


    def _dbt_cmd(cmd):
        return (
            f"cd {os.path.abspath(_get_project_dir())} && "
            f"{cmd}"
        )

    # ===== Tasks =====

    start = BashOperator(
        task_id='start',
        bash_command=_dbt_cmd("dbt clean && dbt deps"),
    )

    dbt_source_freshness = BashOperator(
        task_id='dbt_source_freshness',
        bash_command=_dbt_cmd("dbt source freshness --select source:stripe_stg"),
    )

    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=_dbt_cmd("dbt run --select stripe_stg"),
    )

    start >> dbt_source_freshness >> dbt_run_staging
