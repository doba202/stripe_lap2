import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import sys
from services.common.time_window import build_time_window  # noqa: E402
import logging
from datetime import datetime, timezone
# # Thêm services vào path để import time_window
# dag_dir = os.path.dirname(os.path.realpath(__file__))
# sys.path.insert(0, os.path.join(dag_dir, '..'))



schedule_interval = Variable.get("schedule_interval", default_var='@daily')
execution_timeout_minutes = Variable.get("execution_timeout_minutes", default_var=30)
stripe_lookback_days = Variable.get("stripe_lookback_days", default_var=3)

models = [
    "stripe_balance_stg",
    "stripe_balance_transactions_stg",
    "stripe_charges_stg",
    "stripe_customers_stg",
    "stripe_invoices_stg",
    "stripe_payment_intents_stg",
    "stripe_plans_stg",
    "stripe_prices_stg",
    "stripe_products_stg",
    "stripe_refunds_stg",
    "stripe_subscription_items_stg",
    "stripe_subscriptions_stg",
]

with DAG(
    dag_id="stripe_dbt_pipeline",
    catchup=False,
    default_args={
        'owner': 'stripe',
        'start_date': datetime(2026, 1, 1),
        "pool": "default_pool",
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
        'execution_timeout': timedelta(minutes=int(execution_timeout_minutes)),
    },
    schedule_interval=schedule_interval,
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

    # ===== Task: tính time_window từ build_time_window() =====
    # Gọi trực tiếp hàm build_time_window() để đồng bộ với thời gian kéo API.
    # Kết quả push vào XCom: {'start': int, 'end': int, 'execution_date': str}
    def _compute_time_window(**kwargs):
        log = logging.getLogger(__name__)

        window = build_time_window(
            context=kwargs,
            run_config={"lookback_days": int(stripe_lookback_days)},
        )

        start_dt = datetime.fromtimestamp(window['start'], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        end_dt   = datetime.fromtimestamp(window['end'],   tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

        log.info("=" * 60)
        log.info(f"[stripe_dbt] Incremental window")
        log.info(f"  execution_date : {window['execution_date']}")
        log.info(f"  lookback_days  : {stripe_lookback_days}")
        log.info(f"  start_ts       : {window['start']}  ({start_dt})")
        log.info(f"  end_ts         : {window['end']}  ({end_dt})")
        log.info(f"  stripe_start_ts sẽ được truyền vào dbt --vars")
        log.info("=" * 60)

        kwargs['ti'].xcom_push(key='stripe_start_ts', value=window['start'])
        return window

    compute_time_window = PythonOperator(
        task_id='compute_time_window',
        python_callable=_compute_time_window,
    )

    # ===== Create dbt run tasks =====
    # Lấy stripe_start_ts từ XCom của compute_time_window
    dbt_tasks = []
    for model in models:
        task = BashOperator(
            task_id=f"dbt_run_{model}",
            bash_command=_dbt_cmd(
                f"dbt run --select {model} "
                "--vars '{\"stripe_start_ts\": "
                "{{ ti.xcom_pull(task_ids='compute_time_window', key='stripe_start_ts') }}"
                "}'"
            ),
        )
        dbt_tasks.append(task)

    # ===== Tasks =====
    start = BashOperator(
        task_id='start',
        bash_command=_dbt_cmd("dbt clean && dbt deps"),
    )

    dbt_source_freshness = BashOperator(
        task_id='dbt_source_freshness',
        bash_command=_dbt_cmd("dbt source freshness --select source:stripe_stg"),
    )

    start >> dbt_source_freshness >> compute_time_window >> dbt_tasks
