import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.python import PythonOperator



schedule_interval = Variable.get("schedule_interval", default_var='@daily')
execution_timeout_minutes = Variable.get("execution_timeout_minutes", default_var=30)

models = [
    # ===== Core / base objects =====
    "stripe_customers_stg",
    "stripe_products_stg",
    "stripe_prices_stg",
    "stripe_plans_stg",

    # ===== Payments & transactions =====
    "stripe_payment_intents_stg",
    "stripe_charges_stg",
    "stripe_refunds_stg",
    "stripe_balance_transactions_stg",
    "stripe_balance_stg",

    # ===== Subscriptions =====
    "stripe_subscriptions_stg",
    "stripe_subscription_items_stg",

    # ===== Invoices (base first) =====
    "stripe_invoices_stg",

    # ===== Invoice components =====
    "stripe_invoice_line_items_stg",
    "stripe_invoice_line_item_taxes_stg",
    "stripe_invoice_line_item_discounts_stg",

    # ===== Invoice metadata =====
    "stripe_invoice_customer_tax_ids_stg",
    "stripe_invoice_default_tax_rates_stg",
    "stripe_invoice_discounts_stg",

    # ===== Invoice totals =====
    "stripe_invoice_total_taxes_stg",
    "stripe_invoice_total_discount_amounts_stg",

    # ===== Invoice payments (final layer) =====
    "stripe_invoice_payments_stg",
]

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
    # ===== Create dbt run tasks =====
    dbt_tasks = []
    for model in models:
        task = BashOperator(
            task_id=f"dbt_run_{model}",
            bash_command=_dbt_cmd(f"dbt run --select {model}"),
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

    # dbt_run_staging = BashOperator(
    #     task_id='dbt_run_staging',
    #     bash_command=_dbt_cmd("dbt run --select stripe_stg"),
    # )

    start >> dbt_source_freshness >> dbt_tasks

