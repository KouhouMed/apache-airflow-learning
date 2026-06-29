from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "weatherflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def print_hello():
    print("WeatherFlow pipeline is alive and running.")


def print_context(**kwargs):
    logical = kwargs.get('logical_date')
    end = kwargs.get('data_interval_end')
    print(f"Logical date  : {logical.date() if logical else 'N/A (manual run)'}")
    print(f"Next run date : {end.date() if end else 'N/A (manual run)'}")
    print(f"DAG run id    : {kwargs['run_id']}")


with DAG(
    dag_id="hello_world",
    default_args=default_args,
    description="Day 1 — verify Airflow setup and learn DAG structure",
    schedule="@daily",
    start_date=datetime(2026, 6, 24),
    catchup=False,
    tags=["learning", "day-1"],
) as dag:

    hello = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello,
    )

    context = PythonOperator(
        task_id="print_context",
        python_callable=print_context,
    )

    system_date = BashOperator(
        task_id="print_system_date",
        bash_command='echo "Container date: $(date)"',
    )

    hello >> context >> system_date
