from datetime import datetime, timezone

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def say_hello():
    return "Hello Airflow"


def say_goodbye():
    return "Goodbye Airflow"


with DAG(
    dag_id="sample_dag",
    start_date=datetime(2026, 11, 12, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")
    hello = PythonOperator(task_id="hello_task", python_callable=say_hello)
    goodbye = PythonOperator(task_id="goodbye_task", python_callable=say_goodbye)
    end = EmptyOperator(task_id="end")

    start >> hello >> goodbye >> end