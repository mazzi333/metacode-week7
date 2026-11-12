from datetime import timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, get_current_context


def count_items():
    ti = get_current_context()["ti"]
    if ti.try_number == 1:
        raise RuntimeError("Q6 demo: intentional failure on first attempt")

    return len(["Airflow", "Spark", "Kafka"])


def use_value():
    ti = get_current_context()["ti"]
    value = ti.xcom_pull(task_ids="count_items", key="return_value")
    print(f"Received from XCom: {value}; doubled: {value * 2}")


with DAG(dag_id="xcom_demo_마지희", schedule=None, catchup=False):
    count = PythonOperator(
        task_id="count_items",
        python_callable=count_items,
        retries=2,
        retry_delay=timedelta(seconds=10),
    )

    use = PythonOperator(
        task_id="use_value",
        python_callable=use_value,
    )

    count >> use