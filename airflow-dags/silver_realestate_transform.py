"""과제 02: Bronze 월별 완료 대기 → 실제 spark-submit → Silver S3 검증."""
from datetime import timedelta
import os

import pendulum
try:
    from airflow.sdk import DAG
except ImportError:
    from airflow import DAG
try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
except ImportError:
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator
    from airflow.sensors.external_task import ExternalTaskSensor

LAWD_NAMES = {"11680": "강남구", "11650": "서초구", "11710": "송파구",
              "11440": "마포구", "11170": "용산구", "11200": "성동구"}


def verify_silver(yyyymm):
    import boto3
    bucket = os.environ["REAL_ESTATE_BUCKET"]
    s3 = boto3.client("s3")
    for name in LAWD_NAMES.values():
        prefix = f"silver/시군구={name}/yyyymm={yyyymm}/"
        objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
        parquet = [obj for obj in objects if obj["Key"].endswith(".parquet") and obj["Size"] > 0]
        if not parquet:
            raise RuntimeError(f"실제 S3 Silver parquet 누락: {prefix}")
        print(f"owner=마지희 | s3://{bucket}/{prefix} | parquet={len(parquet)} | 검증 OK", flush=True)


with DAG(
    dag_id="silver_realestate_transform",
    description="마지희 | ExternalTaskSensor → spark-submit → UDF 2개·시군구별 IQR·Snappy Parquet",
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 11, 1, tz="UTC"),
    end_date=pendulum.datetime(2024, 12, 31, 23, 59, 59, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={"owner": "마지희", "retries": 0, "retry_delay": timedelta(seconds=20)},
    tags=["week8", "Q2", "silver", "마지희"],
) as dag:
    wait_bronze = ExternalTaskSensor(
        task_id="wait_bronze",
        external_dag_id="bronze_realestate_collect",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        check_existence=True,
        mode="reschedule",
        deferrable=False,
        poke_interval=15,
        timeout=3600,
    )
    transform = BashOperator(
        task_id="run_silver_spark",
        bash_command=(
            "set -euo pipefail; "
            "echo 'owner=마지희 | job=silver_realestate_transform | month={{ data_interval_start.strftime(\"%Y%m\") }}'; "
            "spark-submit --master 'local[2]' --driver-memory 1g "
            "--conf spark.ui.enabled=true --conf spark.ui.port=4040 "
            "--conf spark.sql.shuffle.partitions=6 "
            "/opt/airflow/scripts/Q2/silver_spark.py "
            "--yyyymm '{{ data_interval_start.strftime(\"%Y%m\") }}' "
            "--ui-hold-file /opt/airflow/work/q2_ui_hold"
        ),
        execution_timeout=timedelta(minutes=40),
    )
    verify = PythonOperator(
        task_id="verify_silver_s3",
        python_callable=verify_silver,
        op_kwargs={"yyyymm": "{{ data_interval_start.strftime('%Y%m') }}"},
    )
    wait_bronze >> transform >> verify
