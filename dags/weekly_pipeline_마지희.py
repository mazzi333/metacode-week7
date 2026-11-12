import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param, get_current_context


def s3_client():
    return S3Hook(aws_conn_id="week7_q9_aws").get_conn()


def download_csv():
    bucket = get_current_context()["params"]["bucket"]
    folder = Path(tempfile.mkdtemp(prefix="week7_q9_"))
    key = "bronze/netflix_titles.csv"
    s3_client().download_file(bucket, key, str(folder / "netflix_titles.csv"))
    print(f"Downloaded s3://{bucket}/{key}")
    return {
        "folder": str(folder),
        "date": datetime.now(ZoneInfo("Asia/Seoul")).date().isoformat(),
    }


def transform():
    context = get_current_context()
    run = context["ti"].xcom_pull(task_ids="download_csv")
    folder = Path(run["folder"])
    script = Path(__file__).parent / "jobs" / "transform.py"

    subprocess.run(
        [
            "spark-submit",
            "--master", "local[2]",
            "--conf", "spark.ui.enabled=false",
            "--conf", "spark.sql.shuffle.partitions=2",
            str(script),
            str(folder / "netflix_titles.csv"),
            str(folder / "parquet"),
            "--min-year", str(context["params"]["min_year"]),
        ],
        check=True,
    )


def upload_silver():
    context = get_current_context()
    bucket = context["params"]["bucket"]
    run = context["ti"].xcom_pull(task_ids="download_csv")
    prefix = f"silver/{run['date']}/"
    files = sorted((Path(run["folder"]) / "parquet").glob("*.parquet"))
    if not files:
        raise RuntimeError("No Parquet files to upload")

    s3 = s3_client()
    for index, file in enumerate(files):
        key = f"{prefix}part-{index:05d}.snappy.parquet"
        s3.upload_file(str(file), bucket, key)
        print(f"Uploaded s3://{bucket}/{key}")

    count = 0
    pages = s3.get_paginator("list_objects_v2")
    for page in pages.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            print(f"{obj['Key']}  {obj['Size']:,} bytes")
            count += 1
    print(f"SILVER_OBJECTS={count}")


with DAG(
    dag_id="weekly_pipeline_마지희",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "bucket": Param("", type="string", title="S3 bucket"),
        "min_year": Param(2015, type="integer", title="Minimum release year"),
    },
):
    download_task = PythonOperator(
        task_id="download_csv", python_callable=download_csv
    )
    transform_task = PythonOperator(
        task_id="transform", python_callable=transform
    )
    upload_task = PythonOperator(
        task_id="upload_silver", python_callable=upload_silver
    )

    download_task >> transform_task >> upload_task