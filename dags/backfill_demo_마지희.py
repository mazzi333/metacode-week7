from pathlib import Path

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.timetables.trigger import CronTriggerTimetable


def write_daily_file(ds: str):
    folder = Path("/tmp/week7_q7")
    folder.mkdir(parents=True, exist_ok=True)
    output = folder / f"daily_{ds}.txt"
    output.write_text(f"logical_date={ds}\n", encoding="utf-8")
    print(f"Created: {output}")


with DAG(
    dag_id="backfill_demo_마지희",
    start_date=pendulum.datetime(2026, 11, 5, tz="UTC"),
    schedule=CronTriggerTimetable("0 0 * * *", timezone="UTC"),
    catchup=True,
    max_active_runs=2,
):
    PythonOperator(
        task_id="write_daily_file",
        python_callable=write_daily_file,
        op_kwargs={"ds": "{{ ds }}"},
    )