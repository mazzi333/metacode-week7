"""과제 03 — Silver 완료 대기 → spark-submit으로 5종 집계/JDBC 적재 → 실제 DB 검증."""
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

GOLD_TABLES = (
    "gold_realestate_district_avg", "gold_realestate_top10", "gold_realestate_size_dist",
    "gold_realestate_age_avg", "gold_realestate_mom_change",
)


def verify_gold_tables(yyyymm):
    import psycopg2
    with psycopg2.connect(host="postgres", dbname="airflow", user="airflow",
                          password=os.environ.get("GOLD_POSTGRES_PASSWORD", "airflow_local_week8")) as conn:
        with conn.cursor() as cur:
            for table in GOLD_TABLES:
                cur.execute(f"SELECT COUNT(*) FROM public.{table}")
                count = cur.fetchone()[0]
                print(f"owner=마지희 | table={table} | row_count={count}", flush=True)
                if count <= 0:
                    raise RuntimeError(f"{table}에 실제 적재된 행이 없습니다.")
            if yyyymm >= "202412":
                cur.execute("SELECT COUNT(*) FROM public.gold_realestate_mom_change "
                            "WHERE yyyymm=%s AND price_change_pct IS NOT NULL AND pp_change_pct IS NOT NULL", (yyyymm,))
                if cur.fetchone()[0] == 0:
                    raise RuntimeError("12월 전월 대비 변화율이 모두 NULL입니다. 11월/12월 Silver 결과를 확인하세요.")
    print(f"GOLD_VERIFY_SUCCESS owner=마지희 month={yyyymm} tables=5", flush=True)


with DAG(
    dag_id="gold_realestate_aggregate",
    description="마지희 | ExternalTaskSensor → Spark 집계 5종·LAG → PostgreSQL JDBC → row_count 검증",
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 11, 1, tz="UTC"),
    end_date=pendulum.datetime(2024, 12, 31, 23, 59, 59, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={"owner": "마지희", "retries": 0, "retry_delay": timedelta(seconds=20)},
    tags=["week8", "Q3", "gold", "마지희"],
) as dag:
    wait_silver = ExternalTaskSensor(
        task_id="wait_silver", external_dag_id="silver_realestate_transform",
        external_task_id=None, allowed_states=["success"], failed_states=["failed"],
        check_existence=True, mode="reschedule", deferrable=False, poke_interval=15, timeout=3600,
    )
    aggregate = BashOperator(
        task_id="run_gold_spark",
        bash_command=(
            "set -euo pipefail; "
            "echo 'owner=마지희 | job=gold_realestate_aggregate | month={{ data_interval_start.strftime(\"%Y%m\") }}'; "
            "python /opt/airflow/scripts/Q3/gold_spark_sql.py --prepare-driver; "
            "spark-submit --master 'local[2]' --driver-memory 1g "
            "--driver-class-path /opt/airflow/work/jars/postgresql-42.7.14.jar "
            "--jars /opt/airflow/work/jars/postgresql-42.7.14.jar "
            "--conf spark.ui.enabled=false --conf spark.sql.shuffle.partitions=6 "
            "/opt/airflow/scripts/Q3/gold_spark_sql.py "
            "--yyyymm '{{ data_interval_start.strftime(\"%Y%m\") }}'"
        ),
        execution_timeout=timedelta(minutes=20),
    )
    verify = PythonOperator(task_id="verify_gold_tables", python_callable=verify_gold_tables,
                            op_kwargs={"yyyymm": "{{ data_interval_start.strftime('%Y%m') }}"})
    wait_silver >> aggregate >> verify
