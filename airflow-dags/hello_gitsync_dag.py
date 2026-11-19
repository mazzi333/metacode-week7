"""과제 04: 공개 GitHub 저장소의 git-sync 배포 및 실제 Bash 실행 확인."""
import pendulum

try:
    from airflow.sdk import DAG
except ImportError:
    from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_gitsync",
    description="마지희 | Q4 | GitHub git-sync 배포 확인",
    start_date=pendulum.datetime(2024, 11, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={"owner": "마지희", "retries": 0},
    tags=["Q4", "gitsync", "마지희"],
) as dag:
    say_hello = BashOperator(
        task_id="say_hello",
        bash_command="set -eu; echo 'owner=마지희 | Q4 | Hello from git-sync'; date -Iseconds; hostname",
    )
