"""과제 01: 월별 아파트 매매 원본 XML 수집 → 검증/분기 → 실제 AWS S3.

제출 전 .env의 API/AWS 키만 빈 문자열로 비워 주세요.
실행 중 생성된 XML을 수정하거나 DataFrame으로 변환하지 않고 응답 bytes 그대로 저장합니다.
"""
from datetime import datetime, timedelta
import hashlib
import os
from pathlib import Path
import re
import xml.etree.ElementTree as ET

import pendulum
try:
    from airflow.sdk import DAG, TaskGroup
except ImportError:
    from airflow import DAG
    from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException
try:
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import BranchPythonOperator, PythonOperator

LAWD_CODES = ("11680", "11650", "11710", "11440", "11170", "11200")
API_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"


def inspect_xml(raw):
    """API 오류/빈 응답/파싱 실패/응답 잘림을 구분한다. 원본 XML은 변경하지 않는다."""
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        return {"ok": False, "reason": "XML_PARSE_FAILED", "rows": 0}
    result_code = (root.findtext("./header/resultCode") or "").strip()
    if not result_code or result_code.strip("0"):
        # API 키가 응답 메시지에 섞여 있을 수 있어 원문 오류 응답을 로그에 출력하지 않는다.
        return {"ok": False, "reason": "API_RESULT_CODE_" + result_code[:12], "rows": 0}
    try:
        total = int(root.findtext("./body/totalCount", "0"))
    except ValueError:
        return {"ok": False, "reason": "INVALID_TOTAL_COUNT", "rows": 0}
    actual = len(root.findall("./body/items/item"))
    if total == 0 or actual == 0:
        return {"ok": False, "reason": "ZERO_ROWS", "rows": 0}
    if actual != total:
        # 한 지역/한 달을 원본 XML 하나로 저장해야 하므로, 누락된 페이지를 성공으로 처리하지 않는다.
        return {"ok": False, "reason": f"INCOMPLETE_RESPONSE_{actual}_OF_{total}", "rows": actual}
    return {"ok": True, "reason": "OK", "rows": actual}


def collect_region(lawd_cd, yyyymm, **context):
    print(f"collector=마지희, time={datetime.now().isoformat(timespec='seconds')}, lawd={lawd_cd}", flush=True)
    # ↑ 수집 task 함수의 첫 실행 문장: 문제지의 본인 이름 실행 지문.
    from urllib.parse import unquote
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    key = os.environ.get("DATA_GO_KR_SERVICE_KEY", "").strip()
    if not key:
        raise AirflowException("DATA_GO_KR_SERVICE_KEY가 비어 있습니다. 로컬 .env를 입력하세요.")
    if not re.fullmatch(r"\d{6}", yyyymm):
        raise AirflowException("수집 월은 YYYYMM 6자리여야 합니다.")
    params = {"serviceKey": unquote(key), "LAWD_CD": lawd_cd, "DEAL_YMD": yyyymm,
              "pageNo": 1, "numOfRows": 10000}
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=Retry(
        total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"])))
    try:
        with session.get(API_URL, params=params, timeout=(10, 90)) as response:
            if response.status_code != 200:
                raise AirflowException(f"실제 API HTTP 상태: {response.status_code}; 키/승인/네트워크를 확인하세요.")
            raw = response.content
    except requests.RequestException as exc:
        # 예외 원문에는 요청 URL과 API 키가 들어갈 수 있다.
        raise AirflowException(f"실제 API 요청 실패: {type(exc).__name__}; 키를 로그에 출력하지 않습니다.") from None
    finally:
        session.close()

    status = inspect_xml(raw)
    result = {"lawd": lawd_cd, "yyyymm": yyyymm, **status}
    if status["ok"]:
        run_token = hashlib.sha256(context["run_id"].encode()).hexdigest()[:16]
        path = Path("/opt/airflow/work/bronze") / run_token / yyyymm / f"{lawd_cd}.xml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        result.update(path=str(path), sha256=hashlib.sha256(raw).hexdigest())
    print(f"lawd={lawd_cd}, month={yyyymm}, rows={status['rows']}, status={status['reason']}", flush=True)
    return result


def collection_results(ti):
    return [ti.xcom_pull(task_ids=f"collect_group.collect_{code}") for code in LAWD_CODES]


def branch_after_collect(ti):
    results = collection_results(ti)
    return "upload_to_s3" if all(r and r.get("ok") for r in results) else "skip_upload"


def skip_upload(ti):
    problems = [f"{r.get('lawd')}: {r.get('reason')}" if r else "수집 결과 없음"
                for r in collection_results(ti) if not r or not r.get("ok")]
    # 0건/XML 오류 분기를 실행하고 전체 DAG는 실패로 남겨 잘못된 완료 신호를 막는다.
    raise AirflowException("빈 응답/XML/API 오류로 업로드 중단: " + "; ".join(problems))


def upload_to_s3(ti):
    import boto3
    bucket = os.environ.get("REAL_ESTATE_BUCKET", "").strip()
    if not bucket:
        raise AirflowException("REAL_ESTATE_BUCKET가 비어 있습니다.")
    s3 = boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2"))
    results = collection_results(ti)
    uploaded = []
    for result in results:
        raw = Path(result["path"]).read_bytes()
        if hashlib.sha256(raw).hexdigest() != result["sha256"]:
            raise AirflowException("원본 XML checksum이 바뀌었습니다.")
        object_key = f"bronze/{result['yyyymm']}/{result['lawd']}.xml"
        s3.put_object(Bucket=bucket, Key=object_key, Body=raw, ContentType="application/xml",
                      Metadata={"collector": "mazzi333", "sha256": result["sha256"]})
        actual = s3.get_object(Bucket=bucket, Key=object_key)["Body"].read()
        if hashlib.sha256(actual).hexdigest() != result["sha256"]:
            raise AirflowException("S3 실제 객체와 수집 원문이 다릅니다.")
        uploaded.append(object_key)
        print(f"collector=마지희 | s3://{bucket}/{object_key} | rows={result['rows']} | 원문 SHA256 일치", flush=True)
    if len(uploaded) != 6:
        raise AirflowException("S3 업로드 검증 결과가 6개가 아닙니다.")
    return uploaded


with DAG(
    dag_id="bronze_realestate_collect",
    description="마지희 | 6개 시군구 병렬 수집·원본 XML·실제 S3·월별 catchup",
    schedule="@monthly",
    # Q3의 전월 대비 분석을 위해 인접한 2024년 11월과 12월 두 달을 재현한다.
    start_date=pendulum.datetime(2024, 11, 1, tz="UTC"),
    end_date=pendulum.datetime(2024, 12, 31, 23, 59, 59, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    max_active_tasks=6,
    default_args={"owner": "마지희", "retries": 1, "retry_delay": timedelta(seconds=20)},
    tags=["week8", "Q1", "bronze", "마지희"],
) as dag:
    with TaskGroup(group_id="collect_group", tooltip="6개 시군구 병렬 API 수집") as collect_group:
        for lawd_cd in LAWD_CODES:
            PythonOperator(
                task_id=f"collect_{lawd_cd}",
                python_callable=collect_region,
                op_kwargs={"lawd_cd": lawd_cd,
                           "yyyymm": "{{ data_interval_start.strftime('%Y%m') }}"},
            )
    branch = BranchPythonOperator(task_id="branch_after_collect", python_callable=branch_after_collect)
    upload = PythonOperator(task_id="upload_to_s3", python_callable=upload_to_s3)
    rejected = PythonOperator(task_id="skip_upload", python_callable=skip_upload)
    done = EmptyOperator(task_id="summary_done")
    collect_group >> branch >> [upload, rejected]
    upload >> done
