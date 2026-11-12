# Week 7 - Airflow and Spark



\## 실습 환경



Windows에서 Docker Desktop과 Docker Compose로 Airflow와 Spark를 실행하고, AWS S3 버킷에 실습 데이터를 저장했다.



\## 회고



Airflow에서 첫 시도가 실패한 태스크가 재시도로 성공하는 과정을 확인했다. 이후 XCom으로 반환값을 다음 태스크에 넘기고, 로그에서 그 값이 실제로 쓰였는지 확인했다.

