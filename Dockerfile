FROM apache/airflow:3.1.8

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY --chown=airflow:root requirements.txt /tmp/week7-requirements.txt
RUN pip install --no-cache-dir "apache-airflow==3.1.8" -r /tmp/week7-requirements.txt