# Airflow image with the pipeline dependencies. The project code (src/, config/) is mounted
# as a volume, not copied, so code changes do not need a rebuild.
FROM apache/airflow:2.10.5-python3.11

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11

# PySpark (Gold DAG) needs a Java runtime. Debian 12 ships Java 17, supported by Spark 3.5.
# default-java is an architecture-independent path, so the image also builds on ARM (Apple Silicon).
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
