FROM apache/airflow:2.10.0-python3.10

ARG OM_VERSION=1.10.7

ENV PIP_NO_CACHE_DIR=1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc libffi-dev git curl \
 && rm -rf /var/lib/apt/lists/*

# Install Python packages as the airflow user
USER airflow

# Airflow constraints
RUN pip install \
    apache-airflow-providers-openlineage==1.10.0 \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.0/constraints-3.10.txt"

# OpenMetadata APIs + deps
RUN pip install \
    openmetadata-managed-apis==${OM_VERSION} \
    openmetadata-ingestion==${OM_VERSION} \
    "clickhouse-connect>=0.7,<0.8" \
    dbt-core==1.8.4 \
    dbt-clickhouse==1.8.0 \
    clickhouse-sqlalchemy clickhouse-driver "clickhouse-connect>=0.7,<0.8"
