FROM apache/airflow:2.7.3-python3.11

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    requests \
    lxml \
    pandas \
    clickhouse-connect \
    pyarrow \
    dbt-core \
    dbt-clickhouse \
    pyiceberg[s3fs,pyarrow] \
    sqlalchemy \
    s3fs \
    boto3

# Create wait script for services
COPY --chown=airflow:root scripts/wait_for_services.sh /opt/airflow/scripts/wait_for_services.sh
RUN chmod +x /opt/airflow/scripts/wait_for_services.sh