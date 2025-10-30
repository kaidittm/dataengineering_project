# Dockerfile
FROM apache/airflow:2.8.1-python3.11

# Install any Python dependencies
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    python-dotenv \
    requests \
    clickhouse-connect \
    lxml \
    pandas
