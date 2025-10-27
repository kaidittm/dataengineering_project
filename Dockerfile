# Dockerfile
FROM apache/airflow:2.8.1

# Install any Python dependencies
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" python-dotenv lxml pandas
