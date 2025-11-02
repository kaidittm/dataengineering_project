FROM apache/airflow:2.8.1-python3.11

# Switch to root to install scripts
USER root

# Copy helper scripts and make executable
COPY scripts/wait_for_services.sh /opt/airflow/scripts/wait_for_services.sh
RUN chmod +x /opt/airflow/scripts/wait_for_services.sh

# Switch back to airflow user for pip installations
USER airflow

# Install all Python packages needed for DAGs and dbt
RUN pip install --no-cache-dir \
    clickhouse-connect \
    lxml \
    requests \
    pandas \
    dbt-core==1.7.4 \
    dbt-clickhouse==1.7.2

WORKDIR /opt/airflow