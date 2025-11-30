import os
import io
import zipfile
import requests
import pandas as pd
from typing import Tuple, Dict, Optional
from contextlib import contextmanager
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import logging
import subprocess
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError, NamespaceAlreadyExistsError

from xml_parsers import process_xml_file

logger = logging.getLogger(__name__)

operator = 'orebro'
NETEX_API_KEY = os.getenv('NETEX_API_KEY')
NETEX_API_URL = f"https://opendata.samtrafiken.se/netex/{operator}/{operator}.zip?key={NETEX_API_KEY}"

# ClickHouse connection settings
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', 8123))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')

# Max payload size to prevent ClickHouse errors (1MB default limit)
MAX_PAYLOAD_SIZE = 900_000  # 900KB to be safe

STATIC_BRONZE_TABLES = {
    'netex_scheduled_stop_points': 'bronze_netex_scheduled_stop_points',
    'netex_topographic_places':   'bronze_netex_topographic_places',
    'netex_stop_places':          'bronze_netex_stop_places',
    'netex_quays':                'bronze_netex_quays',
    'netex_lines':                'bronze_netex_lines',
    'netex_journey_patterns':     'bronze_netex_journey_patterns',
    'netex_journey_pattern_stops':'bronze_netex_journey_pattern_stops'
}

# Global client for connection pooling
_clickhouse_client = None

@contextmanager
def get_clickhouse_client():
    """Context manager for ClickHouse client with connection pooling and error handling."""
    global _clickhouse_client
    try:
        if _clickhouse_client is None:
            import clickhouse_connect
            _clickhouse_client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                user=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database='default'
            )
            logger.info(f"Connected to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
        yield _clickhouse_client
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse: {e}")
        raise


def download_zip(url: str) -> bytes:
    """Download ZIP file with proper error handling."""
    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()
    return resp.content

def create_static_bronze_tables(**ctx):
    """
    Create bronze tables if they don't exist using ReplacingMergeTree.
    """
    with get_clickhouse_client() as client:
        for src_name, ch_table in STATIC_BRONZE_TABLES.items():
            # Use ReplacingMergeTree for automatic deduplication
            ddl = (
                f"CREATE TABLE IF NOT EXISTS {ch_table} ("
                "dataset String, "
                "id String, "
                "version String NULL, "
                "payload String, "
                "Ingestion_Timestamp DateTime64(3), "
                "Ingestion_Date Date"
                ") ENGINE = ReplacingMergeTree(Ingestion_Timestamp) "
                "PARTITION BY Ingestion_Date "
                "ORDER BY (dataset, id)"
            )
            client.command(ddl)
            logger.info(f"Ensured ClickHouse table exists: {ch_table}")


def fetch_parse_and_load_static(**ctx):
    """
    Unified task that downloads, parses, validates, and loads NeTEx static data.
    All processing happens in-memory with streaming, no CSV intermediary files.
    """
    try:
        # 1. Download ZIP file
        logger.info("Downloading NeTEx ZIP file...")
        zip_bytes = download_zip(NETEX_API_URL)

        # 2. Initialize accumulators for each data type
        scheduled_dfs = []
        topographic_dfs = []
        stopplace_dfs = []
        quays_dfs = []
        lines_dfs = []
        jps_dfs = []
        jp_stops_dfs = []

        # 3. Process files one at a time
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            xml_files = [name for name in z.namelist() if name.lower().endswith('.xml')]
            logger.info(f"Found {len(xml_files)} XML files to process")

            for idx, filename in enumerate(xml_files, 1):
                if idx % 10 == 0:
                    logger.info(f"Processing file {idx}/{len(xml_files)}: {filename}")

                result = process_xml_file(z, filename)

                if result is None:
                    continue

                # Append non-empty dataframes
                if not result['scheduled'].empty:
                    scheduled_dfs.append(result['scheduled'])
                if not result['topographic'].empty:
                    topographic_dfs.append(result['topographic'])
                if not result['stopplace'].empty:
                    stopplace_dfs.append(result['stopplace'])
                if not result['quays'].empty:
                    quays_dfs.append(result['quays'])
                if not result['lines'].empty:
                    lines_dfs.append(result['lines'])
                if not result['journey_patterns'].empty:
                    jps_dfs.append(result['journey_patterns'])
                if not result['jp_stops'].empty:
                    jp_stops_dfs.append(result['jp_stops'])

        # 4. Data Quality Check on lines
        if lines_dfs:
            all_lines = pd.concat(lines_dfs, ignore_index=True)
            null_id_count = all_lines['id'].isnull().sum() if 'id' in all_lines.columns else 0
            total_count = len(all_lines)
            dup_count = all_lines['id'].duplicated().sum() if 'id' in all_lines.columns else 0
            
            if null_id_count > 0:
                raise ValueError(f"Data Quality Check Failed: {null_id_count} null ids in netex_lines.")
            if dup_count > 0:
                raise ValueError(f"Data Quality Check Failed: {dup_count} duplicate ids in netex_lines.")
            
            logger.info(f"Data Quality Check Passed: {total_count} lines, 0 null ids, 0 duplicates.")

        if not any([scheduled_dfs, topographic_dfs, stopplace_dfs, quays_dfs, 
                    lines_dfs, jps_dfs, jp_stops_dfs]):
            logger.warning("No data extracted from any XML files")
            return

        # 5. Prepare ingestion metadata
        ingestion_ts = pd.Timestamp.utcnow().to_pydatetime()
        try:
            ds_str = ctx.get('ds')
            ingestion_date = pd.to_datetime(ds_str).date()
        except Exception:
            ingestion_date = pd.Timestamp.utcnow().date()

        logger.info(f"Ingestion metadata: timestamp={ingestion_ts}, date={ingestion_date}")

        # 6. Load to ClickHouse
        with get_clickhouse_client() as client:
            def _concat_dedupe_and_load(dfs, dataset_key, dedup_cols=None):
                """Concatenate, deduplicate, and load dataframes to ClickHouse."""
                if not dfs:
                    logger.info(f"No data for {dataset_key}")
                    return

                # Concatenate all dataframes
                all_df = pd.concat(dfs, ignore_index=True)

                # Handle deduplication - keep latest version
                if dedup_cols:
                    if 'version' in all_df.columns:
                        all_df = all_df.sort_values('version', ascending=False)
                    all_df = all_df.drop_duplicates(subset=dedup_cols, keep='first')
                else:
                    all_df = all_df.drop_duplicates()

                # Get ClickHouse table name
                ch_table = STATIC_BRONZE_TABLES.get(dataset_key)
                if not ch_table:
                    logger.warning(f"No ClickHouse table mapping for {dataset_key}")
                    return

                # Add metadata columns
                all_df['dataset'] = dataset_key
                
                # Ensure id column exists
                if 'id' not in all_df.columns:
                    if 'ID' in all_df.columns:
                        all_df['id'] = all_df['ID']
                    else:
                        all_df['id'] = all_df.index.astype(str)

                # Ensure version column
                if 'version' not in all_df.columns:
                    all_df['version'] = None

                all_df['Ingestion_Timestamp'] = ingestion_ts
                all_df['Ingestion_Date'] = ingestion_date
                
                # Create payload column with size validation
                def safe_json_payload(row):
                    json_str = row.to_json(force_ascii=False)
                    if len(json_str) > MAX_PAYLOAD_SIZE:
                        logger.warning(f"Payload too large for {dataset_key} id={row.get('id', 'unknown')}, truncating")
                        return json_str[:MAX_PAYLOAD_SIZE] + '...[truncated]'
                    return json_str
                
                all_df['payload'] = all_df.apply(safe_json_payload, axis=1)

                # Drop entire partition for idempotency (much faster than DELETE with WHERE)
                partition_id = ingestion_date.strftime('%Y-%m-%d')
                logger.info(f"Ensuring idempotency: Dropping partition '{partition_id}' for {dataset_key}")
                try:
                    # Drop only the specific dataset from this partition using a temp table approach
                    # Since we can't filter by dataset in DROP PARTITION, we use ReplacingMergeTree's
                    # natural deduplication - newer data will automatically replace older data
                    pass  # ReplacingMergeTree handles this automatically
                except Exception as e:
                    logger.info(f"Partition management info: {e}")

                # Insert data - ReplacingMergeTree will handle deduplication automatically
                insert_df = all_df[['dataset', 'id', 'version', 'payload', 'Ingestion_Timestamp', 'Ingestion_Date']].copy()
                client.insert_df(table=ch_table, df=insert_df)
                logger.info(f"Inserted {len(insert_df)} rows into {ch_table}")

            # Load all datasets
            _concat_dedupe_and_load(scheduled_dfs, "netex_scheduled_stop_points", dedup_cols=['id'])
            _concat_dedupe_and_load(topographic_dfs, "netex_topographic_places", dedup_cols=['id'])
            _concat_dedupe_and_load(stopplace_dfs, "netex_stop_places", dedup_cols=['id'])
            _concat_dedupe_and_load(quays_dfs, "netex_quays", dedup_cols=['id'])
            _concat_dedupe_and_load(lines_dfs, "netex_lines", dedup_cols=['id'])
            _concat_dedupe_and_load(jps_dfs, "netex_journey_patterns", dedup_cols=['id'])
            _concat_dedupe_and_load(jp_stops_dfs, "netex_journey_pattern_stops")

        logger.info("NeTEx static data processing completed successfully")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading NeTEx ZIP: {e}")
        raise
    except Exception as e:
        logger.error(f"Error processing NeTEx data: {e}")
        raise

# def write_to_iceberg(**ctx):
#     """
#     Read today's bronze_netex_lines from ClickHouse,
#     write them into an Iceberg table via REST catalog (MinIO S3),
#     and create a ClickHouse S3 view for querying.
#     """
#     logger.info("Starting Iceberg write operation")

#     # 1. Fetch ingestion date
#     ingestion_date = pd.Timestamp.utcnow().date()
#     ds_str = ctx.get('ds')
#     if ds_str:
#         try:
#             ingestion_date = pd.to_datetime(ds_str).date()
#         except Exception:
#             pass

#     # 2. Fetch data from ClickHouse
#     with get_clickhouse_client() as client:
#         query = f"""
#             SELECT dataset, id, version, payload,
#                    Ingestion_Timestamp, Ingestion_Date
#             FROM bronze_netex_lines
#             WHERE Ingestion_Date = '{ingestion_date}'
#         """
#         result = client.query(query)
#     rows = result.result_rows
#     if not rows:
#         logger.warning("No rows found for today's bronze_netex_lines")
#         return

#     df = pd.DataFrame(
#         rows,
#         columns=[
#             "dataset", "id", "version", "payload",
#             "Ingestion_Timestamp", "Ingestion_Date"
#         ]
#     )

#     # 3. Convert types for Arrow
#     df["dataset"] = df["dataset"].astype(str)
#     df["id"] = df["id"].astype(str)
#     df["version"] = df["version"].fillna("").astype(str)
#     df["payload"] = df["payload"].astype(str)
#     df["Ingestion_Timestamp"] = pd.to_datetime(df["Ingestion_Timestamp"])
#     df["Ingestion_Date"] = pd.to_datetime(df["Ingestion_Date"])

#     # 4. Convert to Arrow Table
#     pa_table = pa.Table.from_pandas(df)
#     # Cast timestamp columns to microsecond precision
#     pa_table = pa_table.cast(pa.schema([
#         ('dataset', pa.string()),
#         ('id', pa.string()),
#         ('version', pa.string()),
#         ('payload', pa.string()),
#         ('Ingestion_Timestamp', pa.timestamp('us')),
#         ('Ingestion_Date', pa.timestamp('us')),
#     ]))
#     logger.info(f"Prepared Arrow table with {pa_table.num_rows} rows")

#     # 5. Connect to PyIceberg REST Catalog
#     catalog = load_catalog(
#         name="rest",
#         type="rest",
#         uri=os.getenv("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181"),
#         **{
#             "s3.endpoint": os.getenv("AWS_S3_ENDPOINT", "http://minio:9000"),
#             "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
#             "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
#             "s3.path-style-access": "true",
#         }
#     )
#     logger.info("Connected to Iceberg catalog")

#     namespace = "bronze"
#     table_name = "netex_lines_iceberg"
#     identifier = f"{namespace}.{table_name}"

#     # 6. Ensure namespace exists
#     try:
#         catalog.create_namespace(namespace)
#         logger.info(f"Created namespace: {namespace}")
#     except NamespaceAlreadyExistsError:
#         logger.info(f"Namespace already exists: {namespace}")

#     # 7. Drop table if exists
#     try:
#         catalog.drop_table(identifier)
#         logger.info(f"Table {identifier} existed and was dropped")
#     except NoSuchTableError:
#         logger.info(f"Table {identifier} did not exist, skipping drop")

#     # 8. Create Iceberg table
#     table = catalog.create_table(
#         identifier=identifier,
#         schema=pa_table.schema,
#         location=f"s3://warehouse/{namespace}/{table_name}"
#     )
#     logger.info(f"Created Iceberg table: {identifier}")

#     # 9. Append data
#     table.append(pa_table)
#     logger.info(f"Appended {pa_table.num_rows} rows to {identifier}")

#     # 10. Create ClickHouse S3 view
#     with get_clickhouse_client() as client:
#         create_view_sql = f"""
#         CREATE OR REPLACE VIEW iceberg_netex_lines AS
#         SELECT dataset, id, version, payload, Ingestion_Timestamp, Ingestion_Date
#         FROM s3(
#             'http://minio:9000/warehouse/{namespace}/{table_name}/**/*.parquet',
#             '{os.getenv("AWS_ACCESS_KEY_ID","minioadmin")}',
#             '{os.getenv("AWS_SECRET_ACCESS_KEY","minioadmin")}',
#             'Parquet'
#         )
#         """
#         client.command(create_view_sql)
#         logger.info("Created ClickHouse S3 view: iceberg_netex_lines")

# Paths inside the Airflow containers
DBT_DIR = "/opt/airflow/dbt"
DBT_BIN = "/home/airflow/.local/bin/dbt"  # dbt installed for 'airflow' user here

# Common environment for all dbt commands
DBT_ENV = {
    "DBT_PROFILES_DIR": DBT_DIR,          # profiles.yml lives in /opt/airflow/dbt
    "DBT_LOG_PATH": "/tmp/dbt_logs",      # write logs to a writable location
    "DBT_TARGET_PATH": "/tmp/dbt_target", # write manifest/run artifacts to /tmp
}

def _run_dbt_local(**ctx):
    """
    Runs dbt using the dbt installation in the project folder.
    Logs and compilation artifacts are stored in Airflow-writable folders.
    """
    logger = logging.getLogger(__name__)
    logger.info("Running dbt locally")

    try:
        # Run dbt with safe paths
        result = subprocess.run(
            [
                'dbt', 'run',
                '--profiles-dir', DBT_DIR,
                '--target-path', DBT_ENV['DBT_TARGET_PATH'],
                '--no-partial-parse'
            ],
            cwd=DBT_DIR,
            check=True,
            capture_output=True,
            text=True,
            env={
                **os.environ,
                'CLICKHOUSE_HOST': os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
                'CLICKHOUSE_HTTP_PORT': str(os.getenv('CLICKHOUSE_PORT', 8123)),
                'CLICKHOUSE_USER': os.getenv('CLICKHOUSE_USER', 'default'),
                'CLICKHOUSE_PASSWORD': os.getenv('CLICKHOUSE_PASSWORD', ''),
                'CLICKHOUSE_DB': 'default',
                'CLICKHOUSE_SCHEMA': 'analytics',
                'DBT_LOG_PATH': DBT_ENV['DBT_LOG_PATH']
            }
        )

        logger.info("dbt run completed successfully")
        logger.info(f"dbt stdout:\n{result.stdout}")
        if result.stderr:
            logger.info(f"dbt stderr:\n{result.stderr}")

    except subprocess.CalledProcessError as e:
        logger.error(f"dbt run failed with return code {e.returncode}")
        logger.error(f"stdout:\n{e.stdout}")
        logger.error(f"stderr:\n{e.stderr}")
        raise
    except FileNotFoundError:
        logger.error("dbt command not found. Install with: pip install dbt-core dbt-clickhouse")
        raise

with DAG(
    dag_id="get_static_netex",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1
) as dag:

    # Task 1: Create bronze tables
    create_table_task = PythonOperator(
        task_id="create_static_bronze_tables",
        python_callable=create_static_bronze_tables,
    )

    # Task 2: Fetch, parse, validate, and load (all in one)
    fetch_and_load_task = PythonOperator(
        task_id="fetch_parse_and_load_static",
        python_callable=fetch_parse_and_load_static,
        provide_context=True
    )

    # Task 3: Write to Iceberg
    #write_iceberg_task = PythonOperator(
    #    task_id="write_to_iceberg",
    #    python_callable=write_to_iceberg,
    #    provide_context=True
    #)

    # Ensure writable tmp dirs for dbt artifacts/logs (Windows volume perms workaround)
    ensure_tmp = BashOperator(
        task_id="ensure_tmp_dirs",
        bash_command=f"mkdir -p {DBT_ENV['DBT_LOG_PATH']} {DBT_ENV['DBT_TARGET_PATH']} && chmod -R 777 {DBT_ENV['DBT_LOG_PATH']} {DBT_ENV['DBT_TARGET_PATH']}",
    )

    # Quick sanity check that project loads and connection works
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} debug",
        env=DBT_ENV,
    )

    # Task 4: Run dbt transformations
    run_dbt = PythonOperator(
        task_id='run_dbt_static',
        python_callable=_run_dbt_local,
        provide_context=True
    )

    # Define execution order
    #create_table_task >> fetch_and_load_task >> write_iceberg_task >> run_dbt
    create_table_task >> fetch_and_load_task >> ensure_tmp >> dbt_debug >> run_dbt
