import requests
import lxml
from lxml import etree
import os
from datetime import datetime, date
import pandas as pd
import logging
from contextlib import contextmanager

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

# --- Configuration & Environment Variables ---
operator = 'orebro'
SIRI_API_KEY = os.getenv('SIRI_API_KEY')
# ClickHouse connection details from compose.yml
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', 8123)) 
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')

BRONZE_TABLE_NAME = 'bronze_live_timetable'

# API endpoint URL that returns XML data
api_url_live_data = f"https://opendata.samtrafiken.se/siri/{operator}/EstimatedTimetable.xml?key={SIRI_API_KEY}"

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


def parse_est_timetable(est_timetable: lxml.etree._Element) -> pd.DataFrame:
    events = []

    for el in est_timetable.iter('{http://www.siri.org.uk/siri}EstimatedVehicleJourney'):

        # Initialize per-journey fields
        dt = None
        service_journey_id = None
        line_id = None
        route_id = None
        direction_ref = None
        vehicle_mode = None
        arrival_status = None
        departure_status = None
        cancellation = None
        departure_boarding_activity = None

        # First pass: extract journey-level metadata
        for child in el:
            if child.tag == '{http://www.siri.org.uk/siri}FramedVehicleJourneyRef':
                for child2 in child:
                    if child2.tag == '{http://www.siri.org.uk/siri}DataFrameRef':
                        dt = child2.text
                    elif child2.tag == '{http://www.siri.org.uk/siri}DatedVehicleJourneyRef':
                        service_journey_id = child2.text
            elif child.tag == '{http://www.siri.org.uk/siri}LineRef':
                line_id = child.text or child.get('ref')
            elif child.tag == '{http://www.siri.org.uk/siri}RouteRef':
                route_id = child.text or child.get('ref')
            elif child.tag == '{http://www.siri.org.uk/siri}DirectionRef':
                direction_ref = child.text
            elif child.tag == '{http://www.siri.org.uk/siri}VehicleMode':
                vehicle_mode = child.text
            elif child.tag == '{http://www.siri.org.uk/siri}ArrivalStatus':
                arrival_status = child.text
            elif child.tag == '{http://www.siri.org.uk/siri}DepartureStatus':
                departure_status = child.text
            elif child.tag == '{http://www.siri.org.uk/siri}Cancellation':
                cancellation = child.text
            elif child.tag == '{http://www.siri.org.uk/siri}DepartureBoardingActivity':
                departure_boarding_activity = child.text

        # Second pass: handle RecordedCalls
        for recorded_calls in el.findall('{http://www.siri.org.uk/siri}RecordedCalls'):
            for call in recorded_calls:
                call_dict = {
                    'Date': dt,
                    'DateId': None,
                    'ServiceJourneyId': service_journey_id,
                    'LineId': line_id,
                    'RouteId': route_id,
                    'DirectionRef': direction_ref,
                    'VehicleMode': vehicle_mode,
                    'ArrivalStatus': arrival_status,
                    'DepartureStatus': departure_status,
                    'Cancellation': cancellation,
                    'DepartureBoardingActivity': departure_boarding_activity
                }

                for call_param in call:
                    if call_param.tag == '{http://www.siri.org.uk/siri}StopPointRef':
                        call_dict['QuayId'] = call_param.text
                        call_dict['StopPointId'] = call_param.text
                    elif call_param.tag == '{http://www.siri.org.uk/siri}AimedArrivalTime':
                        call_dict['AimedArrivalTime'] = call_param.text
                    elif call_param.tag == '{http://www.siri.org.uk/siri}ActualArrivalTime':
                        call_dict['ActualArrivalTime'] = call_param.text
                    elif call_param.tag == '{http://www.siri.org.uk/siri}AimedDepartureTime':
                        call_dict['AimedDepartureTime'] = call_param.text
                    elif call_param.tag == '{http://www.siri.org.uk/siri}ActualDepartureTime':
                        call_dict['ActualDepartureTime'] = call_param.text

                # Convert DataFrameRef to DateId
                try:
                    if dt:
                        call_dict['DateId'] = datetime.strptime(dt, '%Y-%m-%d').date()
                    else:
                        call_dict['DateId'] = None
                except Exception:
                    try:
                        call_dict['DateId'] = pd.to_datetime(dt, errors='coerce').date()
                    except Exception:
                        call_dict['DateId'] = None

                events.append(call_dict)

    df = pd.DataFrame(events)

    # Convert dates and times to correct dtypes
    time_cols = ['AimedArrivalTime', 'ActualArrivalTime', 'AimedDepartureTime', 'ActualDepartureTime']
    for col in time_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date

    return df


def create_bronze_table(**ctx):
    """Creates the ClickHouse Bronze table if it does not exist using ReplacingMergeTree."""
    logger.info(f"Connecting to ClickHouse on HTTP port {CLICKHOUSE_PORT} to verify/create table...")
    
    with get_clickhouse_client() as client:
        # Use ReplacingMergeTree for automatic deduplication
        # The ver column (Ingestion_Timestamp) determines which row to keep
        ddl_query = (
            f"CREATE TABLE IF NOT EXISTS {BRONZE_TABLE_NAME} ("
            f"ServiceJourneyId String, QuayId String, StopPointId String, DateId Date, "
            f"LineId String, RouteId String, DirectionRef String, VehicleMode String, "
            f"AimedArrivalTime DateTime64(3), ActualArrivalTime DateTime64(3) NULL, "
            f"AimedDepartureTime DateTime64(3), ActualDepartureTime DateTime64(3) NULL, "
            f"ArrivalStatus String, DepartureStatus String, Cancellation String, DepartureBoardingActivity String, "
            f"Ingestion_Timestamp DateTime64(3), Ingestion_Date Date"
            f") ENGINE = ReplacingMergeTree(Ingestion_Timestamp) "
            f"PARTITION BY Ingestion_Date "
            f"ORDER BY (ServiceJourneyId, QuayId, DateId)"
        )
        
        client.command(ddl_query)
        logger.info(f"Bronze table '{BRONZE_TABLE_NAME}' DDL executed successfully (ReplacingMergeTree)")


def fetch_and_load_live_data(**ctx):
    """
    Unified task that fetches, validates, and loads data directly to ClickHouse.
    All processing happens in-memory with efficient partition management.
    """
    try:
        # 1. Fetch data from API
        logger.info(f"Fetching data from API: {api_url_live_data}")
        response = requests.get(api_url_live_data, timeout=30)
        response.raise_for_status()

        # 2. Parse XML to DataFrame
        logger.info("Parsing XML response...")
        df = parse_est_timetable(etree.fromstring(response.content))
        
        if df.empty:
            logger.warning("No data received from API. Skipping load.")
            return

        # 3. Add ingestion metadata
        ingestion_ts = datetime.now()
        ingestion_date_str = ctx['ds']  # Airflow's execution date
        ingestion_date_obj = datetime.strptime(ingestion_date_str, '%Y-%m-%d').date()
        
        df['Ingestion_Timestamp'] = ingestion_ts
        df['Ingestion_Date'] = ingestion_date_obj

        logger.info(f"Parsed {len(df)} rows. Ingestion Date: {ingestion_date_obj}")

        # 4. Data Quality Check
        null_count = df['ActualArrivalTime'].isnull().sum()
        total_count = len(df)
        null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0

        if null_percentage > 50:
            raise ValueError(
                f"Data Quality Check Failed: {null_count}/{total_count} ({null_percentage:.2f}%) "
                f"of 'ActualArrivalTime' values are NULL. Data quality is too poor to proceed."
            )
        
        logger.info(f"Data Quality Check Passed: {null_percentage:.2f}% of 'ActualArrivalTime' values are NULL.")

        # 5. Prepare data for ClickHouse
        required_cols = [
            'ServiceJourneyId', 'QuayId', 'StopPointId', 'DateId', 'LineId', 'RouteId', 
            'DirectionRef', 'VehicleMode', 'AimedArrivalTime', 'ActualArrivalTime', 
            'AimedDepartureTime', 'ActualDepartureTime', 'ArrivalStatus', 'DepartureStatus', 
            'Cancellation', 'DepartureBoardingActivity', 'Ingestion_Timestamp', 'Ingestion_Date'
        ]
        
        # Ensure all columns exist
        for col in required_cols:
            if col not in df.columns:
                df[col] = None

        # Handle string columns - replace NaN with empty string
        string_cols = [
            'ServiceJourneyId', 'QuayId', 'StopPointId', 'LineId', 'RouteId', 
            'DirectionRef', 'VehicleMode', 'ArrivalStatus', 'DepartureStatus', 
            'Cancellation', 'DepartureBoardingActivity'
        ]
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str)

        # Ensure date columns are date objects
        date_cols = ['DateId', 'Ingestion_Date']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

        # Ensure datetime columns are datetime64
        datetime_cols = ['AimedArrivalTime', 'ActualArrivalTime', 'AimedDepartureTime', 
                        'ActualDepartureTime', 'Ingestion_Timestamp']
        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # 6. Load to ClickHouse with efficient partition management
        with get_clickhouse_client() as client:
            # Drop entire partition for idempotency (much faster than DELETE)
            # This removes all data for the ingestion date in one operation
            partition_id = ingestion_date_obj.strftime('%Y-%m-%d')
            logger.info(f"Ensuring idempotency: Dropping partition '{partition_id}'")
            try:
                client.command(f"ALTER TABLE {BRONZE_TABLE_NAME} DROP PARTITION '{partition_id}'")
            except Exception as e:
                # Partition might not exist on first run
                logger.info(f"Partition drop info: {e}")

            # Insert new data
            logger.info(f"Inserting {len(df)} rows into {BRONZE_TABLE_NAME}...")
            client.insert_df(table=BRONZE_TABLE_NAME, df=df[required_cols])
            logger.info(f"Successfully loaded {len(df)} rows into ClickHouse table {BRONZE_TABLE_NAME}.")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error making API request: {e}")
        raise
    except Exception as e:
        logger.error(f"Error in fetch_and_load_live_data: {e}")
        raise


with DAG(
    dag_id="live_data_pipeline",
    start_date=days_ago(1),
    schedule_interval="*/1 * * * *",
    catchup=False,
    max_active_runs=1
) as dag:

    # Task 1: Create the Bronze table
    create_table_task = PythonOperator(
        task_id="create_bronze_table",
        python_callable=create_bronze_table,
    )
    
    # Task 2: Fetch, validate, and load data (all in one)
    fetch_and_load_task = PythonOperator(
        task_id="fetch_and_load_live_data",
        python_callable=fetch_and_load_live_data,
        provide_context=True
    )
    
    # Define the execution order
    create_table_task >> fetch_and_load_task