import requests
import lxml
from lxml import etree
import os
from datetime import datetime, date
import pandas as pd
import clickhouse_connect

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# --- Configuration & Environment Variables ---
operator = 'orebro'
SIRI_API_KEY = os.getenv('SIRI_API_KEY')

# ClickHouse connection details from compose.yml (required for load/DDL tasks)
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', 8123)) 
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')

BRONZE_TABLE_NAME = 'bronze_live_timetable' # Constant for the Bronze table name

# API endpoint URL that returns XML data
api_url_live_data = f"https://opendata.samtrafiken.se/siri/{operator}/EstimatedTimetable.xml?key={SIRI_API_KEY}"


def parse_est_timetable(est_timetable: lxml.etree._Element) -> pd.DataFrame:
    events = []

    for el in est_timetable.iter('{http://www.siri.org.uk/siri}EstimatedVehicleJourney'):

        dt = ''
        ServiceJourneyId = ''

        for child in el:
            # parse date and servicejourney id, which might be the same for multiple events
            if child.tag == '{http://www.siri.org.uk/siri}FramedVehicleJourneyRef':
                for child2 in child:
                    if child2.tag == '{http://www.siri.org.uk/siri}DataFrameRef':
                        dt = child2.text
                    elif child2.tag == '{http://www.siri.org.uk/siri}DatedVehicleJourneyRef':
                        ServiceJourneyId = child2.text
            
            # parse event-specific data
            elif child.tag == '{http://www.siri.org.uk/siri}RecordedCalls':
                for call in child:

                    call_dict = {
                        'Date': dt,
                        'ServiceJourneyId': ServiceJourneyId
                    }
                    
                    for call_param in call:
                        if call_param.tag == '{http://www.siri.org.uk/siri}StopPointRef':
                            call_dict['QuayId'] = call_param.text
                        elif call_param.tag == '{http://www.siri.org.uk/siri}AimedArrivalTime':
                            call_dict['AimedArrivalTime'] = call_param.text
                        elif call_param.tag == '{http://www.siri.org.uk/siri}ActualArrivalTime':
                            call_dict['ActualArrivalTime'] = call_param.text
                        elif call_param.tag == '{http://www.siri.org.uk/siri}AimedDepartureTime':
                            call_dict['AimedDepartureTime'] = call_param.text
                        elif call_param.tag == '{http://www.siri.org.uk/siri}ActualDepartureTime':
                            call_dict['ActualDepartureTime'] = call_param.text

                    events.append(call_dict)

    df = pd.DataFrame(events)

    # convert dates and times to correct dtypes, coercing errors to NaT
    time_cols = ['AimedArrivalTime', 'ActualArrivalTime', 'AimedDepartureTime', 'ActualDepartureTime']
    for col in time_cols:
        # Use errors='coerce' to turn unparsable data into NaT (Not a Time)
        df[col] = pd.to_datetime(df[col], format='mixed', errors='coerce')
    
    # Ensure 'Date' is standard Date object
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date

    return df

def get_live_data(**ctx):
    """
    Fetches XML data, parses it to a DataFrame, adds metadata, and returns it for XCom.
    """
    try:
        # 1. Make the GET request to the API
        response = requests.get(api_url_live_data)
        response.raise_for_status() # Raise an exception for bad status codes

        # 2. Parse the xml
        df = parse_est_timetable(etree.fromstring(response.content))

        # 3. Add ingestion metadata for partitioning and idempotency (REQUIRED for later steps)
        ingestion_ts = datetime.now()
        ingestion_date_str = ctx['ds'] # Airflow's execution date as a string (e.g., '2025-10-29')
        
        # FIX: Convert the string date (ds) to a datetime.date object for ClickHouse compatibility
        ingestion_date_obj = datetime.strptime(ingestion_date_str, '%Y-%m-%d').date()
        
        df['Ingestion_Timestamp'] = ingestion_ts
        df['Ingestion_Date'] = ingestion_date_obj 

        print(f"DataFrame ready with {len(df)} rows. Ingestion Date: {ingestion_date_obj}")

        # 4. Return the DataFrame so it's pushed to XCom
        return df

    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
        # Re-raise to fail the task
        raise                                
    except Exception as e:
        print(f"Error parsing XML: {e}")
        raise

# --- New Function: Data Quality Check (Fulfills project requirement) ---
def data_quality_check(**ctx):
    """
    Checks for null values in the critical ActualArrivalTime column, a required project step.
    Pulls the DataFrame from the upstream task via XCom.
    """
    ti = ctx['ti']
    # Pull the DataFrame from the 'get_live_data' task
    df = ti.xcom_pull(task_ids='get_live_data')

    if df is None or df.empty:
        raise ValueError("Data Quality Check Failed: No data received from upstream task.")

    # Data Quality Check #1: Check for nulls in the critical time column (ActualArrivalTime)
    null_count = df['ActualArrivalTime'].isnull().sum()
    total_count = len(df)
    null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0

    # Fail the task if more than 50% of the key column is null
    if null_percentage > 50:
        raise ValueError(
            f"Data Quality Check Failed: {null_count}/{total_count} ({null_percentage:.2f}%) "
            f"of 'ActualArrivalTime' values are NULL. Data quality is too poor to proceed."
        )
    
    print(f"Data Quality Check Passed: Only {null_percentage:.2f}% of 'ActualArrivalTime' values are NULL.")
    
    # Return the DataFrame so it's pushed to XCom for the next task (the load task)
    return df 

# ClickHouse DDL Task (Bronze Table Creation)
def create_bronze_table(**ctx):
    """Creates the ClickHouse Bronze table if it does not exist."""
    print(f"Connecting to ClickHouse on HTTP port {CLICKHOUSE_PORT} to verify/create table...")
    try:
        client = clickhouse_connect.get_client( 
            host=CLICKHOUSE_HOST, 
            port=CLICKHOUSE_PORT, 
            user=CLICKHOUSE_USER, 
            password=CLICKHOUSE_PASSWORD,
            database='default' 
        )

        # DDL simplified to a single line string to avoid parsing issues
        ddl_query = (
            f"CREATE TABLE IF NOT EXISTS {BRONZE_TABLE_NAME} ("
            f"Date Date, ServiceJourneyId String, QuayId String, "
            f"AimedArrivalTime DateTime64(3), ActualArrivalTime DateTime64(3) NULL, "
            f"AimedDepartureTime DateTime64(3), ActualDepartureTime DateTime64(3) NULL, "
            f"Ingestion_Timestamp DateTime64(3), Ingestion_Date Date"
            f") ENGINE = MergeTree() "
            f"PARTITION BY Ingestion_Date "
            f"ORDER BY (ServiceJourneyId, QuayId, Ingestion_Timestamp)"
        )
        
        client.command(ddl_query)
        print(f"Bronze table '{BRONZE_TABLE_NAME}' DDL executed successfully.")

    except Exception as e:
        print(f"Error creating ClickHouse table: {e}")
        # The task must fail if the table cannot be created
        raise 

# Load to ClickHouse Bronze Layer (Idempotent Load)
def load_to_clickhouse(**ctx):
    """
    Pulls the DataFrame from the quality check task and loads it idempotently into ClickHouse.
    """
    ti = ctx['ti']
    # Pull the DataFrame from the 'data_quality_check' task
    df = ti.xcom_pull(task_ids='data_quality_check')
    
    if df is None or df.empty:
        print("No data received from upstream task. Skipping load.")
        return

    # Extract the ingestion date for idempotency (partitioning)
    ingestion_date = df['Ingestion_Date'].iloc[0]

    try:
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST, 
            port=CLICKHOUSE_PORT, 
            user=CLICKHOUSE_USER, 
            password=CLICKHOUSE_PASSWORD,
            database='default'
        )

        # 1. Idempotency Step: Delete data for the current ingestion date before insertion
        print(f"Idempotency Check: Deleting existing data for Ingestion_Date='{ingestion_date}'")
        client.command(f"ALTER TABLE {BRONZE_TABLE_NAME} DELETE WHERE Ingestion_Date = '{ingestion_date}'")

        # 2. Insert data (using bulk insert for performance)
        print(f"Inserting {len(df)} rows into {BRONZE_TABLE_NAME}...")
        
        client.insert_df(
            table=BRONZE_TABLE_NAME, 
            df=df
        )

        print(f"Successfully loaded {len(df)} rows into ClickHouse table {BRONZE_TABLE_NAME}.")

    except Exception as e:
        print(f"Error loading data to ClickHouse: {e}")
        # Fail the task if the load fails
        raise 

with DAG(
    dag_id="live_data_pipeline",
    start_date=days_ago(1),
    schedule_interval="*/1 * * * *",
    catchup=False,
    max_active_runs=1
) as dag:

    # 1. New Task: Create the Bronze table (Must run first)
    create_table_task = PythonOperator(
        task_id="create_bronze_table",
        python_callable=create_bronze_table,
    )
    
    # 2. Data Ingestion
    fetch_task = PythonOperator(
        task_id="get_live_data",
        python_callable=get_live_data,
        provide_context=True
    )
    
    # 3. Data Quality Check
    quality_check_task = PythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_check,
        provide_context=True
    )

    # 4. Data Load
    load_task = PythonOperator(
        task_id="load_to_clickhouse",
        python_callable=load_to_clickhouse,
        provide_context=True
    )
    
    # Define the execution order: DDL -> Fetch -> Quality Check -> Load
    create_table_task >> fetch_task >> quality_check_task >> load_task
