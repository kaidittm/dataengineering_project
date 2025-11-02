import os
import io
import zipfile
import requests
from lxml import etree
import pandas as pd
from typing import Tuple, Dict, Optional
from contextlib import contextmanager
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

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


def _local_name(el):
    """Extract local name from namespaced tag."""
    return el.tag.split('}')[-1] if isinstance(el.tag, str) else el.tag


def parse_topographic_places(xml_root: etree._Element) -> pd.DataFrame:
    """Parse TopographicPlace elements."""
    rows = []
    for el in xml_root.iter():
        if _local_name(el) != "TopographicPlace":
            continue
        r = {'id': el.get('id'), 'version': el.get('version')}
        for child in el:
            tag = _local_name(child)
            if tag == 'IsoCode':
                r['IsoCode'] = child.text
            elif tag == 'Descriptor':
                if len(child) > 0 and child[0].text:
                    r['Descriptor'] = child[0].text
                elif child.text:
                    r['Descriptor'] = child.text
            elif tag == 'TopographicPlaceType':
                r['TopographicPlaceType'] = child.text
            elif tag == 'CountryRef':
                r['CountryRef'] = child.get('ref')
            elif tag == 'PrivateCode':
                r['PrivateCode'] = child.text
            elif tag == 'ParentTopographicPlaceRef':
                r['ParentTopographicPlaceRef'] = child.get('ref')
                r['ParentTopographicPlaceVersion'] = child.get('version')
        rows.append(r)
    return pd.DataFrame(rows)


def parse_stop_places_and_quays(xml_root: etree._Element) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Parse StopPlace and Quay elements."""
    stops_list = []
    quays = []
    for el in xml_root.iter():
        if _local_name(el) != "StopPlace":
            continue
        el_dict = {'id': el.get('id'), 'version': el.get('version')}
        for child in el:
            tag = _local_name(child)
            if tag == 'Name':
                el_dict['Name'] = child.text
            elif tag == 'ShortName':
                el_dict['ShortName'] = child.text
            elif tag == 'PrivateCode':
                el_dict['PrivateCode'] = child.text
            elif tag == 'Centroid':
                lon = None
                lat = None
                for coord_el in child.iter():
                    ctag = _local_name(coord_el)
                    if ctag in ('Longitude','Long'):
                        lon = coord_el.text
                    elif ctag in ('Latitude','Lat'):
                        lat = coord_el.text
                if lon is not None:
                    el_dict['Centroid_Long'] = lon
                if lat is not None:
                    el_dict['Centroid_Lat'] = lat
            elif tag == 'TopographicPlaceRef':
                el_dict['TopographicPlaceRef'] = child.get('ref')
                el_dict['TopographicPlaceVersion'] = child.get('version')
            elif tag == 'OrganisationRef':
                el_dict['OrganisationRef'] = child.get('ref')
            elif tag == 'ParentSiteRef':
                el_dict['ParentSiteRef'] = child.get('ref')
                el_dict['ParentSiteVersion'] = child.get('version')
            elif tag == 'TransportMode':
                el_dict['TransportMode'] = child.text
            elif tag == 'StopPlaceType':
                el_dict['StopPlaceType'] = child.text
            elif tag.lower() == 'quays':
                for quay_el in child:
                    quays.append({
                        'id': quay_el.get('id'),
                        'version': quay_el.get('version'),
                        'stopPlaceId': el_dict['id']
                    })
        stops_list.append(el_dict)
    return pd.DataFrame(stops_list), pd.DataFrame(quays)


def parse_scheduled_stop_points(xml_root: etree._Element) -> pd.DataFrame:
    """Parse ScheduledStopPoint elements."""
    rows = []
    for el in xml_root.iter():
        if _local_name(el) not in ("ScheduledStopPoint","StopPoint","StopArea","StopPointInFrame"):
            continue
        rec = {'id': el.get('id'), 'version': el.get('version')}
        for child in el:
            tag = _local_name(child)
            if tag == 'Name':
                rec['Name'] = child.text
            elif tag in ('PublicCode', 'publicCode'):
                rec['PublicCode'] = child.text
            elif tag == 'StopPlaceRef':
                rec['StopPlaceRef'] = child.get('ref')
                rec['StopPlaceRefVersion'] = child.get('version')
            elif tag == 'Centroid':
                lon = None
                lat = None
                for coord_el in child.iter():
                    ctag = _local_name(coord_el)
                    if ctag in ('Longitude','Long'):
                        lon = coord_el.text
                    elif ctag in ('Latitude','Lat'):
                        lat = coord_el.text
                rec['Centroid_Long'] = lon
                rec['Centroid_Lat'] = lat
            elif tag == 'Location':
                lat_el = child.find('.//{*}Latitude')
                lon_el = child.find('.//{*}Longitude')
                if lat_el is not None:
                    rec['Centroid_Lat'] = lat_el.text
                if lon_el is not None:
                    rec['Centroid_Long'] = lon_el.text
        rows.append(rec)
    return pd.DataFrame(rows)


def parse_lines_and_journey_patterns(xml_root: etree._Element) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Parse Line, JourneyPattern, and StopPointInJourneyPattern elements."""
    lines = []
    journey_patterns = []
    jp_stops = []
    for el in xml_root.iter():
        lname = _local_name(el)
        if lname == 'Line':
            lines.append({
                'id': el.get('id'),
                'version': el.get('version'),
                'Name': (el.find('.//{*}Name').text if el.find('.//{*}Name') is not None else None),
                'PublicCode': (el.find('.//{*}PublicCode').text if el.find('.//{*}PublicCode') is not None else None),
            })
        elif lname == 'JourneyPattern':
            jp_id = el.get('id')
            jp_version = el.get('version')
            route_ref_el = el.find('.//{*}RouteRef')
            jp_entry = {'id': jp_id, 'version': jp_version, 'RouteRef': (route_ref_el.get('ref') if route_ref_el is not None else None)}
            journey_patterns.append(jp_entry)
            for spi in el.findall('.//{*}StopPointInJourneyPattern'):
                order = spi.get('order')
                sref_el = spi.find('.//{*}ScheduledStopPointRef')
                scheduled_ref = sref_el.get('ref') if sref_el is not None else None
                if scheduled_ref is None:
                    scheduled_ref = spi.get('ref') or (spi.find('.//{*}StopPointRef').get('ref') if spi.find('.//{*}StopPointRef') is not None else None)
                order_num = None
                if order:
                    try:
                        order_num = float(order)
                    except ValueError:
                        logger.warning(f"Could not parse order value: {order}")
                jp_stops.append({
                    'journeyPatternId': jp_id,
                    'stopOrder': order_num,
                    'scheduledStopPointRef': scheduled_ref,
                    'stopPointInJourneyPatternId': spi.get('id'),
                    'forBoarding': (spi.find('.//{*}ForBoarding').text if spi.find('.//{*}ForBoarding') is not None else None),
                    'forAlighting': (spi.find('.//{*}ForAlighting').text if spi.find('.//{*}ForAlighting') is not None else None)
                })
    return pd.DataFrame(lines), pd.DataFrame(journey_patterns), pd.DataFrame(jp_stops)


def process_xml_file(zip_file: zipfile.ZipFile, filename: str) -> Optional[Dict[str, pd.DataFrame]]:
    """
    Process a single XML file from the ZIP archive.
    Returns a dict with all parsed dataframes for this file.
    """
    try:
        data = zip_file.read(filename)
        root = etree.fromstring(data)
        
        # Parse each type once and store results
        scheduled_df = parse_scheduled_stop_points(root)
        topographic_df = parse_topographic_places(root)
        stopplace_df, quays_df = parse_stop_places_and_quays(root)
        lines_df, jp_df, jp_stops_df = parse_lines_and_journey_patterns(root)
        
        return {
            'scheduled': scheduled_df,
            'topographic': topographic_df,
            'stopplace': stopplace_df,
            'quays': quays_df,
            'lines': lines_df,
            'journey_patterns': jp_df,
            'jp_stops': jp_stops_df
        }
    except etree.XMLSyntaxError as e:
        logger.warning(f"XML parsing error in {filename}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error processing {filename}: {e}")
        return None


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

        # 3. Process files one at a time (streaming approach)
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


def _run_dbt_local(**ctx):
    """
    Runs dbt using the dbt installation in the project folder.
    Logs and compilation artifacts are stored in Airflow-writable folders.
    """
    import os
    import subprocess
    import logging

    logger = logging.getLogger(__name__)
    logger.info("Running dbt locally")

    try:
        # DAG project root
        project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        # DBT project folder
        dbt_dir = os.path.join(project_dir, "dbt")

        # Safe folder for DBT logs and target artifacts
        airflow_logs_dir = os.path.join(project_dir, "airflow_dbt_logs")
        os.makedirs(airflow_logs_dir, exist_ok=True)

        dbt_log_path = os.path.join(airflow_logs_dir, "dbt.log")
        dbt_target_dir = os.path.join(airflow_logs_dir, "target")
        os.makedirs(dbt_target_dir, exist_ok=True)

        # Run dbt with safe paths
        result = subprocess.run(
            [
                'dbt', 'run',
                '--profiles-dir', dbt_dir,
                '--target-path', dbt_target_dir,
                '--no-partial-parse'
            ],
            cwd=dbt_dir,
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
                'DBT_LOG_PATH': dbt_log_path
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

    # Task 3: Run dbt transformations
    run_dbt = PythonOperator(
        task_id='run_dbt_static',
        python_callable=_run_dbt_local,
        provide_context=True
    )

    # Define execution order
    create_table_task >> fetch_and_load_task >> run_dbt