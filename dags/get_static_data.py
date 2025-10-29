import os
import io
import zipfile
import requests
from lxml import etree
import pandas as pd
from typing import Tuple, List
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

logger = logging.getLogger(__name__)

operator = 'orebro'
NETEX_API_KEY = os.getenv('NETEX_API_KEY')
NETEX_API_URL = f"https://opendata.samtrafiken.se/netex/{operator}/{operator}.zip?key={NETEX_API_KEY}"

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
                # Safely handle nested text
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
                # Convert order to float to handle decimals
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

def process_xml_file(zip_file: zipfile.ZipFile, filename: str) -> dict:
    """
    Process a single XML file from the ZIP archive.
    Returns a dict with all parsed dataframes for this file.
    """
    try:
        data = zip_file.read(filename)
        root = etree.fromstring(data)
        
        return {
            'scheduled': parse_scheduled_stop_points(root),
            'topographic': parse_topographic_places(root),
            'stopplace': parse_stop_places_and_quays(root)[0],
            'quays': parse_stop_places_and_quays(root)[1],
            'lines': parse_lines_and_journey_patterns(root)[0],
            'journey_patterns': parse_lines_and_journey_patterns(root)[1],
            'jp_stops': parse_lines_and_journey_patterns(root)[2]
        }
    except etree.XMLSyntaxError as e:
        logger.warning(f"XML parsing error in {filename}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error processing {filename}: {e}")
        return None

def get_static_netex(**ctx):
    """
    Download and process NeTEx data with memory-efficient streaming.
    """
    try:
        logger.info("Downloading NeTEx ZIP file...")
        zip_bytes = download_zip(NETEX_API_URL)
        
        # Initialize accumulators for each data type
        scheduled_dfs = []
        topographic_dfs = []
        stopplace_dfs = []
        quays_dfs = []
        lines_dfs = []
        jps_dfs = []
        jp_stops_dfs = []
        
        # Process files one at a time
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
        
        if not any([scheduled_dfs, topographic_dfs, stopplace_dfs, quays_dfs, 
                    lines_dfs, jps_dfs, jp_stops_dfs]):
            logger.warning("No data extracted from any XML files")
            return
        
        # Prepare output directory
        out_dir = os.path.join(os.getcwd(), 'data', 'static')
        os.makedirs(out_dir, exist_ok=True)
        ts = pd.Timestamp.utcnow().strftime('%Y%m%dT%H%M%SZ')
        
        def _concat_and_write(dfs, name, dedup_cols=None):
            """Concatenate dataframes and write to CSV with deduplication."""
            if not dfs:
                logger.info(f"No data for {name}")
                return
            
            all_df = pd.concat(dfs, ignore_index=True)
            
            # Handle deduplication - keep latest version
            if dedup_cols:
                if 'version' in all_df.columns:
                    # Sort by version (descending) and keep first (latest)
                    all_df = all_df.sort_values('version', ascending=False)
                all_df = all_df.drop_duplicates(subset=dedup_cols, keep='first')
            else:
                all_df = all_df.drop_duplicates()
            
            path = os.path.join(out_dir, f"{name}_{ts}.csv")
            all_df.to_csv(path, index=False)
            logger.info(f"Wrote {len(all_df)} rows to {path}")
            return all_df, path
        
        # Write all datasets
        _concat_and_write(scheduled_dfs, "netex_scheduled_stop_points", dedup_cols=['id'])
        _concat_and_write(topographic_dfs, "netex_topographic_places", dedup_cols=['id'])
        _concat_and_write(stopplace_dfs, "netex_stop_places", dedup_cols=['id'])
        _concat_and_write(quays_dfs, "netex_quays", dedup_cols=['id'])
        _concat_and_write(lines_dfs, "netex_lines", dedup_cols=['id'])
        _concat_and_write(jps_dfs, "netex_journey_patterns", dedup_cols=['id'])
        _concat_and_write(jp_stops_dfs, "netex_journey_pattern_stops")
        
        logger.info("NeTEx static data processing completed successfully")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading NeTEx ZIP: {e}")
        raise
    except Exception as e:
        logger.error(f"Error processing NeTEx data: {e}")
        raise

with DAG(
    dag_id="get_static_netex",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1
) as dag:

    fetch_static = PythonOperator(
        task_id="get_static_netex",
        python_callable=get_static_netex,
        provide_context=True
    )