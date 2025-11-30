from typing import Tuple, Dict, Optional
import pandas as pd
import zipfile
from lxml import etree

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

def _local_name(el):
    """Extract local name from namespaced tag."""
    return el.tag.split('}')[-1] if isinstance(el.tag, str) else el.tag

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
                        # 'stopPlaceId': el_dict['id'],
                        # 'name': el_dict['Name'],
                        # 'Centroid_Long': el_dict['Centroid_Long'],
                        # 'Centroid_Lat': el_dict['Centroid_Lat']
                    })
        stops_list.append(el_dict)
    return pd.DataFrame(stops_list), pd.DataFrame(quays)

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