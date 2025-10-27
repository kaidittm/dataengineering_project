import requests
import zipfile
import io
import lxml
from lxml import etree
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd

operator = 'orebro'
SIRI_API_KEY = os.getenv('SIRI_API_KEY')

# API endpoint URL that returns a ZIP file
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

	# convert dates and times to correct dtypes
	df[['Date', 'AimedArrivalTime', 'ActualArrivalTime', 'AimedDepartureTime', 'ActualDepartureTime']] = df[['Date', 'AimedArrivalTime', 'ActualArrivalTime', 'AimedDepartureTime', 'ActualDepartureTime']].apply(pd.to_datetime, format='mixed')

	return df

def get_live_data(**ctx):
	try:
		# 1. Make the GET request to the API
		response = requests.get(api_url_live_data)
		response.raise_for_status()  # Raise an exception for bad status codes

		# 2. Get the binary content and save it to a temporary file just in case
		#os.makedirs('temp_data/input', exist_ok=True)
		#filename = 'temp_data/input/EstimatedTimetable_' + '_'.join(response.headers['Date'].split(' ')[1:]) + '.xml'
		#with open(filename, 'wb+') as file:
		#	file.write(response.content)
		
		# 3. Parse the xml, print small section of data and write output to csv on disk
		df = parse_est_timetable(etree.fromstring(response.content))

		print(df.head())

		# if the output folder does not exist yet, create it
		#os.makedirs('temp_data/output', exist_ok=True)  
		#df.to_csv('temp_data/output/' + '_'.join(response.headers['Date'].split(' ')[1:]) + '.csv', index=False)

	except requests.exceptions.RequestException as e:
		print(f"Error making API request: {e}")
								
	except Exception as e:
		print(f"Error parsing XML file {filename}: {e}")

with DAG(
	dag_id="get_live_data",
	start_date=days_ago(1),
	schedule_interval="*/1 * * * *",  # every minute
	catchup=False,
	max_active_runs=1
) as dag:

	fetch_task = PythonOperator(
		task_id="get_live_data",
		python_callable=get_live_data,
		provide_context=True
	)