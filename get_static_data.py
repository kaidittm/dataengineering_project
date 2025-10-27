'''
import requests
import zipfile
import io
import xml.etree.ElementTree as ET
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# API endpoint URL that returns a ZIP file
api_url_static_data = "https://opendata.samtrafiken.se/netex/orebro/orebro.zip?key={apikey}"

def get_orebro_static_data(**ctx):
    try:
        # 1. Make the GET request to the API
        response = requests.get(api_url_static_data)
        response.raise_for_status()  # Raise an exception for bad status codes

        # 2. Get the binary content and wrap it in an in-memory file
        zip_data = io.BytesIO(response.content)

        # 3. Use the zipfile module to read and extract the contents
        with zipfile.ZipFile(zip_data, 'r') as zip_ref:
            # Print a list of files contained in the zip archive
            print("Files in the ZIP archive:", zip_ref.namelist())
            list = zip_ref.namelist()
            print(list[0])
            # Extract all files to a specified directory
            # This will create the directory if it doesn't exist.
            extract_to_path = "data/static_data"   
            zip_ref.extractall(extract_to_path)
            print(f"All files extracted to: {extract_to_path}")
            
            # Alternatively, extract a single file
            # zip_ref.extract('path/to/my_file.csv', extract_to_path)

    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
    except zipfile.BadZipFile as e:
        print(f"Error processing ZIP file: {e}")

    # The path to the directory where you extracted the ZIP file
    #extract_to_path = "./extracted_content"
    extract_to_path = "data/static_data"   

    # Iterate over all files in the directory
    for filename in os.listdir(extract_to_path):
        if filename.endswith(".xml") and filename == "_stops.xml":
            filepath = os.path.join(extract_to_path, filename)
            print(f"--- Processing file: {filename} ---")
            
            try:
                # Parse the XML file
                tree = ET.parse(filepath)
                root = tree.getroot()
                
                # Print the root tag to understand the document structure
                print(f"Root tag: {root.tag}")
                topographic_place_tags = []

                for element in tree.iter('{http://www.netex.org.uk/netex}TopographicPlace'):
                    childrenTags = [child.tag.split('}')[-1] for child in element]
                    for tag in childrenTags:
                        if tag not in topographic_place_tags:
                            print("TAG: ", tag)
                            #topographic_place_tags.append(tag)  
                                
            except ET.ParseError as e:
                print(f"Error parsing XML file {filename}: {e}")

with DAG(
    dag_id="get_orebro_static_data",
    start_date=days_ago(1),
    schedule_interval="*/1 * * * *",  # every minute
    catchup=False,
    max_active_runs=1
) as dag:

    fetch_task = PythonOperator(
        task_id="get_orebro_static_data",
        python_callable=get_orebro_static_data,
        provide_context=True
    )

'''

return 0