import functions_framework
import requests
import os
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timezone, timedelta

@functions_framework.http
def hello_http(request):

    #Initializing all variables
    url = "https://creativecommons.tankerkoenig.de/json/list.php"
    cities = {
        "Berlin": {
            "lat": 52.52560,
            "lng": 13.40683
        },
        "Munich": {
            "lat": 48.13397,
            "lng": 11.57309
        },
        "Frankfurt": {
            "lat": 50.11052,
            "lng": 8.67294
        },
        "Stuttgart": {
            "lat": 48.77562,
            "lng": 9.18193
        },
        "Dortmund": {
            "lat": 51.51246,
            "lng": 7.46475
        }
    }
    rad=25
    type="all"
    apikey = os.environ.get("apikey")
    project_id="tanker-koenig-de-project"
    live_table_id="live_fuel_data"
    history_table_id="history_fuel_data"
    table_id = "fuel_data"
    dataset_id = "fuel_dataset"

    column_names = ["unique_id", "timestamp", "station_id", "station_name", "brand", "street", "place", "latitude", "longitude", "distance", "diesel_price", "e5_price", "e10_price", "isOpen", "house_number", "pincode"]
    fuel_df = pd.DataFrame(columns=column_names)

    #Looping through city coordinates and fetching data
    #Storing data from json to pandas dataframe
    for coords in cities.values():
        parameters = {
            'lat': coords["lat"],
            'lng': coords["lng"],
            'rad': rad,
            'type': type,
            'apikey': apikey
        }
        response = requests.get(url=url, params=parameters)

        raw_json = response.json()

        if raw_json["ok"]:
            stations = raw_json["stations"]
            stations_list = []
            for station in stations:
                # timestamp = datetime.now().timestamp()
                timestamp = datetime.now()
                formatted_time = datetime.fromtimestamp(timestamp.timestamp()).strftime('%Y-%m-%d %H:%M:%S.%f')
                unique_id = station["id"] + "_" + formatted_time
                station_row = []
                station_row.append(unique_id)
                station_row.append(timestamp)
                for key, value in station.items():
                    station_row.append(value)
                stations_list.append(station_row)
            
            stations_df = pd.DataFrame(stations_list, columns=column_names)
            fuel_df = pd.concat([fuel_df, stations_df], ignore_index=True)

            client = bigquery.Client(project=project_id)
            dataset_ref = client.dataset(dataset_id)
            live_table_ref = dataset_ref.table(live_table_id)
            history_table_ref = dataset_ref.table(history_table_id)

            # Define the schema explicitly with correct types
            schema = [
                bigquery.SchemaField("unique_id", "STRING"),
                bigquery.SchemaField("timestamp", "TIMESTAMP"),
                bigquery.SchemaField("station_id", "STRING"),
                bigquery.SchemaField("station_name", "STRING"),
                bigquery.SchemaField("brand", "STRING"),
                bigquery.SchemaField("street", "STRING"),
                bigquery.SchemaField("place", "STRING"),
                bigquery.SchemaField("latitude", "FLOAT"),
                bigquery.SchemaField("longitude", "FLOAT"),
                bigquery.SchemaField("distance", "FLOAT"),
                bigquery.SchemaField("diesel_price", "FLOAT"),
                bigquery.SchemaField("e5_price", "FLOAT"),
                bigquery.SchemaField("e10_price", "FLOAT"),
                bigquery.SchemaField("isOpen", "BOOLEAN"),
                bigquery.SchemaField("house_number", "STRING"),
                bigquery.SchemaField("pincode", "INTEGER")
            ]
        
            live_table_exists = False
            history_table_exists = False

            #LIVE TABLE
            try:
                live_table = client.get_table(live_table_ref)
                live_table_exists = True
                print(f"Table {live_table_id} already exists.")                
            except Exception as e:
                print(f"Table {live_table_id} does not exist. Creating it now.")
                live_table = bigquery.Table(live_table_ref, schema=schema)
                live_table = client.create_table(live_table)
                print(f"Table {live_table_id} created successfully with schema: {schema}")

            #HISTORY TABLE
            try:
                history_table = client.get_table(history_table_ref)
                history_table_exists = True
                print(f"Table {history_table_id} already exists.")                
            except Exception as e:
                print(f"Table {history_table_id} does not exist. Creating it now.")
                history_table = bigquery.Table(history_table_ref, schema=schema)
                history_table = client.create_table(history_table)
                print(f"Table {history_table_id} created successfully with schema: {schema}")

            copy_query = f"""
                INSERT INTO `{project_id}.{dataset_id}.{history_table_id}`
                SELECT * FROM `{project_id}.{dataset_id}.{live_table_id}`;
            """
            copy_job = client.query(copy_query)
            # Wait for the copy job to complete
            copy_job.result()

            #Insert data into live table with 'replace' option
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            job = client.load_table_from_dataframe(fuel_df, live_table_ref, job_config=job_config)

            # Wait for the job to complete
            job.result()

            return "Data replaced/inserted successfully."
        else:
            return "No data"