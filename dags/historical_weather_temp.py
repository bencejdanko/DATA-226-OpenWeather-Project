from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import get_current_context

from datetime import datetime, timedelta
import requests

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    return hook.get_conn()

def get_logical_date():
    # Get the current Airflow context
    context = get_current_context()
    
    # Extract the logical_date (the scheduled run date)
    logical_date = context['logical_date'].date()
    
    # Set start and end dates for a 90 day range
    start_date = logical_date - timedelta(days=90)
    end_date = logical_date

    # Convert to Unix timestamps
    start = int(datetime.combine(start_date, datetime.min.time()).timestamp())
    end = int(datetime.combine(end_date, datetime.min.time()).timestamp())

    return start, end

@task
def extract():
    start, end = get_logical_date()
    url = f'https://history.openweathermap.org/data/2.5/history/city?lat={41.85}&lon={-87.65}&type=hour&start={start}&end={end}&appid={project_api_key}'

    r = requests.get(url)
    data = r.json()
    return (data)

@task
def transform(data):
    cleaned_data = []

    # Loop through each weather record in the 'list'
    for record in data['list']:
        # Convert the timestamp to a readable format
        dt = datetime.utcfromtimestamp(record['dt']).strftime('%Y-%m-%d %H:%M:%S')

        # Extract temperature, weather description, and other details
        temp = round(record['main']['temp'] - 273.15, 2)
        feels_like = round(record['main']['feels_like'] - 273.15, 2)
        pressure = record['main']['pressure']
        humidity = record['main']['humidity']
        wind_speed = record['wind']['speed']
        wind_dir = record['wind'].get('gust', None)
        cloud_coverage = record['clouds']['all']
        weather_main = record['weather'][0]['main']
        weather_det = record['weather'][0]['description']
        weather_icon = record['weather'][0]['icon']

        # Create a dictionary for each record
        cleaned_record = {
            'datetime': dt,
            'temp': temp,
            'feels_like': feels_like,
            'pressure': pressure,
            'humidity': humidity,
            'wind_speed': wind_speed,
            'wind_dir': wind_dir,
            'cloud_coverage': cloud_coverage,
            'weather_main': weather_main,
            'weather_det': weather_det,
            'weather_icon': weather_icon
        }

        # Append the cleaned record to the new list
        cleaned_data.append(cleaned_record)

    return cleaned_data

@task
def load(lines, target_table):
    # Sort the lines by datetime
    #sorted_lines = sorted(lines, key=lambda x: x["datetime"])

    conn = return_snowflake_conn()
    cur = conn.cursor()

    try:
        cur.execute("BEGIN;")
        # Create table only if it doesn't exist (keeps old data)
        cur.execute(f"""
        CREATE OR REPLACE TABLE {target_table} (
            datetime TIMESTAMP PRIMARY KEY,
            temp DECIMAL(10, 2) NOT NULL,
            feels_like DECIMAL(10, 2) NOT NULL,
            pressure INT NOT NULL,
            humidity INT NOT NULL,
            wind_speed DECIMAL(10, 4) NOT NULL,
            wind_dir DECIMAL(10, 4),
            cloud_coverage INT NOT NULL,
            weather_main VARCHAR(255) NOT NULL,
            weather_det VARCHAR(255) NOT NULL,
            weather_icon VARCHAR(255) NOT NULL
        )
        """)

        # Prepare the MERGE query for incremental updates
        for r in lines:
            datetime = r["datetime"]
            temp = r["temp"]
            feels_like = r["feels_like"]
            pressure = r["pressure"]
            humidity = r["humidity"]
            wind_speed = r["wind_speed"]
            wind_dir = r["wind_dir"]
            cloud_coverage = r["cloud_coverage"]
            weather_main = r["weather_main"]
            weather_det = r["weather_det"]
            weather_icon = r["weather_icon"]

            insert_query = f"""
            INSERT INTO {target_table}
                (datetime, temp, feels_like, pressure, humidity, wind_speed, wind_dir, cloud_coverage, weather_main, weather_det, weather_icon)
                VALUES ( '{datetime}',
                    {temp},
                    {feels_like},
                    {pressure},
                    {humidity},
                    {wind_speed},
                    {wind_dir if wind_dir is not None else 'NULL'},
                    {cloud_coverage},
                    '{weather_main}',
                    '{weather_det}',
                    '{weather_icon}'
                )
            """

            cur.execute(insert_query)

        # Commit the transaction
        cur.execute("COMMIT;")

    except Exception as e:
        # Rollback in case of error
        cur.execute("ROLLBACK;")
        print(f"Error: {e}")
        raise e

    finally:
        # Close the cursor and connection
        cur.close()
        conn.close()

with DAG(
    dag_id = 'historical_weather_temp',
    start_date = datetime(2024,11,1),
    catchup=False,
    tags=['ETL','Project', 'Full Refresh'],
    schedule = None
) as dag:
    target_table = "project.raw_data.historical_weather_temp"
    project_api_key = Variable.get("project_api_key")
    
    #Tasks
    data = extract()
    lines = transform(data)
    load(lines, target_table)