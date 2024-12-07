import pandas as pd
import requests
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import datetime
import time


# Function to return a Snowflake cursor
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn().cursor()


# Task to fetch city data from Snowflake
@task
def get_city_data():
    query = 'SELECT * FROM OPENWEATHER.RAW_DATA.CITY_DIMENSION_TABLE'
    cursor = return_snowflake_conn()
    cursor.execute(query)
    data = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    cities_df = pd.DataFrame(data, columns=columns)
    
    # Normalizing column names to lowercase
    cities_df.columns = cities_df.columns.str.lower()
    print("Retrieved columns:", cities_df.columns)
    
    return cities_df


# Task to fetch weather data from OpenWeather API
@task
def get_weather_data(cities_df):
    key = Variable.get('weather_api_key')
    weather_data = []

    for _, row in cities_df.iterrows():
        city_id = row['city_id']
        city_name = row['name']
        lat = row['latitude']
        lon = row['longitude']

        url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&APPID={key}'
        response = requests.get(url)
        weather_json = response.json()

        weather_info = {
            'city_id': city_id,
            'date_time': pd.to_datetime(weather_json.get('dt', None), unit='s'),
            'temp': weather_json.get('main', {}).get('temp', None),
            'feels_like': weather_json.get('main', {}).get('feels_like', None),
            'pressure': weather_json.get('main', {}).get('pressure', None),
            'humidity': weather_json.get('main', {}).get('humidity', None),
            'wind_speed': weather_json.get('wind', {}).get('speed', None),
            'cloud_coverage': weather_json.get('clouds', {}).get('all', None),
            'weather_main': weather_json.get('weather', [{}])[0].get('main', ''),
            'weather_det': weather_json.get('weather', [{}])[0].get('description', '')
        }

        weather_data.append(weather_info)
        time.sleep(0.2)  # To limit the API call to 1 per second

    weather_df = pd.DataFrame(weather_data)
    return weather_df


# Task to load weather data into Snowflake
@task
def load_weather_data(weather_df):
    cursor = return_snowflake_conn()

    try:
        # Begin transaction
        cursor.execute("BEGIN")
        
        # Insert data into Snowflake
        for _, row in weather_df.iterrows():
            insert_query = f"""
                INSERT INTO weather_realtime_table (city_id, date_time, temp, feels_like, pressure, humidity, wind_speed, cloud_coverage, weather_main, weather_det)
                VALUES (
                    '{row['city_id']}',
                    '{row['date_time']}',
                    {row['temp']},
                    {row['feels_like']},
                    {row['pressure']},
                    {row['humidity']},
                    {row['wind_speed']},
                    {row['cloud_coverage']},
                    '{row['weather_main']}',
                    '{row['weather_det']}'
                )
            """
            cursor.execute(insert_query)
        
        # Commit transaction
        cursor.execute("COMMIT")
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(f"Error occurred: {e}")
    finally:
        cursor.close()


# Example DAG definition
from airflow import DAG

with DAG(
    'load_weather_per_city_realtime',
    start_date=datetime.datetime(2024, 9, 1),
    schedule_interval='@hourly',
    tags=['ETL', 'RealTime'],
    catchup=False
) as dag:

    cities_df = get_city_data()
    weather_df = get_weather_data(cities_df)
    load_weather_data(weather_df)
