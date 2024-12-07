from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context

import pandas as pd
import requests
import datetime
from datetime import timedelta
import time


# Function to return a Snowflake cursor
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')  # Use the Airflow connection ID
    return hook.get_conn().cursor()


# Function to calculate logical date range
def get_logical_date():
    context = get_current_context()
    logical_date = context['logical_date']
    start_date = logical_date
    end_date = logical_date + timedelta(days=1)
    start = int(datetime.datetime(start_date.year, start_date.month, start_date.day).timestamp())
    end = int(datetime.datetime(end_date.year, end_date.month, end_date.day).timestamp()) - 1
    return start, end


# Task to fetch city data from Snowflake
@task
def get_city_data():
    query = 'SELECT * FROM CITY_DIMENSION_TABLE'
    cursor = return_snowflake_conn()

    cursor.execute(query)
    data = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    cities_df = pd.DataFrame(data, columns=columns)
    
    # Normalize column names to lowercase
    cities_df.columns = cities_df.columns.str.lower()
    
    print("City Data Columns:", cities_df.columns)  # Debugging
    return cities_df


# Task to fetch weather data from OpenWeather API
@task
def get_weather_data(cities_df):
    key = Variable.get('weather_api_key')
    weather_data = []

    for index, row in cities_df.iterrows():
        city_id = row['city_id']
        city_name = row['name']
        lat = row['latitude']
        lon = row['longitude']

        start, end = get_logical_date()

        url = f'https://history.openweathermap.org/data/2.5/history/city?lat={lat}&lon={lon}&type=hour&start={start}&end={end}&appid={key}'
        time.sleep(0.2)  # Limit the API calls to 1 per second
        response = requests.get(url)
        weather_json = response.json()

        if 'list' in weather_json and weather_json['list']:
            for record in weather_json['list']:
                weather_info = {
                    'city_id': city_id,
                    'date_time': pd.to_datetime(record['dt'], unit='s'),
                    'temp': record['main']['temp'],
                    'feels_like': record['main']['feels_like'],
                    'pressure': record['main']['pressure'],
                    'humidity': record['main']['humidity'],
                    'wind_speed': record['wind']['speed'],
                    'cloud_coverage': record['clouds']['all'],
                    'weather_main': record['weather'][0]['main'],
                    'weather_det': record['weather'][0]['description']
                }
                weather_data.append(weather_info)
        else:
            print(f"No data found for {city_name} at {start} to {end}")
            print(weather_json)
            break

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
                INSERT INTO weather_fact_table (city_id, date_time, temp, feels_like, pressure, humidity, wind_speed, cloud_coverage, weather_main, weather_det)
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


# DAG Definition
from airflow import DAG

with DAG(
    'load_weather_per_city_historical',
    start_date=datetime.datetime(2024, 9, 1),
    schedule_interval='@daily',
    tags=['ETL', 'Historical'],
    catchup=True
) as dag:

    cities_df = get_city_data()
    weather_df = get_weather_data(cities_df)
    load_weather = load_weather_data(weather_df)

    cities_df >> weather_df >> load_weather
