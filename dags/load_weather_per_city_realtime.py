import pandas as pd
import requests
from airflow.decorators import task
from airflow.models import Variable
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

import datetime
import time

def return_snowflake_engine():
    snowflake_url = URL(
        user=Variable.get('SNOWFLAKE_USER'),
        password=Variable.get('SNOWFLAKE_PASSWORD'),
        account=Variable.get('SNOWFLAKE_ACCOUNT'),
        warehouse='compute_wh',
        database='openweather',
        schema='raw_data'
    )
    return create_engine(snowflake_url)

@task
def get_city_data():
    query = 'select * from CITY_DIMENSION_TABLE'
    engine = return_snowflake_engine()
    cities_df = pd.read_sql(query, engine)
    return cities_df

@task
def get_weather_data(cities_df):
    key = Variable.get('OPENWEATHER_API_KEY')
    weather_data = []

    for index, row in cities_df.iterrows():
        city_id = row['city_id']
        city_name = row['Name']
        lat = row['Latitude']
        lon = row['Longitude']

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

@task
def load_weather_data(weather_df):
    engine = return_snowflake_engine()
    connection = engine.connect()
    try:
        transaction = connection.begin()    # Start Transaction (Similar to BEGIN)
        weather_df.to_sql('weather_realtime_table', con=connection, index=False, if_exists='replace')
        transaction.commit()    # Commit Transaction (Similar to COMMIT)
    except Exception as e:
        transaction.rollback()
        print(f"Error occurred: {e}")
    finally:
        connection.close()      # Close the connection

# Example DAG definition
from airflow import DAG

with DAG(
    'load_weather_per_city_realtime',
    start_date= datetime.datetime(2024,8,1),
    schedule_interval='@hourly',
    tags=['ETL', 'RealTime'],
    catchup=False
) as dag:

    cities_df = get_city_data()
    weather_df = get_weather_data(cities_df)
    load_weather_data(weather_df)