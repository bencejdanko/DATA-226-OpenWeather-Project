from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import pandas as pd
import requests
from datetime import datetime
import time

from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData, Float


def return_snowflake_engine():

    snowflake_url = URL(
        user= Variable.get('SNOWFLAKE_USER'),
        password= Variable.get('SNOWFLAKE_PASSWORD'),
        account= Variable.get('SNOWFLAKE_ACCOUNT'),
        warehouse='compute_wh',
        database='openweather',
        schema='raw_data'
    )

    engine = create_engine(snowflake_url)
    return engine


@task
def get_city_data():
    query = 'select * from CAL_CITIES_LAT_LONG limit 50'
    engine = return_snowflake_engine()
    cities_df = pd.read_sql(query, engine)
    return cities_df

@task
def get_weather_data(cities_df):
    key = Variable.get('OPENWEATHER_API_KEY')
    weather_data = []

    for index, row in cities_df.iterrows():
        city_name = row['Name']
        lat = row['Latitude']
        lon = row['Longitude']

        url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&APPID={key}'
        response = requests.get(url)
        weather_json = response.json()

        weather_info = {
            'City': city_name,
            'Latitude': lat,
            'Longitude': lon,
            'Weather': weather_json.get('weather', [{}])[0].get('description', ''),
            'Temperature': weather_json.get('main', {}).get('temp', None),
            'Humidity': weather_json.get('main', {}).get('humidity', None),
            'Wind_Speed': weather_json.get('wind', {}).get('speed', None),
            'Timestamp': pd.Timestamp.now()
        }

        weather_data.append(weather_info)
        time.sleep(2)

    weather_df = pd.DataFrame(weather_data)
    return weather_df

@task
def load_weather_data(weather_df):
    engine = return_snowflake_engine()
    weather_df.to_sql('weather_data', con=engine, index=False, if_exists='append')

with DAG(
    start_date = datetime(2024,10,5),
    dag_id='load_weather_per_city',
    description='Load weather data per city to Snowflake',
    schedule_interval='@hourly',
    catchup=False
) as dag:
    cities_df = get_city_data()
    weather_df = get_weather_data(cities_df)
    load_weather_data(weather_df)