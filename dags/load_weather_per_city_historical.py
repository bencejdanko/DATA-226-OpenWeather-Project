import pandas as pd
import requests
from airflow.decorators import task
from airflow.models import Variable
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from airflow.operators.python import get_current_context

import datetime, timedelta
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

def get_logical_date():
    # Get the current Airflow context
    context = get_current_context()
    
    # Extract the logical_date (the scheduled run date)
    logical_date = context['logical_date'].date()
    
    # Set start and end dates for a 1 day range
    start_date = logical_date + timedelta(days=1)
    end_date = logical_date

    # Convert to Unix timestamps
    start = int(datetime.combine(start_date, datetime.min.time()).timestamp())
    end = int(datetime.combine(end_date, datetime.min.time()).timestamp())

    return start, end

@task
def get_city_data():
    query = 'select * from CAL_CITIES_LAT_LONG'
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

        start, end = get_logical_date()

        url = f'https://history.openweathermap.org/data/2.5/history/city?lat={lat}&lon={lon}&type=hour&start={start}&end={end}&appid={key}'

        response = requests.get(url)
        weather_json = response.json()

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
            time.sleep(1)  # To limit the API call to 1 per second

    weather_df = pd.DataFrame(weather_data)
    return weather_df

@task
def load_weather_data(weather_df):
    engine = return_snowflake_engine()
    weather_df.to_sql('weather_fact_table', con=engine, index=False, if_exists='append')

# Example DAG definition
from airflow import DAG

with DAG(
    'load_weather_per_city_historical',
    start_date= datetime.datetime(2024,10,15),
    schedule_interval='@daily',
    catchup=False
) as dag:

    cities_df = get_city_data()
    weather_df = get_weather_data(cities_df)
    load_weather_data(weather_df)

    #cities_df >> weather_df >> load_weather_data