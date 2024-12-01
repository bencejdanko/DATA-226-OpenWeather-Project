#importing necessary libraries
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from datetime import datetime, timedelta
import requests
import pandas as pd
import time

#snowflake conn function
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

#defining dag dependicies
with DAG(
    dag_id="historical_weather_etl_v3",
    start_date=datetime(2024, 11, 30),
    schedule_interval="@hourly",
    tags=["weather", "ETL", "California"],
    catchup=False,
) as dag:

    @task()
    def fetch_california_cities():
        # Top 10 Northern California cities
        northern_california_cities = [
            {"city_id": 1, "name": "San Francisco", "lat": 37.7749, "lon": -122.4194},
            {"city_id": 2, "name": "Sacramento", "lat": 38.5816, "lon": -121.4944},
            {"city_id": 3, "name": "Oakland", "lat": 37.8044, "lon": -122.2711},
            {"city_id": 4, "name": "San Jose", "lat": 37.3382, "lon": -121.8863},
            {"city_id": 5, "name": "Fremont", "lat": 37.5483, "lon": -121.9886},
            {"city_id": 6, "name": "Santa Rosa", "lat": 38.4405, "lon": -122.7144},
            {"city_id": 7, "name": "Berkeley", "lat": 37.8715, "lon": -122.2730},
            {"city_id": 8, "name": "Sunnyvale", "lat": 37.3688, "lon": -122.0363},
            {"city_id": 9, "name": "Santa Clara", "lat": 37.3541, "lon": -121.9552},
            {"city_id": 10, "name": "Hayward", "lat": 37.6688, "lon": -122.0808},
        ]

        # Top 10 Southern California cities
        southern_california_cities = [
            {"city_id": 11, "name": "Los Angeles", "lat": 34.0522, "lon": -118.2437},
            {"city_id": 12, "name": "San Diego", "lat": 32.7157, "lon": -117.1611},
            {"city_id": 13, "name": "Anaheim", "lat": 33.8366, "lon": -117.9143},
            {"city_id": 14, "name": "Long Beach", "lat": 33.7701, "lon": -118.1937},
            {"city_id": 15, "name": "Santa Ana", "lat": 33.7455, "lon": -117.8677},
            {"city_id": 16, "name": "Irvine", "lat": 33.6846, "lon": -117.8265},
            {"city_id": 17, "name": "Chula Vista", "lat": 32.6401, "lon": -117.0842},
            {"city_id": 18, "name": "Glendale", "lat": 34.1425, "lon": -118.2551},
            {"city_id": 19, "name": "Huntington Beach", "lat": 33.6595, "lon": -117.9988},
            {"city_id": 20, "name": "Oceanside", "lat": 33.1959, "lon": -117.3795},
        ]

        # Combine all cities, removing duplicates if any
        all_cities = {city["name"]: city for city in (northern_california_cities + southern_california_cities)}
        unique_cities = list(all_cities.values())

        print(f"Selected {len(unique_cities)} unique cities for processing.")
        return unique_cities

    @task()
    def extract_historical_weather_data(cities):
        historical_data = []
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=90)

        for city in cities:
            current_start_date = start_date
            while current_start_date < end_date:
                current_end_date = current_start_date + timedelta(days=5)
                if current_end_date > end_date:
                    current_end_date = end_date

                start_timestamp = int(current_start_date.timestamp())
                end_timestamp = int(current_end_date.timestamp())

                api_key = Variable.get('weather_api_key', default_var=None)
                if not api_key:
                    raise ValueError("API key for weather data is not set in Airflow Variables.")

                url = f"https://history.openweathermap.org/data/2.5/history/city?lat={city['lat']}&lon={city['lon']}&type=hour&start={start_timestamp}&end={end_timestamp}&appid={api_key}"
                response = requests.get(url)
                data = response.json()

                if response.status_code == 200 and "list" in data:
                    for record in data["list"]:
                        weather_info = {
                            'city_id': city["city_id"],
                            'city_name': city["name"],
                            'date_time': pd.to_datetime(record.get('dt', None), unit='s').strftime('%Y-%m-%d %H:%M:%S'),
                            'temp': record.get('main', {}).get('temp', None),
                            'feels_like': record.get('main', {}).get('feels_like', None),
                            'pressure': record.get('main', {}).get('pressure', None),
                            'humidity': record.get('main', {}).get('humidity', None),
                            'wind_speed': record.get('wind', {}).get('speed', None),
                            'cloud_coverage': record.get('clouds', {}).get('all', None),
                            'weather_main': record.get('weather', [{}])[0].get('main', ''),
                            'weather_det': record.get('weather', [{}])[0].get('description', ''),
                        }
                        historical_data.append(weather_info)
                    print(f"Data extracted for {city['name']} from {current_start_date} to {current_end_date}")
                else:
                    print(f"Failed to fetch data for {city['name']} from {current_start_date} to {current_end_date}: {data}")

                current_start_date = current_end_date
                time.sleep(1)

        return historical_data

    @task()
    def load_data_to_snowflake(historical_data):
        df = pd.DataFrame(historical_data)

        if df.empty:
            print("No data to load into Snowflake.")
            return

        engine = return_snowflake_engine()

        with engine.connect() as conn:
            transaction = conn.begin() #begin sql transcation
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS weather_info (
                        city_id INTEGER,
                        city_name STRING,
                        date_time TIMESTAMP,
                        temp FLOAT,
                        feels_like FLOAT,
                        pressure FLOAT,
                        humidity FLOAT,
                        wind_speed FLOAT,
                        cloud_coverage FLOAT,
                        weather_main STRING,
                        weather_det STRING
                    );
                """)

                df.to_sql(
                    "weather_info",
                    con=conn,
                    schema="raw_data",
                    if_exists="append",
                    index=False
                )
                transaction.commit() #commiting if success
                print("Data loaded successfully into Snowflake.")
            except Exception as e:
                transaction.rollback() #rollback to prev state if failed for any reason
                print(f"Error occurred while loading data to Snowflake: {e}")

    cities = fetch_california_cities()
    historical_data = extract_historical_weather_data(cities)
    load_data_to_snowflake(historical_data)
