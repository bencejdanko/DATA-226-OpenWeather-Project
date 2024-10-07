import pandas as pd
import os

import requests

from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData, Float


key = os.getenv('OPENWEATHER_API_KEY')

snowflake_url = URL(
    user= os.getenv('SNOWFLAKE_USER'),
    password= os.getenv('SNOWFLAKE_PASSWORD'),
    account= os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse='compute_wh',
    database='openweather',
    schema='raw_data'
)

engine = create_engine(snowflake_url)

query = 'select * from CAL_CITIES_LAT_LONG limit 10'
cities_df = pd.read_sql(query, engine)
print(cities_df.head())

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

weather_df = pd.DataFrame(weather_data)
weather_df.to_sql('weather_data', con=engine, index=False, if_exists='append')