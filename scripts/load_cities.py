import pandas as pd
import os

from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData, Float

cal_cities_lat_long = pd.read_csv('data/cal_cities_lat_long.csv')

snowflake_url = URL(
    user= os.getenv('SNOWFLAKE_USER'),
    password= os.getenv('SNOWFLAKE_PASSWORD'),
    account= os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse='compute_wh',
    database='openweather',
    schema='raw_data'
)

engine = create_engine(snowflake_url)

cal_cities_lat_long.to_sql('cal_cities_lat_long', con=engine, index=False, if_exists='replace')