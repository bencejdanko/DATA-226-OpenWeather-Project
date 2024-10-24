import pandas as pd
import os
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

# Load the CSV file into a DataFrame
cal_cities_lat_long = pd.read_csv('data/cal_cities_lat_long.csv')

# Generate a new ID for each city
cal_cities_lat_long['city_id'] = range(1, len(cal_cities_lat_long) + 1)

# Snowflake connection URL
snowflake_url = URL(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse='compute_wh',
    database='openweather',
    schema='raw_data'
)

# Create SQLAlchemy engine
engine = create_engine(snowflake_url)

# Upload the DataFrame to Snowflake
cal_cities_lat_long.to_sql('city_dimension_table', con=engine, index=False, if_exists='replace')