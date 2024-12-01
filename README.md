# DATA-226-OpenWeather-Project

## Project Directory Structure

### `dags/`
Contains Apache Airflow DAGs for data extraction and processing:
- **`load_weather_per_city_historical.py`**: Loads historical weather data per city (ETL).
- **`load_weather_per_city_realtime.py`**: Loads real-time weather data per city (ETL).
- **`build_elt_with_dbt.py`**: Implements ELT processes using dbt.

### `scripts/`
Includes Python scripts and related resources:
- **`data/`**: Stores data files:
  - `cal_cities_lat_long.csv`: Latitude and longitude information for California cities.
  - `current_weather_response.json`: Sample API response for current weather.
  - `historical_weather_response.json`: Sample API response for historical weather.
- **`load_cities.py`**: Script to load city data.
- **`load_weather_per_city.py`**: Script to fetch and load weather data per city.
- **`requirements.txt`**: Python dependencies.

### `docker-compose-min.yaml`
Configuration file for setting up Docker containers for the project.

### `weather_dbt/`
Folder for dbt models and configurations:
- **`models/`**: 
  - **`input/`**:
    - `openweather_join.sql`: SQL for joining raw OpenWeather Historical data.
    - `realtime_weather_join.sql`: SQL for joining raw OpenWeather Realtime data.

  - **`output/`**:
    - `cloud_coverage_over_time.sql`: Analyzes cloud coverage trends.
    - `correlation.sql`: Examines correlations between weather variables.
    - `extreme_weather_events.sql`: Identifies extreme weather occurrences.
    - `humidity_vs_temp.sql`: Compares humidity and temperature patterns.
    - `temp_trends_by_city.sql`: Tracks temperature trends for each city.
    - `wind_speed_analysis.sql`: Studies wind speed variations.
  - `schema.yml`: dbt tests and schema definitions.
  - `sources.yml`: Configuration for data sources.
- **`snapshots/`**:
  - `cloud_coverage_over_time_snapshot.sql`: Snapshot for cloud coverage analysis.
  - `correlations_snapshot.sql`: Snapshot for correlations between variables.
  - `extreme_weather_events_snapshot.sql`: Snapshot for extreme weather occurrences.
  - `humidity_vs_temp_snapshot.sql`: Snapshot for humidity vs temperature analysis.
  - `temp_trends_by_city_snapshot.sql`: Snapshot for city-specific temperature trends.
  - `wind_speed_analysis_snapshot.sql`: Snapshot for wind speed trends.
- **`tests/`**:
  - `valid_temp.sql`: Validates temperature data consistency.
  - `weather_alert_consistency.sql`: Tests consistency in weather alerts.
- **`dbt_project.yml`**: Main dbt project configuration file.
- **`profiles.yml`**: dbt profile for database connection settings.
