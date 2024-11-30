-- checking valid temperature values for extreme_weather_events.sql
SELECT
    CITY_ID,
    DATE_TIME,
    TEMP_FAHRENHEIT
FROM {{ ref('extreme_weather_events') }}
WHERE TEMP_FAHRENHEIT IS NULL
   OR TEMP_FAHRENHEIT < -100 OR TEMP_FAHRENHEIT > 150