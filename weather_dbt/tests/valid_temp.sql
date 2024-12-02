-- checking valid temperature values for extreme_weather_events.sql
SELECT
    CITY_NAME,
    DAY,
    AVG_TEMP_FAHRENHEIT
FROM {{ ref('extreme_weather_events') }}
WHERE AVG_TEMP_FAHRENHEIT IS NULL
   OR AVG_TEMP_FAHRENHEIT < -100 
   OR AVG_TEMP_FAHRENHEIT > 150