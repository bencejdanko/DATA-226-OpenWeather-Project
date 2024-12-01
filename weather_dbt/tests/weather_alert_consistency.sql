-- This test checks for consistency in the weather alerts generated in the extreme_weather_events table.
SELECT
    CITY_NAME,
    DAY,
    WEATHER_ALERT,
    AVG_TEMP_FAHRENHEIT,
    AVG_WIND_SPEED
FROM {{ ref('extreme_weather_events') }}
WHERE 
    (WEATHER_ALERT = 'Extreme Heat' AND AVG_TEMP_FAHRENHEIT <= 100)
    OR (WEATHER_ALERT = 'Extreme Cold' AND AVG_TEMP_FAHRENHEIT >= 32)
    OR (WEATHER_ALERT = 'High Winds' AND AVG_WIND_SPEED <= 20)