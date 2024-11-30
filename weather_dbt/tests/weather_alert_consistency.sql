-- This test checks for consistency in the weather alerts generated in the extreme_weather_events table.
SELECT
    CITY_ID,
    DATE_TIME,
    WEATHER_ALERT,
    TEMP_FAHRENHEIT,
    WIND_SPEED
FROM {{ ref('extreme_weather_events') }}
WHERE 
    (WEATHER_ALERT = 'Extreme Heat' AND TEMP_FAHRENHEIT <= 100)
    OR (WEATHER_ALERT = 'Extreme Cold' AND TEMP_FAHRENHEIT >= 32)
    OR (WEATHER_ALERT = 'High Winds' AND WIND_SPEED <= 20)