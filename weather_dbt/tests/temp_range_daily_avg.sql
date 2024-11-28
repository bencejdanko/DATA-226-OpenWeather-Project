-- temperature range
SELECT *
FROM {{ ref('daily_avg_weather_trend') }}
WHERE AVG_TEMP_FAHRENHEIT < -50 OR AVG_TEMP_FAHRENHEIT > 130