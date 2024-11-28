-- humidity range
SELECT *
FROM {{ ref('daily_avg_weather_trend') }}
WHERE AVG_HUMIDITY < 0 OR AVG_HUMIDITY > 100