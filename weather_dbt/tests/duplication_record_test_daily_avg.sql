-- duplicate record test
SELECT CITY_ID, WEATHER_DATE, COUNT(*)
FROM {{ ref('daily_avg_weather_trend') }}
GROUP BY CITY_ID, WEATHER_DATE
HAVING COUNT(*) > 1