WITH CorrelationData AS (
    SELECT 
        w.CITY_NAME,
        DATE_TRUNC('DAY', w.DATE_TIME) AS DAY,
        AVG(w.TEMP_FAHRENHEIT) AS AVG_TEMP,
        AVG(w.HUMIDITY) AS AVG_HUMIDITY,
        AVG(w.PRESSURE) AS AVG_PRESSURE,
        AVG(w.WIND_SPEED) AS AVG_WIND_SPEED
    FROM
        {{ ref('openweather_join') }} w
    GROUP BY 
        1, 2
)
SELECT *
FROM CorrelationData