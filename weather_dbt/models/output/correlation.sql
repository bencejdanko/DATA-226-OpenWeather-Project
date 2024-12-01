WITH CorrelationData AS (
    SELECT 
        c."Name" AS CITY_NAME,
        DATE_TRUNC('DAY', w.DATE_TIME) AS DAY,
        AVG(((w.TEMP - 273.15) * 9/5 + 32)) AS AVG_TEMP,
        AVG(w.HUMIDITY) AS AVG_HUMIDITY,
        AVG(w.PRESSURE) AS AVG_PRESSURE,
        AVG(w.WIND_SPEED) AS AVG_WIND_SPEED
    FROM
        {{ source('openweather', 'WEATHER_FACT_TABLE') }} w
    JOIN
        {{ source('openweather', 'CITY_DIMENSION_TABLE') }} c
    ON
        w.CITY_ID = c.CITY_ID
    GROUP BY 
        1, 2
)
SELECT *
FROM CorrelationData