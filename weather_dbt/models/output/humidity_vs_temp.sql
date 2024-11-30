WITH HumidityTemp AS (
    SELECT 
        ((w.TEMP - 273.15) * 9/5 + 32) AS TEMP_FAHRENHEIT,
        w.HUMIDITY,
        c."Name" AS CITY_NAME,
        DATE_TRUNC('DAY', w.DATE_TIME) AS DAY
    FROM
        {{ source('openweather', 'WEATHER_FACT_TABLE') }} w
    JOIN
        {{ source('openweather', 'CITY_DIMENSION_TABLE') }} c
    ON
        w.CITY_ID = c.CITY_ID
    WHERE
        w.HUMIDITY IS NOT NULL 
        AND w.TEMP IS NOT NULL
)
SELECT 
    CITY_NAME,
    DAY,
    AVG(TEMP_FAHRENHEIT) AS AVG_TEMP_FAHRENHEIT,
    AVG(HUMIDITY) AS AVG_HUMIDITY
FROM HumidityTemp
GROUP BY 
    CITY_NAME,
    DAY
ORDER BY 
    CITY_NAME,
    DAY