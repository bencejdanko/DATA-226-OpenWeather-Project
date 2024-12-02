WITH HumidityTemp AS (
    SELECT 
        w.TEMP_FAHRENHEIT,
        w.HUMIDITY,
        w.CITY_NAME,
        DATE_TRUNC('DAY', w.DATE_TIME) AS DAY
    FROM
        {{ ref('openweather_join') }} w
    WHERE
        w.HUMIDITY IS NOT NULL 
        AND w.TEMP_FAHRENHEIT IS NOT NULL
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