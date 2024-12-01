WITH TempTrends AS (
    SELECT 
        c."Name" AS CITY_NAME,
        DATE_TRUNC('DAY', w.DATE_TIME) AS DAY,
        AVG(((w.TEMP - 273.15) * 9/5 + 32)) AS AVG_TEMP_FAHRENHEIT
    FROM
        {{ source('openweather', 'WEATHER_FACT_TABLE') }} w
    JOIN
        {{ source('openweather', 'CITY_DIMENSION_TABLE') }} c
    ON
        w.CITY_ID = c.CITY_ID
    GROUP BY 
        CITY_NAME, DATE_TRUNC('DAY', w.DATE_TIME)
)
SELECT *
FROM TempTrends
ORDER BY CITY_NAME, DAY