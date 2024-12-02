WITH TempTrends AS (
    SELECT 
        w.CITY_NAME,
        DATE_TRUNC('DAY', w.DATE_TIME) AS DAY,
        AVG(w.TEMP_FAHRENHEIT) AS AVG_TEMP_FAHRENHEIT
    FROM
        {{ ref('openweather_join') }} w
    GROUP BY 
        w.CITY_NAME, DATE_TRUNC('DAY', w.DATE_TIME)
)
SELECT *
FROM TempTrends
ORDER BY CITY_NAME, DAY