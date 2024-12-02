WITH WindSpeedData AS (
    SELECT 
        w.WIND_SPEED,
        w.CITY_NAME,
        w.DATE_TIME,
        DATE_TRUNC('DAY', w.DATE_TIME) AS DAY
    FROM
        {{ ref('openweather_join') }} w
    WHERE
        w.WIND_SPEED IS NOT NULL
)
SELECT 
    CITY_NAME,
    DAY,
    AVG(WIND_SPEED) AS AVG_WIND_SPEED
FROM WindSpeedData
GROUP BY 
    CITY_NAME,
    DAY
ORDER BY 
    CITY_NAME, DAY