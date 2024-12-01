WITH CloudCoverage AS (
    SELECT 
        c.city_name AS CITY_NAME,
        DATE_TRUNC('DAY', w.DATE_TIME) AS DAY,
        AVG(w.CLOUD_COVERAGE) AS AVG_CLOUD_COVERAGE
    FROM
        {{ source('openweather', 'WEATHER_FACT_TABLE') }} w
    JOIN
        {{ source('openweather', 'CITY_DIMENSION_TABLE') }} c
    ON
        w.CITY_ID = c.CITY_ID
    GROUP BY 
        1, DATE_TRUNC('DAY', w.DATE_TIME)
)
SELECT *
FROM CloudCoverage
ORDER BY DAY
