WITH CloudCoverage AS (
    SELECT 
        w.CITY_NAME,
        DATE_TRUNC('DAY', w.DATE_TIME) AS DAY,
        AVG(w.CLOUD_COVERAGE) AS AVG_CLOUD_COVERAGE
    FROM
        {{ ref('openweather_join') }} w
    GROUP BY 
        1, 2
)
SELECT *
FROM CloudCoverage
ORDER BY DAY