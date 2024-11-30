WITH WeatherDistribution AS (
    SELECT 
        w.WEATHER_MAIN,
        COUNT(*) AS COUNT,
        c."Name" AS CITY_NAME
    FROM
        {{ source('openweather', 'WEATHER_FACT_TABLE') }} w
    JOIN
        {{ source('openweather', 'CITY_DIMENSION_TABLE') }} c
    ON
        w.CITY_ID = c.CITY_ID
    GROUP BY 
        w.WEATHER_MAIN, 3
)
SELECT *
FROM WeatherDistribution
ORDER BY COUNT DESC