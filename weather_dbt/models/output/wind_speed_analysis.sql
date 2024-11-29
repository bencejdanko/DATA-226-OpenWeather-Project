WITH WindSpeedData AS (
    SELECT 
        w.WIND_SPEED,
        c."Name" AS CITY_NAME
    FROM
        {{ source('openweather', 'WEATHER_FACT_TABLE') }} w
    JOIN
        {{ source('openweather', 'CITY_DIMENSION_TABLE') }} c
    ON
        w.CITY_ID = c.CITY_ID
    WHERE
        w.WIND_SPEED IS NOT NULL 
)
SELECT *
FROM WindSpeedData