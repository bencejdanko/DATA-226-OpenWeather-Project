WITH Extreme_Weather_Events AS (
    SELECT
        w.CITY_ID,
        c."Name" AS CITY_NAME,
        w.DATE_TIME,
        ((w.TEMP - 273.15) * 9/5 + 32) AS TEMP_FAHRENHEIT,
        w.WIND_SPEED,
        CASE
            WHEN ((w.TEMP - 273.15) * 9/5 + 32) > 100 THEN 'Extreme Heat'
            WHEN ((w.TEMP - 273.15) * 9/5 + 32) < 32 THEN 'Extreme Cold'
            WHEN w.WIND_SPEED > 20 THEN 'High Winds'
            ELSE 'Normal'
        END AS WEATHER_ALERT
    FROM
        {{ source('openweather', 'WEATHER_FACT_TABLE') }} w
    JOIN
        {{ source('openweather', 'CITY_DIMENSION_TABLE') }} c
    ON
        w.CITY_ID = c.CITY_ID
    WHERE
        ((w.TEMP - 273.15) * 9/5 + 32) > 100
        OR ((w.TEMP - 273.15) * 9/5 + 32) < 32
        OR w.WIND_SPEED > 20
)
SELECT
    *
FROM
    Extreme_Weather_Events
ORDER BY
    DATE_TIME, CITY_NAME