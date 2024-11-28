WITH Hourly_Weather_Patterns AS (
    SELECT
        w.CITY_ID,
        c."Name" AS CITY_NAME, -- Include city name for better insights
        EXTRACT(HOUR FROM w.DATE_TIME) AS HOUR_OF_DAY,
        AVG(((w.TEMP - 273.15) * 9/5 + 32)) AS AVG_TEMP_FAHRENHEIT,
        AVG(w.WIND_SPEED) AS AVG_WIND_SPEED,
        AVG(w.HUMIDITY) AS AVG_HUMIDITY
    FROM
        {{ source('openweather', 'WEATHER_FACT_TABLE') }} w
    JOIN
        {{ source('openweather', 'CITY_DIMENSION_TABLE') }} c
    ON
        w.CITY_ID = c.CITY_ID
    GROUP BY
        w.CITY_ID, c."Name", EXTRACT(HOUR FROM w.DATE_TIME)
)
SELECT
    *
FROM
    Hourly_Weather_Patterns
ORDER BY
    CITY_NAME, HOUR_OF_DAY