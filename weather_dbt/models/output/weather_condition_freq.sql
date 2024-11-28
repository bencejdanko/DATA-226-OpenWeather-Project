WITH Weather_Conditions_Frequency AS (
    SELECT
        w.CITY_ID,
        c."Name" AS CITY_NAME,
        w.WEATHER_MAIN,
        COUNT(*) AS CONDITION_COUNT
    FROM
        {{ source('openweather', 'WEATHER_FACT_TABLE') }} w
    JOIN
        {{ source('openweather', 'CITY_DIMENSION_TABLE') }} c
    ON
        w.CITY_ID = c.CITY_ID
    GROUP BY
        w.CITY_ID, c."Name", w.WEATHER_MAIN
)
SELECT
    *
FROM
    Weather_Conditions_Frequency
ORDER BY
    CONDITION_COUNT DESC, CITY_NAME