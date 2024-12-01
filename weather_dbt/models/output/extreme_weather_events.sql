-- transforms raw data to display daily weather events
WITH extreme_weather_events AS (
    SELECT
        w.CITY_ID,
        c.city_name AS CITY_NAME,
        DATE_TRUNC('DAY', w.DATE_TIME) AS DAY,
        ((w.TEMP - 273.15) * 9/5 + 32) AS TEMP_FAHRENHEIT,
        w.WIND_SPEED,
        CASE
            WHEN ((w.TEMP - 273.15) * 9/5 + 32) > 85 THEN 'Hot Day'
            WHEN ((w.TEMP - 273.15) * 9/5 + 32) < 59 THEN 'Cold Day'
            WHEN w.WIND_SPEED > 4 THEN 'Breezy Day'
            ELSE 'Normal'
        END AS HOURLY_WEATHER_ALERT
    FROM
        {{ source('openweather', 'WEATHER_FACT_TABLE') }} w
    JOIN
        {{ source('openweather', 'CITY_DIMENSION_TABLE') }} c
    ON
        w.CITY_ID = c.CITY_ID
),
Daily_Weather AS (
    SELECT
        CITY_NAME,
        DAY,
        AVG(TEMP_FAHRENHEIT) AS AVG_TEMP_FAHRENHEIT,
        AVG(WIND_SPEED) AS AVG_WIND_SPEED,
        CASE
            WHEN AVG(TEMP_FAHRENHEIT) > 85 THEN 'Hot Day'
            WHEN AVG(TEMP_FAHRENHEIT) < 59 THEN 'Cold Day'
            WHEN AVG(WIND_SPEED) > 4 THEN 'Breezy Day'
            ELSE 'Normal'
        END AS WEATHER_ALERT
    FROM
        extreme_weather_events
    GROUP BY
        CITY_NAME,
        DAY
)
SELECT
    CITY_NAME,
    DAY,
    AVG_TEMP_FAHRENHEIT,
    AVG_WIND_SPEED,
    WEATHER_ALERT
FROM 
    Daily_Weather
ORDER BY 
    CITY_NAME,
    DAY
