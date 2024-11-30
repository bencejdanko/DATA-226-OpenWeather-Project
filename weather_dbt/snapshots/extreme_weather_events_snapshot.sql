{% snapshot snapshot_extreme_weather_events %}

{{
  config(
    target_schema='snapshot',
    unique_key='DAY',
    strategy='timestamp',
    updated_at='DAY',
    invalidate_hard_deletes=True
  )
}}

WITH Extreme_Weather_Events AS (
    SELECT
        c."Name" AS CITY_NAME,
        DATE_TRUNC('DAY', w.DATE_TIME) AS DAY,
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
    CITY_NAME,
    DAY,
    AVG(TEMP_FAHRENHEIT) AS AVG_TEMP_FAHRENHEIT,
    AVG(WIND_SPEED) AS AVG_WIND_SPEED,
    CASE
        WHEN AVG(TEMP_FAHRENHEIT) > 100 THEN 'Extreme Heat'
        WHEN AVG(TEMP_FAHRENHEIT) < 32 THEN 'Extreme Cold'
        WHEN AVG(WIND_SPEED) > 20 THEN 'High Winds'
        ELSE 'Normal'
    END AS WEATHER_ALERT
FROM 
    Extreme_Weather_Events
GROUP BY 
    CITY_NAME,
    DAY
ORDER BY 
    DAY

{% endsnapshot %}
