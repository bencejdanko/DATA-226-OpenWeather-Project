{% snapshot snapshot_humidity_temp %}

{{
  config(
    target_schema='snapshot',
    unique_key='CITY_NAME',
    strategy='check',
    check_cols='all',
    invalidate_hard_deletes=True
  )
}}

WITH HumidityTemp AS (
    SELECT 
        ((w.TEMP - 273.15) * 9/5 + 32) AS TEMP_FAHRENHEIT,
        w.HUMIDITY,
        c."Name" AS CITY_NAME
    FROM
        {{ source('openweather', 'WEATHER_FACT_TABLE') }} w
    JOIN
        {{ source('openweather', 'CITY_DIMENSION_TABLE') }} c
    ON
        w.CITY_ID = c.CITY_ID
    WHERE
        w.HUMIDITY IS NOT NULL 
        AND w.TEMP IS NOT NULL
)
SELECT *
FROM HumidityTemp

{% endsnapshot %}