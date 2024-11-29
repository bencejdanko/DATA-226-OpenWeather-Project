{% snapshot snapshot_wind_speed_data %}

{{
  config(
    target_schema='snapshot',
    unique_key='CITY_NAME',
    strategy='check',
    check_cols='all',
    invalidate_hard_deletes=True
  )
}}

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

{% endsnapshot %}