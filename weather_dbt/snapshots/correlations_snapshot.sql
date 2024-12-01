{% snapshot snapshot_correlation_data %}

{{
  config(
    target_schema='snapshot',
    unique_key='CITY_NAME',
    strategy='check',
    check_cols='all',
    invalidate_hard_deletes=True
  )
}}

WITH CorrelationData AS (
    SELECT 
        c.city_name AS CITY_NAME,
        AVG(((w.TEMP - 273.15) * 9/5 + 32)) AS AVG_TEMP,
        AVG(w.HUMIDITY) AS AVG_HUMIDITY,
        AVG(w.PRESSURE) AS AVG_PRESSURE,
        AVG(w.WIND_SPEED) AS AVG_WIND_SPEED
    FROM
        {{ source('openweather', 'WEATHER_FACT_TABLE') }} w
    JOIN
        {{ source('openweather', 'CITY_DIMENSION_TABLE') }} c
    ON
        w.CITY_ID = c.CITY_ID
    GROUP BY 
        1
)
SELECT *
FROM CorrelationData

{% endsnapshot %}
