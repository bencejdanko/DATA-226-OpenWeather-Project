{% snapshot snapshot_correlation_data %}

{{
  config(
    target_schema='snapshot',
    unique_key='CITY_NAME',  
    strategy='timestamp',
    updated_at='UPDATED_AT',     
    invalidate_hard_deletes=True
  )
}}

WITH CorrelationData AS (
    SELECT 
        w.CITY_NAME,
        DATE_TRUNC('DAY', w.DATE_TIME) AS DAY,
        AVG(w.TEMP_FAHRENHEIT) AS AVG_TEMP,
        AVG(w.HUMIDITY) AS AVG_HUMIDITY,
        AVG(w.PRESSURE) AS AVG_PRESSURE,
        AVG(w.WIND_SPEED) AS AVG_WIND_SPEED,
        DATE_TRUNC('DAY', w.DATE_TIME) AS UPDATED_AT
    FROM
        {{ ref('openweather_join') }} w
    GROUP BY 
        w.CITY_NAME, DATE_TRUNC('DAY', w.DATE_TIME)
)
SELECT *
FROM CorrelationData

{% endsnapshot %}