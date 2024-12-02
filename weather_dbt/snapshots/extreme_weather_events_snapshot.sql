{% snapshot snapshot_extreme_weather_events %}

{{
  config(
    target_schema='snapshot',
    unique_key='day',
    strategy='timestamp',
    updated_at='DAY',
    invalidate_hard_deletes=True
  )
}}

WITH Extreme_Weather_Events AS (
    SELECT
        w.CITY_NAME,
        DATE_TRUNC('DAY', w.DATE_TIME) AS DAY,
        w.TEMP_FAHRENHEIT,
        w.WIND_SPEED,
        CASE
            WHEN w.TEMP_FAHRENHEIT > 85 THEN 'Hot Day'
            WHEN w.TEMP_FAHRENHEIT < 59 THEN 'Cold Day'
            WHEN w.WIND_SPEED > 4 THEN 'Breezy Day'
            ELSE 'Normal'
        END AS HOURLY_WEATHER_ALERT
    FROM
        {{ ref('openweather_join') }} w
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
        Extreme_Weather_Events
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
    DAY

{% endsnapshot %}