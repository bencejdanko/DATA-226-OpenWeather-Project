{% snapshot snapshot_temp_trends %}

{{
  config(
    target_schema='snapshot',
    unique_key='CITY_NAME',
    strategy='timestamp',
    updated_at='DAY',
    invalidate_hard_deletes=True
  )
}}

WITH TempTrends AS (
    SELECT 
        "Name" AS CITY_NAME,
        DATE_TRUNC('DAY', w.DATE_TIME) AS DAY,
        AVG(((w.TEMP - 273.15) * 9/5 + 32)) AS AVG_TEMP_FAHRENHEIT
    FROM
        {{ source('openweather', 'WEATHER_FACT_TABLE') }} w
    JOIN
        {{ source('openweather', 'CITY_DIMENSION_TABLE') }} c
    ON
        w.CITY_ID = c.CITY_ID
    GROUP BY 
        CITY_NAME, DATE_TRUNC('DAY', w.DATE_TIME)
)
SELECT *
FROM TempTrends
ORDER BY CITY_NAME, DAY

{% endsnapshot %}