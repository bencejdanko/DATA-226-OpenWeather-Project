{% snapshot daily_average_weather_snapshot %}

{{ 
    config(
        target_schema='snapshot',          
        unique_key='CITY_ID',              
        strategy='timestamp',              
        updated_at='LAST_UPDATED',
        invalidate_hard_deletes='True'
    ) 
}}

WITH Daily_Average_Weather_Trend AS (
    SELECT
        w.CITY_ID,
        c."Name" AS CITY_NAME,
        DATE_TRUNC('day', w.DATE_TIME) AS WEATHER_DATE,
        AVG(((w.TEMP - 273.15) * 9/5 + 32)) AS AVG_TEMP_FAHRENHEIT,
        AVG(w.HUMIDITY) AS AVG_HUMIDITY
    FROM
        {{ source('openweather', 'WEATHER_FACT_TABLE') }} w
    JOIN
        {{ source('openweather', 'CITY_DIMENSION_TABLE') }} c
    ON
        w.CITY_ID = c.CITY_ID
    GROUP BY
        w.CITY_ID, c."Name", DATE_TRUNC('day', w.DATE_TIME)
)
SELECT
    CITY_ID,
    CITY_NAME,
    WEATHER_DATE,
    AVG_TEMP_FAHRENHEIT,
    AVG_HUMIDITY,
    CURRENT_TIMESTAMP AS LAST_UPDATED  -- Simulated timestamp column for change detection
FROM
    Daily_Average_Weather_Trend
ORDER BY
    WEATHER_DATE, CITY_NAME

{% endsnapshot %}