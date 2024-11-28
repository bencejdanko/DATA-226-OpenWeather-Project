{% snapshot weather_conditions_frequency_snapshot %}

{{ 
    config(
        target_schema='snapshot',          
        unique_key='CITY_ID',              
        strategy='timestamp',              
        updated_at='LAST_UPDATED',
        invalidate_hard_deletes='True'
    ) 
}}

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
    CITY_ID,
    CITY_NAME,
    WEATHER_MAIN,
    CONDITION_COUNT,
    CURRENT_TIMESTAMP AS LAST_UPDATED  -- Simulated timestamp column for change detection
FROM
    Weather_Conditions_Frequency
ORDER BY
    CONDITION_COUNT DESC, CITY_NAME

{% endsnapshot %}