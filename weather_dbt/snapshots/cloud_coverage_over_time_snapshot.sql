{% snapshot snapshot_cloud_coverage %}

{{
  config(
    target_schema='snapshot',
    unique_key='DAY',
    strategy='timestamp',
    updated_at='DAY',
    invalidate_hard_deletes=True
  )
}}

WITH CloudCoverage AS (
    SELECT 
        c."Name" AS CITY_NAME,
        DATE_TRUNC('DAY', w.DATE_TIME) AS DAY,
        AVG(w.CLOUD_COVERAGE) AS AVG_CLOUD_COVERAGE
    FROM
        {{ source('openweather', 'WEATHER_FACT_TABLE') }} w
    JOIN
        {{ source('openweather', 'CITY_DIMENSION_TABLE') }} c
    ON
        w.CITY_ID = c.CITY_ID
    GROUP BY 
        1, DATE_TRUNC('DAY', w.DATE_TIME)
)
SELECT *
FROM CloudCoverage
ORDER BY DAY

{% endsnapshot %}