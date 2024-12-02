{% snapshot snapshot_cloud_coverage %}

{{
  config(
    target_schema='snapshot',
    unique_key='day',
    strategy='timestamp',
    updated_at='DAY',
    invalidate_hard_deletes=True
  )
}}

WITH CloudCoverage AS (
    SELECT 
        w.CITY_NAME,
        DATE_TRUNC('DAY', w.DATE_TIME) AS DAY,
        AVG(w.CLOUD_COVERAGE) AS AVG_CLOUD_COVERAGE
    FROM
        {{ ref('openweather_join') }} w
    GROUP BY 
        w.CITY_NAME, DATE_TRUNC('DAY', w.DATE_TIME)
)
SELECT *
FROM CloudCoverage
ORDER BY DAY

{% endsnapshot %}