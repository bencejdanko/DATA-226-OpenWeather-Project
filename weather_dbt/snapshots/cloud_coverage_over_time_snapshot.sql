{% snapshot snapshot_cloud_coverage %}

{{
  config(
    target_schema='snapshot',
    unique_key="CITY_NAME || '-' || DAY",
    strategy='timestamp',
    updated_at='DAY',
    invalidate_hard_deletes=True
  )
}}

SELECT * FROM {{ ref('cloud_coverage_over_time') }}

{% endsnapshot %}
