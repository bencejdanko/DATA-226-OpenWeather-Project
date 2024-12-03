{% snapshot snapshot_extreme_weather_events %}

{{
  config(
    target_schema='snapshot',
    unique_key="CITY_NAME || '-' || DAY",
    strategy='timestamp',
    updated_at='DAY',
    invalidate_hard_deletes=True
  )
}}

SELECT * FROM {{ ref('extreme_weather_events') }}

{% endsnapshot %}
