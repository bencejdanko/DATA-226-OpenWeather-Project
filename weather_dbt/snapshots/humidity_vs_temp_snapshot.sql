{% snapshot snapshot_humidity_temperature %}

{{
  config(
    target_schema='snapshot',
    unique_key="CITY_NAME || '-' || DAY",
    strategy='timestamp',
    updated_at='DAY',
    invalidate_hard_deletes=True
  )
}}

SELECT * FROM {{ ref('humidity_vs_temp') }}

{% endsnapshot %}
