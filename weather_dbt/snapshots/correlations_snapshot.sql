{% snapshot snapshot_correlation_data %}

{{
  config(
    target_schema='snapshot',
    unique_key="CITY_NAME || '-' || DAY",
    strategy='timestamp',
    updated_at='DAY',     
    invalidate_hard_deletes=True
  )
}}

SELECT * FROM {{ ref('correlation') }}

{% endsnapshot %}
