{% snapshot snapshot_temp_trends %}

{{
  config(
    target_schema='snapshot',
    unique_key="CITY_NAME || '-' || DAY",
    strategy='timestamp',
    updated_at='DAY',
    invalidate_hard_deletes=True
  )
}}

SELECT * FROM {{ ref('temp_trends_by_city') }}

{% endsnapshot %}
