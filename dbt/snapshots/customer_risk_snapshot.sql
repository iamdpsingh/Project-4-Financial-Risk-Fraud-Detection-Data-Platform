{% snapshot customer_risk_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='check',
      check_cols=['streaming_risk_level', 'streaming_risk_score']
    )
}}

with latest_risk as (
    select
        customer_id,
        max(streaming_risk_level) as streaming_risk_level,
        max(streaming_risk_score) as streaming_risk_score
    from {{ ref('stg_transactions') }}
    group by customer_id
)

select * from latest_risk

{% endsnapshot %}
