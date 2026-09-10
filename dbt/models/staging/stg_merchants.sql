{{ config(materialized='view') }}

with source as (
    select * from {{ source('financial_risk_staging', 'merchants') }}
),
renamed as (
    select
        merchant_id,
        name as merchant_name,
        category as merchant_category,
        country as merchant_country,
        risk_tier as merchant_risk_tier
    from source
)
select * from renamed
