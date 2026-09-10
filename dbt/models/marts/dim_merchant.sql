{{ config(materialized='table') }}

with merchants as (
    select * from {{ ref('stg_merchants') }}
)

select
    merchant_id,
    merchant_name,
    merchant_category,
    merchant_country,
    merchant_risk_tier
from merchants
