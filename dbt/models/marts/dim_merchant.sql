{{ config(materialized='table') }}

with merchants as (
    select * from {{ ref('stg_merchants') }}
)

select
    merchant_id,
    merchant_name,
    merchant_category,
    mcc_code,
    merchant_country,
    merchant_city,
    merchant_risk_tier
from merchants
