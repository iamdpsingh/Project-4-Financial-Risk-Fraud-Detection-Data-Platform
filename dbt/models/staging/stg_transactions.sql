{{ config(materialized='view') }}

with source as (
    select * from {{ source('financial_risk_analytics', 'transaction_risk') }}
),
renamed as (
    select
        transaction_id,
        customer_id,
        account_id,
        merchant_id,
        device_id,
        amount,
        timestamp as transaction_timestamp,
        type as transaction_type,
        location as transaction_location,
        is_fraud as actual_is_fraud,
        risk_score as streaming_risk_score,
        risk_level as streaming_risk_level
    from source
)
select * from renamed
