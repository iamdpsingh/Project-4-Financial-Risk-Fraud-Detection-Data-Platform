{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    partition_by={
      "field": "transaction_timestamp",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

with transactions as (
    select * from {{ ref('stg_transactions') }}
)

select
    transaction_id,
    customer_id,
    account_id,
    merchant_id,
    device_id,
    amount,
    amount_usd,
    currency,
    transaction_timestamp,
    transaction_type,
    transaction_location,
    streaming_risk_score,
    streaming_risk_level,
    signals_triggered
from transactions

{% if is_incremental() %}
  where transaction_timestamp > (select max(transaction_timestamp) from {{ this }})
{% endif %}
