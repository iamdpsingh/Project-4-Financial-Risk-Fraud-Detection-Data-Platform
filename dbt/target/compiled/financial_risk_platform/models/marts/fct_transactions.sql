

with transactions as (
    select * from `financial-data-platform-508216`.`analytics`.`stg_transactions`
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


  where transaction_timestamp > (select max(transaction_timestamp) from `financial-data-platform-508216`.`analytics`.`fct_transactions`)
