

  create or replace view `financial-data-platform-508216`.`analytics`.`stg_transactions`
  OPTIONS()
  as 

with source as (
    select * from `financial-data-platform-508216`.`analytics`.`transaction_risk`
),
renamed as (
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
        country as transaction_location,
        risk_score as streaming_risk_score,
        risk_level as streaming_risk_level,
        signals_triggered
    from source
)
select * from renamed;

