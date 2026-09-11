-- back compat for old kwarg name
  
  
        
            
            
            
            
        
    

    

    merge into `financial-data-platform-508216`.`analytics`.`fct_transactions` as DBT_INTERNAL_DEST
        using (

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

        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.transaction_id = DBT_INTERNAL_DEST.transaction_id))

    
    when matched then update set
        `transaction_id` = DBT_INTERNAL_SOURCE.`transaction_id`,`customer_id` = DBT_INTERNAL_SOURCE.`customer_id`,`account_id` = DBT_INTERNAL_SOURCE.`account_id`,`merchant_id` = DBT_INTERNAL_SOURCE.`merchant_id`,`device_id` = DBT_INTERNAL_SOURCE.`device_id`,`amount` = DBT_INTERNAL_SOURCE.`amount`,`amount_usd` = DBT_INTERNAL_SOURCE.`amount_usd`,`currency` = DBT_INTERNAL_SOURCE.`currency`,`transaction_timestamp` = DBT_INTERNAL_SOURCE.`transaction_timestamp`,`transaction_type` = DBT_INTERNAL_SOURCE.`transaction_type`,`transaction_location` = DBT_INTERNAL_SOURCE.`transaction_location`,`streaming_risk_score` = DBT_INTERNAL_SOURCE.`streaming_risk_score`,`streaming_risk_level` = DBT_INTERNAL_SOURCE.`streaming_risk_level`,`signals_triggered` = DBT_INTERNAL_SOURCE.`signals_triggered`
    

    when not matched then insert
        (`transaction_id`, `customer_id`, `account_id`, `merchant_id`, `device_id`, `amount`, `amount_usd`, `currency`, `transaction_timestamp`, `transaction_type`, `transaction_location`, `streaming_risk_score`, `streaming_risk_level`, `signals_triggered`)
    values
        (`transaction_id`, `customer_id`, `account_id`, `merchant_id`, `device_id`, `amount`, `amount_usd`, `currency`, `transaction_timestamp`, `transaction_type`, `transaction_location`, `streaming_risk_score`, `streaming_risk_level`, `signals_triggered`)


    