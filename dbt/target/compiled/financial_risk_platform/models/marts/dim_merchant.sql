

with merchants as (
    select * from `financial-data-platform-508216`.`analytics`.`stg_merchants`
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