

  create or replace view `financial-data-platform-508216`.`analytics`.`stg_merchants`
  OPTIONS()
  as 

with source as (
    select * from `financial-data-platform-508216`.`staging`.`merchants`
),
renamed as (
    select
        merchant_id,
        merchant_name,
        merchant_category,
        mcc_code,
        country as merchant_country,
        city as merchant_city,
        risk_category as merchant_risk_tier
    from source
)
select * from renamed;

