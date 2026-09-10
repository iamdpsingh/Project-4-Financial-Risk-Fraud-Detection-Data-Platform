{{ config(materialized='view') }}

with source as (
    select * from {{ source('financial_risk_staging', 'customers') }}
),
renamed as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        address,
        city,
        country,
        date_of_birth,
        registration_date,
        risk_score as initial_risk_score,
        is_active
    from source
)
select * from renamed
