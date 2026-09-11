{{ config(materialized='view') }}

with source as (
    select * from {{ source('financial_risk_staging', 'customers') }}
),
renamed as (
    select
        customer_id,
        name as full_name,
        email,
        phone,
        city,
        country,
        date_of_birth,
        customer_segment,
        created_at as registration_date
    from source
)
select * from renamed
