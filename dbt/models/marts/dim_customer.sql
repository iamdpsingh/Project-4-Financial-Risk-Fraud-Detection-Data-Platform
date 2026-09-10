{{ config(materialized='table') }}

with customers as (
    select * from {{ ref('stg_customers') }}
)

select
    customer_id,
    first_name,
    last_name,
    email,
    city,
    country,
    initial_risk_score,
    is_active,
    -- Add computed age
    DATE_DIFF(CURRENT_DATE(), CAST(date_of_birth AS DATE), YEAR) as age
from customers
