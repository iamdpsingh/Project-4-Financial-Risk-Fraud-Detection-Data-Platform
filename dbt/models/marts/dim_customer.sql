{{ config(materialized='table') }}

with customers as (
    select * from {{ ref('stg_customers') }}
)

select
    customer_id,
    full_name,
    email,
    city,
    country,
    customer_segment,
    -- Add computed age
    DATE_DIFF(CURRENT_DATE(), CAST(date_of_birth AS DATE), YEAR) as age
from customers
