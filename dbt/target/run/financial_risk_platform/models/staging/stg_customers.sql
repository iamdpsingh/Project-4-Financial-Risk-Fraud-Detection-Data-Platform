

  create or replace view `financial-data-platform-508216`.`analytics`.`stg_customers`
  OPTIONS()
  as 

with source as (
    select * from `financial-data-platform-508216`.`staging`.`customers`
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
select * from renamed;

