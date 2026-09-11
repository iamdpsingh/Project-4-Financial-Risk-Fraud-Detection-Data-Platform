



with latest_risk as (
    select
        customer_id,
        max(streaming_risk_level) as streaming_risk_level,
        max(streaming_risk_score) as streaming_risk_score
    from `financial-data-platform-508216`.`analytics`.`stg_transactions`
    group by customer_id
)

select * from latest_risk
