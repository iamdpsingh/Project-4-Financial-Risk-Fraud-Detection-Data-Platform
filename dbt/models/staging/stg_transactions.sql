-- Staging model for raw transactions

WITH raw_transactions AS (
    SELECT *
    FROM `{{ var('project_id', 'YOUR_GCP_PROJECT_ID') }}.staging.transactions`
)

SELECT
    transaction_id,
    customer_id,
    account_id,
    merchant_id,
    device_id,
    CAST(transaction_timestamp AS TIMESTAMP) AS transaction_timestamp,
    CAST(amount AS FLOAT64) AS amount_local,
    currency,
    CAST(amount_usd AS FLOAT64) AS amount_usd,
    transaction_type,
    country,
    payment_method,
    status,
    is_international
FROM raw_transactions
