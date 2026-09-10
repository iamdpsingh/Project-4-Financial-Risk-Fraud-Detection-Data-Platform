-- Fact table combining streaming risk scores and batch transactions

WITH batch_txns AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

streaming_risk AS (
    SELECT
        transaction_id,
        CAST(risk_score AS INT64) AS risk_score,
        risk_level,
        signals_triggered
    FROM `{{ var('project_id', 'YOUR_GCP_PROJECT_ID') }}.analytics.transaction_risk`
)

SELECT
    b.transaction_id,
    b.transaction_timestamp,
    b.customer_id,
    b.account_id,
    b.merchant_id,
    b.device_id,
    b.amount_usd,
    b.transaction_type,
    b.status,
    COALESCE(s.risk_score, 0) AS risk_score,
    COALESCE(s.risk_level, 'UNKNOWN') AS risk_level,
    s.signals_triggered
FROM batch_txns b
LEFT JOIN streaming_risk s
  ON b.transaction_id = s.transaction_id
