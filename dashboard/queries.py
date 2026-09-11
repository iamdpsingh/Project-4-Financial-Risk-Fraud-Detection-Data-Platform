"""
SQL queries for fetching live dashboard metrics from BigQuery.
"""

def get_high_level_metrics(project_id: str, dataset: str) -> str:
    """Gets total transactions, fraud amount, and fraud count for today."""
    return f"""
    SELECT
        COUNT(*) as total_transactions,
        SUM(CASE WHEN risk_level = 'HIGH' THEN 1 ELSE 0 END) as fraud_count,
        SUM(CASE WHEN risk_level = 'HIGH' THEN amount_usd ELSE 0 END) as fraud_amount_usd
    FROM `{project_id}.{dataset}.transaction_risk`
    WHERE DATE(transaction_timestamp) = CURRENT_DATE()
    """

def get_recent_anomalies(project_id: str, dataset: str, limit: int = 50) -> str:
    """Gets the most recent high-risk transactions."""
    return f"""
    SELECT
        transaction_id,
        customer_id,
        merchant_id,
        amount_usd,
        transaction_type,
        country,
        transaction_timestamp,
        risk_score,
        signals_triggered
    FROM `{project_id}.{dataset}.transaction_risk`
    WHERE risk_level = 'HIGH'
    ORDER BY transaction_timestamp DESC
    LIMIT {limit}
    """

def get_risk_distribution(project_id: str, dataset: str) -> str:
    """Gets the distribution of risk levels for today."""
    return f"""
    SELECT
        COALESCE(risk_level, 'LOW') as risk_level,
        COUNT(*) as count
    FROM `{project_id}.{dataset}.transaction_risk`
    WHERE DATE(transaction_timestamp) = CURRENT_DATE()
    GROUP BY 1
    """

def get_hourly_trend(project_id: str, dataset: str) -> str:
    """Gets transaction volume by hour for today."""
    return f"""
    SELECT
        EXTRACT(HOUR FROM transaction_timestamp) as hour,
        COUNT(*) as volume,
        SUM(CASE WHEN risk_level = 'HIGH' THEN 1 ELSE 0 END) as fraud_volume
    FROM `{project_id}.{dataset}.transaction_risk`
    WHERE DATE(transaction_timestamp) = CURRENT_DATE()
    GROUP BY 1
    ORDER BY 1
    """
