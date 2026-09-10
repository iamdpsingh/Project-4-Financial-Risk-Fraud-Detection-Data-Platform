import os

import pandas as pd
import plotly.express as px
import queries
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery

# Load environment variables from .env
load_dotenv()

# Config
st.set_page_config(
    page_title="Executive Risk Dashboard",
    page_icon="🚨",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize BigQuery client
@st.cache_resource
def get_bq_client():
    return bigquery.Client()

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "financial-data-platform-508216")
DATASET = "financial_risk_analytics"  # The standard analytics dataset

# Custom CSS for styling
st.markdown("""
<style>
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .high-risk {
        color: #ff4b4b;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

st.title("🚨 Real-Time Financial Risk & Fraud Dashboard")
st.markdown("Live monitoring of transaction anomalies and fraud patterns across the platform.")

# Load Data
client = get_bq_client()

@st.cache_data(ttl=60) # Cache for 1 minute to avoid hammering BigQuery
def fetch_data(query_str):
    try:
        query_job = client.query(query_str)
        return query_job.to_dataframe()
    except Exception as e:
        st.error(f"Error fetching data: {e!s}")
        return pd.DataFrame()

# -- Main Dashboard Layout --
st.header("Today's Overview")

# 1. High Level Metrics
metrics_df = fetch_data(queries.get_high_level_metrics(PROJECT_ID, DATASET))

if not metrics_df.empty:
    col1, col2, col3 = st.columns(3)
    
    total_txns = metrics_df['total_transactions'].iloc[0]
    fraud_cnt = metrics_df['fraud_count'].iloc[0]
    fraud_amt = metrics_df['fraud_amount_usd'].iloc[0]
    
    col1.metric("Total Transactions", f"{total_txns:,}")
    col2.metric("High-Risk / Fraud Count", f"{fraud_cnt:,}")
    col3.metric("Fraud Value (USD)", f"${fraud_amt:,.2f}")
else:
    st.info("No transaction data available for today yet.")

st.markdown("---")

# 2. Charts
col1, col2 = st.columns(2)

with col1:
    st.subheader("Risk Distribution")
    risk_df = fetch_data(queries.get_risk_distribution(PROJECT_ID, DATASET))
    if not risk_df.empty:
        fig_pie = px.pie(
            risk_df, 
            names='risk_level', 
            values='count', 
            hole=0.4,
            color='risk_level',
            color_discrete_map={'LOW': '#00cc96', 'MEDIUM': '#ffa15a', 'HIGH': '#ef553b'}
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("No risk data available.")

with col2:
    st.subheader("Hourly Transaction Trend")
    trend_df = fetch_data(queries.get_hourly_trend(PROJECT_ID, DATASET))
    if not trend_df.empty:
        # Create a line chart with two lines: total volume and fraud volume
        fig_line = px.line(
            trend_df,
            x='hour',
            y=['volume', 'fraud_volume'],
            labels={'value': 'Transaction Count', 'hour': 'Hour of Day (UTC)', 'variable': 'Type'},
            color_discrete_map={'volume': '#636efa', 'fraud_volume': '#ef553b'}
        )
        st.plotly_chart(fig_line, use_container_width=True)
    else:
        st.info("No hourly trend data available.")

st.markdown("---")

# 3. Recent Anomalies Table
st.subheader("Recent High-Risk Transactions & Anomalies")
st.caption("Transactions flagged by the live Dataflow scoring engine.")

anomalies_df = fetch_data(queries.get_recent_anomalies(PROJECT_ID, DATASET, limit=20))

if not anomalies_df.empty:
    st.dataframe(
        anomalies_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "amount_usd": st.column_config.NumberColumn(
                "Amount (USD)",
                help="The transaction amount converted to USD",
                format="$ %.2f",
            ),
            "risk_score": st.column_config.NumberColumn(
                "Risk Score",
                help="0-100 score assigned by Dataflow",
            ),
            "signals_triggered": "Alert Signals",
            "transaction_timestamp": st.column_config.DatetimeColumn(
                "Time (UTC)",
                format="YYYY-MM-DD HH:mm:ss"
            )
        }
    )
else:
    st.info("No high-risk transactions detected recently.")

# Auto-refresh helper
st.sidebar.markdown("### Dashboard Controls")
if st.sidebar.button("🔄 Refresh Data Now"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.info("This dashboard reads live from BigQuery. The Dataflow streaming pipeline pushes scored events directly to the analytics dataset.")
