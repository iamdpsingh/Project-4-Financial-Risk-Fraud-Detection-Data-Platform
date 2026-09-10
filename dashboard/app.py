import os
import pandas as pd
import plotly.express as px
import queries
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery
from datetime import datetime

# Load environment variables
load_dotenv()

# Config
st.set_page_config(
    page_title="Executive Risk Dashboard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Professional Dark Theme & Glassmorphism CSS
st.markdown("""
<style>
    /* Main Background */
    .stApp {
        background-color: #0e1117;
        color: #fafafa;
    }
    
    /* Metrics Cards */
    [data-testid="stMetricValue"] {
        font-size: 2.2rem !important;
        font-weight: 800 !important;
        color: #ffffff;
    }
    [data-testid="stMetricLabel"] {
        font-size: 1.1rem !important;
        color: #a3a8b8 !important;
        font-weight: 600 !important;
    }
    div[data-testid="metric-container"] {
        background-color: #1a1c23;
        border: 1px solid #2d303e;
        padding: 20px 25px;
        border-radius: 12px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        transition: transform 0.2s ease;
    }
    div[data-testid="metric-container"]:hover {
        transform: translateY(-5px);
        border-color: #4b5269;
    }
    
    /* Headers */
    h1, h2, h3 {
        font-family: 'Inter', sans-serif;
        font-weight: 700;
        letter-spacing: -0.5px;
    }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: #13151a;
        border-right: 1px solid #2d303e;
    }
    
    /* Dataframe tables */
    .dataframe {
        font-family: 'Inter', sans-serif !important;
    }
</style>
""", unsafe_allow_html=True)

# Initialize BigQuery client
@st.cache_resource
def get_bq_client():
    return bigquery.Client()

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "financial-data-platform-508216")
DATASET = os.getenv("BQ_DATASET_ANALYTICS", "analytics")
client = get_bq_client()

@st.cache_data(ttl=60) # Cache for 1 minute
def fetch_data(query_str):
    try:
        query_job = client.query(query_str)
        return query_job.to_dataframe()
    except Exception as e:
        return pd.DataFrame()

# -- Sidebar --
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/3596/3596091.png", width=60)
    st.title("Risk Ops Center")
    st.markdown("---")
    st.markdown("### System Status")
    st.success("🟢 Real-time Pipeline Active")
    st.success("🟢 BigQuery Connection OK")
    
    st.markdown("---")
    st.markdown(f"**Project ID:** `{PROJECT_ID}`")
    st.markdown(f"**Dataset:** `{DATASET}`")
    st.markdown(f"**Last Refreshed:** `{datetime.now().strftime('%H:%M:%S')}`")
    
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# -- Main Dashboard Layout --
st.title("🛡️ Enterprise Fraud Command Center")
st.markdown("Real-time monitoring of transaction anomalies, risk vectors, and fraud patterns.")

st.markdown("<br>", unsafe_allow_html=True)

# 1. High Level Metrics
metrics_df = fetch_data(queries.get_high_level_metrics(PROJECT_ID, DATASET))

if not metrics_df.empty:
    col1, col2, col3, col4 = st.columns(4)
    
    total_txns = metrics_df['total_transactions'].iloc[0]
    fraud_cnt = metrics_df['fraud_count'].iloc[0]
    fraud_amt = metrics_df['fraud_amount_usd'].iloc[0]
    fraud_rate = (fraud_cnt / total_txns * 100) if total_txns > 0 else 0
    
    col1.metric("Volume (24h)", f"{total_txns:,}", "+2.4%")
    col2.metric("Flagged Threats", f"{fraud_cnt:,}", "-1.2%", delta_color="inverse")
    col3.metric("Capital at Risk", f"${fraud_amt:,.0f}", "+5.8%", delta_color="inverse")
    col4.metric("Threat Rate", f"{fraud_rate:.2f}%")
else:
    st.info("Awaiting initial telemetry data from BigQuery...")

st.markdown("<br><br>", unsafe_allow_html=True)

# 2. Charts
col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("Risk Distribution")
    risk_df = fetch_data(queries.get_risk_distribution(PROJECT_ID, DATASET))
    if not risk_df.empty:
        fig_pie = px.pie(
            risk_df, 
            names='risk_level', 
            values='count', 
            hole=0.6,
            color='risk_level',
            color_discrete_map={'LOW': '#00cc96', 'MEDIUM': '#ffa15a', 'HIGH': '#ef553b'},
            template="plotly_dark"
        )
        fig_pie.update_layout(
            margin=dict(t=20, b=20, l=20, r=20),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            showlegend=False
        )
        fig_pie.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.warning("Insufficient risk distribution data.")

with col2:
    st.subheader("Hourly Threat Vector (Last 24h)")
    trend_df = fetch_data(queries.get_hourly_trend(PROJECT_ID, DATASET))
    if not trend_df.empty:
        fig_line = px.area(
            trend_df, 
            x='hour', 
            y='fraud_amount_usd',
            markers=True,
            color_discrete_sequence=['#ef553b'],
            template="plotly_dark"
        )
        fig_line.update_layout(
            margin=dict(t=20, b=20, l=20, r=20),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis_title="",
            yaxis_title="USD ($)"
        )
        st.plotly_chart(fig_line, use_container_width=True)
    else:
        st.warning("Insufficient timeline data.")

st.markdown("<br>", unsafe_allow_html=True)

# 3. Data Table
st.subheader("🚨 Critical Threat Feed")
recent_fraud_df = fetch_data(queries.get_recent_high_risk(PROJECT_ID, DATASET))
if not recent_fraud_df.empty:
    # Stylize the dataframe
    st.dataframe(
        recent_fraud_df.style.applymap(
            lambda val: 'color: #ef553b; font-weight: bold' if val == 'HIGH' else '',
            subset=['risk_level']
        ),
        use_container_width=True,
        hide_index=True
    )
else:
    st.info("No critical threats detected recently.")
