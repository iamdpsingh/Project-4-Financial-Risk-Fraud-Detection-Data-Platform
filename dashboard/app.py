import concurrent.futures
import os
from datetime import datetime

import pandas as pd
import plotly.express as px
import queries
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery

# Load environment variables
load_dotenv()

# Config
st.set_page_config(
    page_title="Risk Command Center",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Premium UI CSS Injection
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&display=swap');

    /* Global Typography */
    html, body, [class*="css"] {
        font-family: 'Outfit', sans-serif !important;
    }

    /* Deep Space Background with subtle gradient */
    .stApp {
        background: radial-gradient(circle at 50% 0%, #151822 0%, #090a0f 100%);
        color: #f0f2f6;
    }
    
    /* Header Styling */
    h1 {
        font-size: 3.5rem !important;
        font-weight: 800 !important;
        background: linear-gradient(135deg, #ffffff 0%, #8a93a8 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        letter-spacing: -1px;
        margin-bottom: 0.5rem !important;
    }
    h3 {
        font-weight: 600 !important;
        color: #e2e8f0 !important;
    }

    /* Premium Glassmorphism Metric Cards */
    div[data-testid="metric-container"] {
        background: rgba(20, 24, 35, 0.4);
        backdrop-filter: blur(16px);
        border: 1px solid rgba(255, 255, 255, 0.05);
        border-radius: 16px;
        padding: 24px;
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3);
        border-top: 1px solid rgba(255, 255, 255, 0.1);
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    }
    div[data-testid="metric-container"]:hover {
        transform: translateY(-5px);
        border-color: rgba(0, 242, 254, 0.3);
        box-shadow: 0 12px 40px 0 rgba(0, 242, 254, 0.15);
    }

    /* Metric Values - Neon Glow */
    [data-testid="stMetricValue"] {
        font-size: 2.8rem !important;
        font-weight: 800 !important;
        background: linear-gradient(90deg, #00f2fe 0%, #4facfe 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    [data-testid="stMetricLabel"] {
        font-size: 1.1rem !important;
        color: #8b95a5 !important;
        font-weight: 500 !important;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background-color: rgba(13, 15, 22, 0.95);
        border-right: 1px solid rgba(255, 255, 255, 0.05);
    }

    /* Dataframe tables */
    .dataframe {
        font-family: 'Outfit', sans-serif !important;
    }
    
    /* Hide the default Streamlit top-right spinner and gray-out */
    [data-testid="stStatusWidget"] {
        display: none !important;
    }
    
    /* Force opacity on all elements to prevent default streamlit stale dimming */
    div[data-testid="stVerticalBlock"] {
        opacity: 1 !important;
        filter: none !important;
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

@st.cache_data(ttl=2, show_spinner=False) # TTL=2 ensures the 10s fragment always gets fresh data
def fetch_all_data(project_id, dataset):
    """Fetches all 4 queries concurrently for lightning-fast UI loads."""
    q_metrics = queries.get_high_level_metrics(project_id, dataset)
    q_risk = queries.get_risk_distribution(project_id, dataset)
    q_trend = queries.get_hourly_trend(project_id, dataset)
    q_anomalies = queries.get_recent_anomalies(project_id, dataset)
    
    def run_query(q_str):
        try:
            return client.query(q_str).to_dataframe()
        except Exception:
            return pd.DataFrame()
            
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        f_metrics = executor.submit(run_query, q_metrics)
        f_risk = executor.submit(run_query, q_risk)
        f_trend = executor.submit(run_query, q_trend)
        f_anomalies = executor.submit(run_query, q_anomalies)
        
        return f_metrics.result(), f_risk.result(), f_trend.result(), f_anomalies.result()

# -- Sidebar --
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/3596/3596091.png", width=70)
    st.markdown("## Risk Ops Center")
    st.markdown("---")
    st.markdown("### 🌐 System Status")
    st.success("🟢 Real-time Pipeline Active")
    st.success("🟢 BigQuery Connection OK")
    
    st.markdown("---")
    st.markdown(f"**Project ID:** `{PROJECT_ID}`")
    st.markdown(f"**Dataset:** `{DATASET}`")
    st.markdown(f"**Last Refreshed:** `{datetime.now().strftime('%H:%M:%S')}`")
    
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("🔄 Sync Live Data", use_container_width=True, type="primary"):
        st.cache_data.clear()
        st.rerun()

# -- Main Dashboard Layout --
st.title("🛡️ Enterprise Risk Command Center")
st.markdown("<p style='font-size: 1.2rem; color: #a0aec0; margin-top: -15px;'>Real-time AI-driven monitoring of transaction anomalies and global threat vectors.</p>", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

@st.fragment(run_every="10s")
def live_dashboard():
    # Show fetching indicator in top right corner
    status_placeholder = st.empty()
    status_placeholder.markdown("""
        <div class="live-fetch-toast">🔄 Fetching live data...</div>
        <style>
            .live-fetch-toast {
                position: fixed !important;
                top: 15px !important;
                right: 15px !important;
                z-index: 999999 !important;
                color: #00f2fe !important;
                font-size: 0.9rem !important;
                font-weight: 600 !important;
                padding: 8px 16px !important;
                border-radius: 8px !important;
                background: rgba(0, 242, 254, 0.15) !important;
                border: 1px solid rgba(0, 242, 254, 0.3) !important;
                backdrop-filter: blur(4px) !important;
                box-shadow: 0 4px 12px rgba(0,242,254,0.1) !important;
                pointer-events: none !important;
            }
        </style>
    """, unsafe_allow_html=True)
    
    # Fetch all data concurrently
    metrics_df, risk_df, trend_df, recent_fraud_df = fetch_all_data(PROJECT_ID, DATASET)
    
    # Hide indicator once fetched
    status_placeholder.empty()

    # 1. High Level Metrics
    if not metrics_df.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        total_txns = metrics_df['total_transactions'].iloc[0]
        fraud_cnt = metrics_df['fraud_count'].iloc[0]
        fraud_amt = metrics_df['fraud_amount_usd'].iloc[0]
        fraud_rate = (fraud_cnt / total_txns * 100) if total_txns > 0 else 0
        
        col1.metric("Volume (24h)", f"{total_txns:,}")
        # Highlight critical threats in red
        st.markdown("""<style>div:nth-child(2) > div[data-testid="metric-container"] [data-testid="stMetricValue"] { background: linear-gradient(90deg, #ff416c 0%, #ff4b2b 100%); -webkit-background-clip: text; }</style>""", unsafe_allow_html=True)
        col2.metric("Flagged Threats", f"{fraud_cnt:,}")
        
        st.markdown("""<style>div:nth-child(3) > div[data-testid="metric-container"] [data-testid="stMetricValue"] { background: linear-gradient(90deg, #f7b733 0%, #fc4a1a 100%); -webkit-background-clip: text; }</style>""", unsafe_allow_html=True)
        col3.metric("Capital at Risk", f"${fraud_amt:,.0f}")
        
        st.markdown("""<style>div:nth-child(4) > div[data-testid="metric-container"] [data-testid="stMetricValue"] { background: linear-gradient(90deg, #11998e 0%, #38ef7d 100%); -webkit-background-clip: text; }</style>""", unsafe_allow_html=True)
        col4.metric("Threat Rate", f"{fraud_rate:.2f}%")
    else:
        st.info("Awaiting initial telemetry data from BigQuery...")

    st.markdown("<br><br>", unsafe_allow_html=True)

    # 2. Charts
    col1, col2 = st.columns([1, 2])

    with col1:
        st.subheader("Risk Distribution")
        if not risk_df.empty:
            fig_pie = px.pie(
                risk_df, 
                names='risk_level', 
                values='count', 
                hole=0.7,
                color='risk_level',
                color_discrete_map={'LOW': '#00f2fe', 'MEDIUM': '#f7b733', 'HIGH': '#ff416c'},
                template="plotly_dark"
            )
            fig_pie.update_layout(
                margin=dict(t=20, b=20, l=20, r=20),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                showlegend=False,
                annotations=[dict(text='RISK', x=0.5, y=0.5, font_size=24, showarrow=False, font_color="#a0aec0", font_family="Outfit")]
            )
            fig_pie.update_traces(
                textposition='outside', 
                textinfo='percent+label',
                marker=dict(line=dict(color='#151822', width=3))
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.warning("Insufficient risk distribution data.")

    with col2:
        st.subheader("Hourly Threat Vector (Last 24h)")
        if not trend_df.empty:
            fig_line = px.area(
                trend_df, 
                x='hour', 
                y='fraud_volume',
                markers=True,
                template="plotly_dark"
            )
            fig_line.update_traces(
                line=dict(color='#ff416c', width=4),
                fillcolor='rgba(255, 65, 108, 0.2)',
                marker=dict(size=10, symbol="circle", color='#ffffff', line=dict(color='#ff416c', width=3))
            )
            fig_line.update_layout(
                margin=dict(t=20, b=20, l=20, r=20),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                xaxis_title="",
                yaxis_title="Blocked Threats",
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.05)')
            )
            st.plotly_chart(fig_line, use_container_width=True)
        else:
            st.warning("Insufficient timeline data.")

    st.markdown("<br>", unsafe_allow_html=True)

    # 3. Data Table
    st.subheader("🚨 Critical Threat Feed")
    if not recent_fraud_df.empty:
        st.dataframe(
            recent_fraud_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "transaction_id": st.column_config.TextColumn(
                    "Transaction ID",
                    width="medium"
                ),
                "amount_usd": st.column_config.NumberColumn(
                    "Amount (USD)",
                    format="$ %.2f"
                ),
                "risk_score": st.column_config.ProgressColumn(
                    "Risk Score",
                    format="%d",
                    min_value=0,
                    max_value=100,
                ),
                "transaction_timestamp": st.column_config.DatetimeColumn(
                    "Timestamp",
                    format="HH:mm:ss"
                ),
                "signals_triggered": st.column_config.TextColumn(
                    "Triggered Rules"
                )
            }
        )
    else:
        st.info("No critical threats detected recently.")

live_dashboard()
