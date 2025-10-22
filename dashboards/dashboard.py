import streamlit as st
import pandas as pd
import plotly.express as px
from psycopg2 import pool
import time
from streamlit_autorefresh import st_autorefresh

# -----------------------
# Connection Pool
# -----------------------
conn_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=5,
    host="localhost",
    port=5432,
    dbname="churn_db",
    user="admin",
    password="admin"
)

# -----------------------
# Streamlit Page Config
# -----------------------
st.set_page_config(page_title="Churn Simulation Dashboard", layout="wide")
st.title("Churn Simulation Dashboard")

# -----------------------
# Apply Custom CSS
# -----------------------
st.markdown("""
<style>
    .stMetricDelta { color: #FF4B4B; font-weight: bold; }
    .stMetricValue { color: #1F77B4; font-size: 26px; font-weight: bold; }
    .stAlert { background-color: #FFF0F0; border-left: 4px solid #FF4B4B; padding: 10px; }
    .block-container { padding: 1rem 2rem 2rem 2rem; }
</style>
""", unsafe_allow_html=True)

# -----------------------
# Load Data
# -----------------------
@st.cache_data(ttl=8)
def load_data():
    conn = conn_pool.getconn()
    try:
        daily_signups = pd.read_sql("SELECT * FROM daily_signups_summary ORDER BY day", conn)
        plan_dist = pd.read_sql("SELECT * FROM plan_distribution_summary", conn)
        region_device = pd.read_sql("SELECT * FROM signups_by_region_device", conn)
    finally:
        conn_pool.putconn(conn)
    return daily_signups, plan_dist, region_device

daily_signups, plan_dist, region_device = load_data()

# -----------------------
# KPIs
# -----------------------
st.subheader("Key Metrics")
col1, col2, col3, col4 = st.columns(4)

total_signups = daily_signups['total_signups'].sum() if not daily_signups.empty else 0
today_signups = daily_signups['total_signups'].iloc[-1] if not daily_signups.empty else 0
yesterday_signups = daily_signups['total_signups'].iloc[-2] if len(daily_signups) > 1 else 0
unique_regions = daily_signups['unique_regions'].iloc[-1] if not daily_signups.empty else 0
avg_daily_signups = int(daily_signups['total_signups'].mean()) if not daily_signups.empty else 0

col1.metric("Total Signups", total_signups)
col2.metric("Signups Today", today_signups, delta=today_signups - yesterday_signups)
col3.metric("Regions Today", unique_regions)
col4.metric("Avg Daily Signups", avg_daily_signups)

# -----------------------
# Tabs for Charts
# -----------------------
tab1, tab2, tab3 = st.tabs(["Trends", "Plan Distribution", "Region & Device"])

with tab1:
    st.subheader("Daily Signups Trend")
    fig1 = px.line(daily_signups, x='day', y='total_signups', markers=True, template='plotly_white')
    fig1.update_layout(xaxis_title="Date", yaxis_title="Signups")
    st.plotly_chart(fig1, use_container_width=True)

with tab2:
    st.subheader("Plan Distribution")
    fig2 = px.pie(plan_dist, names='plan', values='count', color_discrete_sequence=px.colors.sequential.RdBu)
    st.plotly_chart(fig2, use_container_width=True)

with tab3:
    st.subheader("Signups by Region & Device")
    fig3 = px.bar(region_device, x='region', y='count', color='device', barmode='group', template='plotly_white')
    st.plotly_chart(fig3, use_container_width=True)

# -----------------------
# Alerts
# -----------------------
st.subheader("Alerts")
threshold = avg_daily_signups * 0.5
if today_signups < threshold:
    st.warning("🚨 Signups today dropped more than 50% compared to average!")
else:
    st.success("✅ Signups are within normal range.")

# -----------------------
# Expanders for raw tables
# -----------------------
with st.expander("Show raw data tables"):
    st.dataframe(daily_signups)
    st.dataframe(plan_dist)
    st.dataframe(region_device)

# Auto-refresh every 60s
st_autorefresh(interval=8*1000, key="dashboard_refresh")