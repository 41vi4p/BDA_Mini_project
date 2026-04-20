import os
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
from pymongo import MongoClient

st.set_page_config(page_title="Dashboard | Energy Analytics", page_icon="📊", layout="wide")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongodb:27017/")
MONGO_DB  = os.environ.get("MONGO_DB",  "energy_db")

SHARED_CSS = """
<style>
.metric-card{background:linear-gradient(135deg,#1E2130,#252940);border:1px solid #00D4FF33;
  border-radius:12px;padding:1.2rem 1.5rem;text-align:center;box-shadow:0 4px 20px #00D4FF11;}
.metric-card .label{font-size:.82rem;color:#8892a4;text-transform:uppercase;letter-spacing:.06em;}
.metric-card .value{font-size:2rem;font-weight:700;color:#00D4FF;margin:.25rem 0;}
.metric-card .sub{font-size:.78rem;color:#aab4c4;}
.section-title{font-size:1.1rem;font-weight:600;color:#00D4FF;border-bottom:2px solid #00D4FF33;
  padding-bottom:.4rem;margin-bottom:1rem;}
</style>
"""
st.markdown(SHARED_CSS, unsafe_allow_html=True)

PLOTLY_LAYOUT = dict(
    paper_bgcolor="#0E1117",
    plot_bgcolor="#1E2130",
    font=dict(color="#FAFAFA", family="sans-serif"),
    xaxis=dict(gridcolor="#2a3040", zerolinecolor="#2a3040"),
    yaxis=dict(gridcolor="#2a3040", zerolinecolor="#2a3040"),
    margin=dict(l=60, r=20, t=50, b=60),
)


@st.cache_resource(ttl=60)
def get_db():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)[MONGO_DB]


@st.cache_data(ttl=120)
def load_daily():
    db = get_db()
    rows = list(db["daily_consumption"].find({}, {"_id": 0}))
    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True) if rows else pd.DataFrame()


@st.cache_data(ttl=120)
def load_monthly():
    db = get_db()
    rows = list(db["monthly_summary"].find({}, {"_id": 0}))
    return pd.DataFrame(rows).sort_values("year_month").reset_index(drop=True) if rows else pd.DataFrame()


@st.cache_data(ttl=120)
def load_hourly():
    db = get_db()
    rows = list(db["hourly_patterns"].find({}, {"_id": 0}))
    return pd.DataFrame(rows).sort_values("hour").reset_index(drop=True) if rows else pd.DataFrame()


# ── Page ──────────────────────────────────────────────────────────────────────

st.title("📊 Energy Consumption Dashboard")
st.caption("Aggregated results from Hadoop MapReduce · stored in MongoDB")

daily = load_daily()
monthly = load_monthly()
hourly = load_hourly()

if daily.empty:
    st.warning("No data yet — pipeline may still be running. Refresh in a minute.")
    st.stop()

# KPIs
total_kwh = daily["total_kwh"].sum()
avg_daily  = daily["total_kwh"].mean()
peak_power = daily["max_power"].max()
best_hour  = int(hourly.loc[hourly["avg_active_power"].idxmax(), "hour"]) if not hourly.empty else "—"

st.markdown('<div class="section-title">Key Performance Indicators</div>', unsafe_allow_html=True)
c1, c2, c3, c4 = st.columns(4)
cards = [
    ("Total Consumption", f"{total_kwh:,.1f}", "kWh"),
    ("Avg Daily Usage",   f"{avg_daily:.2f}",  "kWh / day"),
    ("Peak Instantaneous Power", f"{peak_power:.2f}", "kW"),
    ("Peak Consumption Hour", f"{best_hour}:00", "hour of day"),
]
for col, (label, value, sub) in zip([c1, c2, c3, c4], cards):
    with col:
        st.markdown(f"""
        <div class="metric-card">
          <div class="label">{label}</div>
          <div class="value">{value}</div>
          <div class="sub">{sub}</div>
        </div>""", unsafe_allow_html=True)

st.divider()

# Daily consumption trend
st.markdown('<div class="section-title">Daily Energy Consumption Trend</div>', unsafe_allow_html=True)

# 7-day rolling average
daily["rolling7"] = daily["total_kwh"].rolling(7, center=True).mean()

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=daily["date"], y=daily["total_kwh"],
    name="Daily kWh",
    mode="lines",
    line=dict(color="#00D4FF", width=1.2),
    opacity=0.5,
))
fig.add_trace(go.Scatter(
    x=daily["date"], y=daily["rolling7"],
    name="7-day Avg",
    mode="lines",
    line=dict(color="#FF6B35", width=2.5),
))
fig.update_layout(**PLOTLY_LAYOUT, title="Daily Consumption (kWh)", height=340,
                  legend=dict(bgcolor="#1E2130", bordercolor="#00D4FF33"))
st.plotly_chart(fig, use_container_width=True)

# Two columns: hourly + monthly
col_l, col_r = st.columns(2)

with col_l:
    st.markdown('<div class="section-title">Average Hourly Power Demand</div>', unsafe_allow_html=True)
    if not hourly.empty:
        colors = ["#FF6B35" if h in [7, 8, 18, 19, 20, 21] else "#00D4FF"
                  for h in hourly["hour"]]
        fig2 = go.Figure(go.Bar(
            x=hourly["hour"],
            y=hourly["avg_active_power"],
            marker_color=colors,
            text=hourly["avg_active_power"].round(2),
            textposition="outside",
            textfont=dict(size=9),
        ))
        fig2.update_layout(
            **PLOTLY_LAYOUT,
            title="Avg Active Power by Hour (orange = peak)",
            height=360,
            xaxis_title="Hour of Day",
            yaxis_title="kW",
        )
        st.plotly_chart(fig2, use_container_width=True)

with col_r:
    st.markdown('<div class="section-title">Monthly Energy Consumption</div>', unsafe_allow_html=True)
    if not monthly.empty:
        fig3 = go.Figure(go.Bar(
            x=monthly["year_month"],
            y=monthly["total_kwh"],
            marker=dict(
                color=monthly["total_kwh"],
                colorscale="Blues",
                showscale=False,
            ),
        ))
        fig3.update_layout(
            **PLOTLY_LAYOUT,
            title="Total kWh per Month",
            height=360,
            xaxis_title="Month",
            yaxis_title="kWh",
            xaxis_tickangle=-45,
        )
        st.plotly_chart(fig3, use_container_width=True)

# Top 10 highest days table
st.markdown('<div class="section-title">Top 10 Highest Consumption Days</div>', unsafe_allow_html=True)
top10 = daily.nlargest(10, "total_kwh")[["date", "total_kwh", "avg_power", "max_power"]].copy()
top10.columns = ["Date", "Total kWh", "Avg Power (kW)", "Peak Power (kW)"]
top10 = top10.reset_index(drop=True)
top10.index += 1
st.dataframe(top10.style.format({"Total kWh": "{:.2f}", "Avg Power (kW)": "{:.3f}", "Peak Power (kW)": "{:.3f}"}),
             use_container_width=True)
