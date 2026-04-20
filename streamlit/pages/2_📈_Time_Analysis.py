import os
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
from pymongo import MongoClient

st.set_page_config(page_title="Time Analysis | Energy Analytics", page_icon="📈", layout="wide")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongodb:27017/")
MONGO_DB  = os.environ.get("MONGO_DB",  "energy_db")

st.markdown("""
<style>
.section-title{font-size:1.1rem;font-weight:600;color:#00D4FF;border-bottom:2px solid #00D4FF33;
  padding-bottom:.4rem;margin-bottom:1rem;}
.insight-box{background:#1E2130;border-left:4px solid #00D4FF;border-radius:6px;
  padding:.8rem 1.2rem;margin:.5rem 0;font-size:.9rem;color:#FAFAFA;}
</style>
""", unsafe_allow_html=True)

PLOTLY_LAYOUT = dict(
    paper_bgcolor="#0E1117",
    plot_bgcolor="#1E2130",
    font=dict(color="#FAFAFA"),
    xaxis=dict(gridcolor="#2a3040", zerolinecolor="#2a3040"),
    yaxis=dict(gridcolor="#2a3040", zerolinecolor="#2a3040"),
    margin=dict(l=60, r=20, t=50, b=60),
)


@st.cache_resource(ttl=60)
def get_db():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)[MONGO_DB]


@st.cache_data(ttl=120)
def load_data():
    db = get_db()
    daily = pd.DataFrame(list(db["daily_consumption"].find({}, {"_id": 0})))
    hourly = pd.DataFrame(list(db["hourly_patterns"].find({}, {"_id": 0})))
    monthly = pd.DataFrame(list(db["monthly_summary"].find({}, {"_id": 0})))
    return daily, hourly, monthly


st.title("📈 Time-Series & Pattern Analysis")
st.caption("Temporal patterns discovered via MapReduce on Hadoop HDFS")

try:
    daily, hourly, monthly = load_data()
except Exception as e:
    st.error(f"Cannot connect to MongoDB: {e}")
    st.stop()

if daily.empty:
    st.warning("No data yet — pipeline may still be running.")
    st.stop()

daily = daily.sort_values("date").reset_index(drop=True)
daily["datetime"] = pd.to_datetime(daily["date"])
daily["day_of_week"] = daily["datetime"].dt.dayofweek
daily["week_number"] = daily["datetime"].dt.isocalendar().week.astype(int)
daily["year"] = daily["datetime"].dt.year
daily["month"] = daily["datetime"].dt.month

# ── Sidebar filters ───────────────────────────────────────────────────────────
st.sidebar.header("Filters")
years = sorted(daily["year"].unique())
sel_years = st.sidebar.multiselect("Year", years, default=years)
filtered = daily[daily["year"].isin(sel_years)]

# ── 1. Day-of-week heatmap ────────────────────────────────────────────────────
st.markdown('<div class="section-title">Day-of-Week vs Month Heatmap</div>', unsafe_allow_html=True)

dow_month = filtered.groupby(["day_of_week", "month"])["total_kwh"].mean().reset_index()
pivot = dow_month.pivot(index="day_of_week", columns="month", values="total_kwh")
dow_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
month_labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

fig_heat = go.Figure(go.Heatmap(
    z=pivot.values,
    x=[month_labels[m - 1] for m in pivot.columns],
    y=[dow_labels[i] for i in pivot.index],
    colorscale="YlOrRd",
    colorbar=dict(title="Avg kWh"),
    hovertemplate="Month: %{x}<br>Day: %{y}<br>Avg: %{z:.2f} kWh<extra></extra>",
))
fig_heat.update_layout(**PLOTLY_LAYOUT, title="Avg Daily Consumption by Day-of-Week × Month", height=300)
st.plotly_chart(fig_heat, use_container_width=True)

# ── 2. Hourly demand with confidence band ─────────────────────────────────────
st.markdown('<div class="section-title">Hourly Demand Profile</div>', unsafe_allow_html=True)

if not hourly.empty:
    col_a, col_b = st.columns([2, 1])
    with col_a:
        hourly_s = hourly.sort_values("hour")
        fig_h = go.Figure()
        # Shade peak hours 7-9 AM and 6-10 PM
        for x0, x1 in [(7, 9), (18, 22)]:
            fig_h.add_vrect(x0=x0, x1=x1, fillcolor="#FF6B35", opacity=0.08, line_width=0)
        fig_h.add_trace(go.Scatter(
            x=hourly_s["hour"],
            y=hourly_s["avg_active_power"],
            name="Avg Power",
            mode="lines+markers",
            line=dict(color="#00D4FF", width=3),
            marker=dict(size=8, color="#00D4FF"),
        ))
        fig_h.add_trace(go.Scatter(
            x=hourly_s["hour"],
            y=hourly_s["max_active_power"],
            name="Max Power",
            mode="lines",
            line=dict(color="#FF6B35", width=1.5, dash="dot"),
        ))
        fig_h.update_layout(
            **PLOTLY_LAYOUT,
            title="Hourly Power Profile (shaded = typical peak hours)",
            height=360,
            xaxis=dict(tickmode="linear", tick0=0, dtick=1, title="Hour of Day",
                       gridcolor="#2a3040"),
            yaxis_title="kW",
            legend=dict(bgcolor="#1E2130"),
        )
        st.plotly_chart(fig_h, use_container_width=True)

    with col_b:
        st.markdown('<div class="section-title">Hourly Insights</div>', unsafe_allow_html=True)
        peak_h = hourly.loc[hourly["avg_active_power"].idxmax()]
        low_h  = hourly.loc[hourly["avg_active_power"].idxmin()]
        diff_pct = (peak_h["avg_active_power"] - low_h["avg_active_power"]) / low_h["avg_active_power"] * 100

        st.markdown(f'<div class="insight-box">🔴 Peak Hour: <b>{int(peak_h["hour"])}:00</b><br>'
                    f'Avg {peak_h["avg_active_power"]:.3f} kW</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="insight-box">🟢 Lowest Hour: <b>{int(low_h["hour"])}:00</b><br>'
                    f'Avg {low_h["avg_active_power"]:.3f} kW</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="insight-box">📊 Peak/Low Ratio: <b>{diff_pct:.0f}%</b> higher<br>'
                    f'demand at peak vs off-peak</div>', unsafe_allow_html=True)

        avg_voltage = hourly["avg_voltage"].mean()
        st.markdown(f'<div class="insight-box">⚡ Avg Voltage: <b>{avg_voltage:.1f} V</b></div>',
                    unsafe_allow_html=True)

# ── 3. Monthly trend with year overlay ───────────────────────────────────────
st.markdown('<div class="section-title">Monthly Consumption by Year</div>', unsafe_allow_html=True)

if not monthly.empty:
    monthly_df = monthly.sort_values("year_month").copy()
    monthly_df["year"] = monthly_df["year_month"].str[:4]
    monthly_df["month_num"] = monthly_df["year_month"].str[5:].astype(int)
    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    monthly_df["month_name"] = monthly_df["month_num"].apply(lambda x: month_names[x - 1])

    fig_m = go.Figure()
    colors_yr = ["#00D4FF", "#FF6B35", "#00FF88", "#FFB300"]
    for i, yr in enumerate(sorted(monthly_df["year"].unique())):
        yd = monthly_df[monthly_df["year"] == yr]
        fig_m.add_trace(go.Scatter(
            x=yd["month_name"],
            y=yd["total_kwh"],
            name=yr,
            mode="lines+markers",
            line=dict(color=colors_yr[i % len(colors_yr)], width=2.5),
            marker=dict(size=8),
        ))
    fig_m.update_layout(
        **PLOTLY_LAYOUT,
        title="Monthly Total kWh (year-over-year)",
        height=360,
        xaxis_title="Month",
        yaxis_title="kWh",
        legend=dict(bgcolor="#1E2130", bordercolor="#00D4FF33"),
    )
    st.plotly_chart(fig_m, use_container_width=True)

# ── 4. Rolling statistics ─────────────────────────────────────────────────────
st.markdown('<div class="section-title">Rolling Statistics (30-day window)</div>', unsafe_allow_html=True)

filtered = filtered.copy()
filtered["roll_mean"] = filtered["total_kwh"].rolling(30).mean()
filtered["roll_std"]  = filtered["total_kwh"].rolling(30).std()
filtered["upper"]     = filtered["roll_mean"] + filtered["roll_std"]
filtered["lower"]     = filtered["roll_mean"] - filtered["roll_std"]

fig_roll = go.Figure()
fig_roll.add_trace(go.Scatter(
    x=pd.concat([filtered["date"], filtered["date"][::-1]]),
    y=pd.concat([filtered["upper"], filtered["lower"][::-1]]),
    fill="toself",
    fillcolor="#00D4FF22",
    line=dict(color="rgba(0,0,0,0)"),
    name="±1 Std Dev",
    showlegend=True,
))
fig_roll.add_trace(go.Scatter(
    x=filtered["date"],
    y=filtered["roll_mean"],
    name="30-day Rolling Mean",
    line=dict(color="#00D4FF", width=2.5),
))
fig_roll.add_trace(go.Scatter(
    x=filtered["date"],
    y=filtered["total_kwh"],
    name="Daily kWh",
    mode="lines",
    line=dict(color="#ffffff", width=0.8),
    opacity=0.3,
))
fig_roll.update_layout(**PLOTLY_LAYOUT, title="Daily kWh with 30-day Rolling Statistics", height=360)
st.plotly_chart(fig_roll, use_container_width=True)
