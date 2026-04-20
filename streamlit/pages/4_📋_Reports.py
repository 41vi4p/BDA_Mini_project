import os
import io
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from pymongo import MongoClient

st.set_page_config(page_title="Reports | Energy Analytics", page_icon="📋", layout="wide")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongodb:27017/")
MONGO_DB  = os.environ.get("MONGO_DB",  "energy_db")

st.markdown("""
<style>
.section-title{font-size:1.1rem;font-weight:600;color:#00D4FF;border-bottom:2px solid #00D4FF33;
  padding-bottom:.4rem;margin-bottom:1rem;}
.stat-row{display:flex;gap:1rem;flex-wrap:wrap;margin-bottom:1rem;}
.stat-item{background:#1E2130;border-radius:8px;padding:.8rem 1.2rem;flex:1;min-width:140px;
  border:1px solid #ffffff11;}
.stat-item .s-label{font-size:.78rem;color:#8892a4;}
.stat-item .s-val{font-size:1.3rem;font-weight:700;color:#00D4FF;}
</style>
""", unsafe_allow_html=True)

PLOTLY_LAYOUT = dict(
    paper_bgcolor="#0E1117", plot_bgcolor="#1E2130",
    font=dict(color="#FAFAFA"),
    xaxis=dict(gridcolor="#2a3040"), yaxis=dict(gridcolor="#2a3040"),
    margin=dict(l=60, r=20, t=50, b=60),
)


@st.cache_resource(ttl=60)
def get_db():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)[MONGO_DB]


@st.cache_data(ttl=120)
def load_all():
    db = get_db()
    daily   = pd.DataFrame(list(db["daily_consumption"].find({}, {"_id": 0})))
    monthly = pd.DataFrame(list(db["monthly_summary"].find({}, {"_id": 0})))
    hourly  = pd.DataFrame(list(db["hourly_patterns"].find({}, {"_id": 0})))
    sub     = pd.DataFrame(list(db["submetering_daily"].find({}, {"_id": 0})))
    status  = db["pipeline_status"].find_one(sort=[("_id", -1)])
    return daily, monthly, hourly, sub, status


st.title("📋 Reports & Data Export")
st.caption("Summary statistics · tabular views · CSV download")

try:
    daily, monthly, hourly, sub, status = load_all()
except Exception as e:
    st.error(f"MongoDB error: {e}")
    st.stop()

if daily.empty:
    st.warning("No data yet — run the pipeline first.")
    st.stop()

daily   = daily.sort_values("date").reset_index(drop=True)
monthly = monthly.sort_values("year_month").reset_index(drop=True)

# ── Pipeline status ───────────────────────────────────────────────────────────
st.markdown('<div class="section-title">Pipeline Status</div>', unsafe_allow_html=True)
if status:
    import time as _time
    pipe_status = status.get("status", "unknown")
    completed   = status.get("completed_at")
    color = {"completed": "#00FF88", "running": "#FFB300", "failed": "#FF4444"}.get(pipe_status, "#FAFAFA")
    st.markdown(f'<span style="color:{color};font-weight:600;font-size:1.1rem;">'
                f'● {pipe_status.upper()}</span>', unsafe_allow_html=True)
    if completed:
        import datetime
        ts = datetime.datetime.fromtimestamp(completed).strftime("%Y-%m-%d %H:%M:%S")
        st.caption(f"Completed at: {ts}")

st.divider()

# ── Summary statistics ────────────────────────────────────────────────────────
st.markdown('<div class="section-title">Overall Statistics</div>', unsafe_allow_html=True)

total_kwh    = daily["total_kwh"].sum()
avg_daily    = daily["total_kwh"].mean()
std_daily    = daily["total_kwh"].std()
max_day      = daily.loc[daily["total_kwh"].idxmax()]
min_day      = daily.loc[daily["total_kwh"].idxmin()]
peak_h_row   = hourly.loc[hourly["avg_active_power"].idxmax()] if not hourly.empty else None

stats_html = f"""
<div class="stat-row">
  <div class="stat-item"><div class="s-label">Total kWh</div><div class="s-val">{total_kwh:,.1f}</div></div>
  <div class="stat-item"><div class="s-label">Days Recorded</div><div class="s-val">{len(daily)}</div></div>
  <div class="stat-item"><div class="s-label">Avg Daily kWh</div><div class="s-val">{avg_daily:.2f}</div></div>
  <div class="stat-item"><div class="s-label">Std Dev Daily</div><div class="s-val">{std_daily:.2f}</div></div>
  <div class="stat-item"><div class="s-label">Max Daily kWh</div><div class="s-val">{max_day['total_kwh']:.2f}</div></div>
  <div class="stat-item"><div class="s-label">Min Daily kWh</div><div class="s-val">{min_day['total_kwh']:.2f}</div></div>
</div>
"""
st.markdown(stats_html, unsafe_allow_html=True)
if peak_h_row is not None:
    st.info(f"Peak demand hour: **{int(peak_h_row['hour'])}:00** "
            f"— avg {peak_h_row['avg_active_power']:.3f} kW, "
            f"max {peak_h_row['max_active_power']:.3f} kW")

st.divider()

# ── Monthly summary table ─────────────────────────────────────────────────────
st.markdown('<div class="section-title">Monthly Summary Table</div>', unsafe_allow_html=True)
monthly_disp = monthly.copy()
monthly_disp.columns = ["Month", "Total kWh", "Avg Power (kW)", "Peak Power (kW)", "Readings"]
st.dataframe(
    monthly_disp.style
    .format({"Total kWh": "{:.2f}", "Avg Power (kW)": "{:.3f}", "Peak Power (kW)": "{:.3f}"})
    .background_gradient(subset=["Total kWh"], cmap="Blues"),
    use_container_width=True,
    height=420,
)

# ── Top 20 days ───────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">Top 20 Highest Consumption Days</div>', unsafe_allow_html=True)
top20 = daily.nlargest(20, "total_kwh")[["date", "total_kwh", "avg_power", "max_power", "count"]].copy()
top20.columns = ["Date", "Total kWh", "Avg Power (kW)", "Peak Power (kW)", "Readings"]
top20 = top20.reset_index(drop=True)
top20.index += 1
st.dataframe(
    top20.style.format({"Total kWh": "{:.2f}", "Avg Power (kW)": "{:.3f}", "Peak Power (kW)": "{:.3f}"}),
    use_container_width=True,
    height=560,
)

# ── Hourly profile table ──────────────────────────────────────────────────────
if not hourly.empty:
    st.markdown('<div class="section-title">Hourly Profile Table</div>', unsafe_allow_html=True)
    hourly_disp = hourly.sort_values("hour")[
        ["hour", "avg_active_power", "avg_reactive_power", "avg_voltage", "avg_intensity", "max_active_power", "count"]
    ].copy()
    hourly_disp.columns = ["Hour", "Avg Active (kW)", "Avg Reactive (kW)", "Avg Voltage (V)",
                           "Avg Intensity (A)", "Max Active (kW)", "Readings"]
    hourly_disp = hourly_disp.reset_index(drop=True)
    st.dataframe(
        hourly_disp.style.format({c: "{:.3f}" for c in hourly_disp.columns if c != "Hour" and c != "Readings"}),
        use_container_width=True,
    )

st.divider()

# ── CSV Export ────────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">Export Data</div>', unsafe_allow_html=True)
col1, col2, col3, col4 = st.columns(4)

def to_csv(df):
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode()

with col1:
    st.download_button(
        "⬇ Daily Consumption CSV",
        data=to_csv(daily),
        file_name="daily_consumption.csv",
        mime="text/csv",
        use_container_width=True,
    )

with col2:
    st.download_button(
        "⬇ Monthly Summary CSV",
        data=to_csv(monthly),
        file_name="monthly_summary.csv",
        mime="text/csv",
        use_container_width=True,
    )

with col3:
    if not hourly.empty:
        st.download_button(
            "⬇ Hourly Profile CSV",
            data=to_csv(hourly),
            file_name="hourly_patterns.csv",
            mime="text/csv",
            use_container_width=True,
        )

with col4:
    if not sub.empty:
        st.download_button(
            "⬇ Sub-metering CSV",
            data=to_csv(sub),
            file_name="submetering_daily.csv",
            mime="text/csv",
            use_container_width=True,
        )
