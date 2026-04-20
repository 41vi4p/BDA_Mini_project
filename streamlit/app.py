import os
import time
import requests
import streamlit as st
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

st.set_page_config(
    page_title="Energy Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongodb:27017/")
MONGO_DB = os.environ.get("MONGO_DB", "energy_db")
NAMENODE_HOST = os.environ.get("NAMENODE_HOST", "namenode")

# ── Shared CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  /* Sidebar nav */
  [data-testid="stSidebarNav"] { padding-top: 1rem; }

  /* Metric cards */
  .metric-card {
      background: linear-gradient(135deg, #1E2130 0%, #252940 100%);
      border: 1px solid #00D4FF33;
      border-radius: 12px;
      padding: 1.2rem 1.5rem;
      text-align: center;
      box-shadow: 0 4px 20px #00D4FF11;
  }
  .metric-card .label {
      font-size: 0.82rem;
      color: #8892a4;
      text-transform: uppercase;
      letter-spacing: 0.06em;
  }
  .metric-card .value {
      font-size: 2rem;
      font-weight: 700;
      color: #00D4FF;
      margin: 0.25rem 0;
  }
  .metric-card .sub {
      font-size: 0.78rem;
      color: #aab4c4;
  }

  /* Status badges */
  .badge-ok   { background:#00FF8833; color:#00FF88; border:1px solid #00FF8866;
                border-radius:6px; padding:3px 10px; font-size:0.78rem; }
  .badge-warn { background:#FFB30033; color:#FFB300; border:1px solid #FFB30066;
                border-radius:6px; padding:3px 10px; font-size:0.78rem; }
  .badge-err  { background:#FF444433; color:#FF4444; border:1px solid #FF444466;
                border-radius:6px; padding:3px 10px; font-size:0.78rem; }

  /* Hero banner */
  .hero {
      background: linear-gradient(135deg, #0a0e1a 0%, #0d1a2e 50%, #091520 100%);
      border: 1px solid #00D4FF22;
      border-radius: 16px;
      padding: 2rem 2.5rem;
      margin-bottom: 2rem;
  }
  .hero h1 { color: #00D4FF; margin: 0; font-size: 2.4rem; }
  .hero p  { color: #8892a4; margin: 0.5rem 0 0; font-size: 1rem; }

  /* Section headers */
  .section-title {
      font-size: 1.1rem;
      font-weight: 600;
      color: #00D4FF;
      border-bottom: 2px solid #00D4FF33;
      padding-bottom: 0.4rem;
      margin-bottom: 1rem;
  }
</style>
""", unsafe_allow_html=True)


@st.cache_resource(ttl=30)
def get_db():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    return client[MONGO_DB]


def hdfs_status():
    try:
        r = requests.get(
            f"http://{NAMENODE_HOST}:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus",
            timeout=3
        )
        return r.status_code == 200
    except Exception:
        return False


def mongo_status():
    try:
        db = get_db()
        db.client.server_info()
        return True
    except Exception:
        return False


def pipeline_status():
    try:
        db = get_db()
        doc = db["pipeline_status"].find_one(sort=[("_id", -1)])
        return doc.get("status", "unknown") if doc else "not started"
    except Exception:
        return "unknown"


# ── Page content ──────────────────────────────────────────────────────────────

st.markdown("""
<div class="hero">
  <h1>⚡ Real-time Energy Consumption Analysis</h1>
  <p>Big Data Analytics Mini Project &nbsp;|&nbsp; Household Power Consumption &nbsp;|&nbsp;
     MapReduce on Hadoop · MongoDB · Streamlit</p>
</div>
""", unsafe_allow_html=True)

# System status row
st.markdown('<div class="section-title">System Status</div>', unsafe_allow_html=True)
c1, c2, c3, c4 = st.columns(4)

hdfs_ok = hdfs_status()
mongo_ok = mongo_status()
pipe_st = pipeline_status()

with c1:
    badge = "badge-ok" if hdfs_ok else "badge-err"
    icon = "✅" if hdfs_ok else "❌"
    st.markdown(f'<span class="{badge}">{icon} Hadoop HDFS</span>', unsafe_allow_html=True)
    st.caption("NameNode WebUI :9870")

with c2:
    badge = "badge-ok" if mongo_ok else "badge-err"
    icon = "✅" if mongo_ok else "❌"
    st.markdown(f'<span class="{badge}">{icon} MongoDB</span>', unsafe_allow_html=True)
    st.caption("Port 27017")

with c3:
    color_map = {"completed": "badge-ok", "running": "badge-warn",
                 "failed": "badge-err", "not started": "badge-warn"}
    badge = color_map.get(pipe_st, "badge-warn")
    icon = {"completed": "✅", "running": "⏳", "failed": "❌"}.get(pipe_st, "⏸")
    st.markdown(f'<span class="{badge}">{icon} Pipeline: {pipe_st.title()}</span>',
                unsafe_allow_html=True)
    st.caption("MapReduce ETL")

with c4:
    st.markdown('<span class="badge-ok">✅ Streamlit</span>', unsafe_allow_html=True)
    st.caption("Port 8501")

st.divider()

# Dataset overview
st.markdown('<div class="section-title">Dataset Overview</div>', unsafe_allow_html=True)

try:
    db = get_db()
    daily = list(db["daily_consumption"].find({}, {"_id": 0}))
    monthly = list(db["monthly_summary"].find({}, {"_id": 0}))

    if daily:
        total_kwh = sum(d["total_kwh"] for d in daily)
        avg_daily = total_kwh / len(daily)
        max_day = max(daily, key=lambda x: x["total_kwh"])
        dates = sorted(d["date"] for d in daily)

        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.markdown(f"""
            <div class="metric-card">
              <div class="label">Total Energy Consumed</div>
              <div class="value">{total_kwh:,.0f}</div>
              <div class="sub">kWh over dataset period</div>
            </div>""", unsafe_allow_html=True)
        with m2:
            st.markdown(f"""
            <div class="metric-card">
              <div class="label">Avg Daily Consumption</div>
              <div class="value">{avg_daily:.1f}</div>
              <div class="sub">kWh per day</div>
            </div>""", unsafe_allow_html=True)
        with m3:
            st.markdown(f"""
            <div class="metric-card">
              <div class="label">Data Coverage</div>
              <div class="value">{len(daily)}</div>
              <div class="sub">days of records</div>
            </div>""", unsafe_allow_html=True)
        with m4:
            st.markdown(f"""
            <div class="metric-card">
              <div class="label">Peak Day</div>
              <div class="value">{max_day['total_kwh']:.1f}</div>
              <div class="sub">kWh on {max_day['date']}</div>
            </div>""", unsafe_allow_html=True)

        st.divider()
        st.markdown('<div class="section-title">Date Range</div>', unsafe_allow_html=True)
        col_a, col_b, col_c = st.columns(3)
        col_a.metric("First Record", dates[0])
        col_b.metric("Last Record", dates[-1])
        col_c.metric("Total Months", len(monthly))
    else:
        st.info("⏳ Pipeline is still processing data. Check back shortly.")
        st.markdown("""
        **Pipeline stages:**
        1. Upload CSV → HDFS
        2. Run 4 MapReduce jobs (Hourly / Daily / Monthly / Sub-metering)
        3. Store results → MongoDB
        4. Visualize here
        """)
except Exception as e:
    st.warning(f"Could not connect to MongoDB: {e}")

st.divider()

# Architecture diagram
with st.expander("System Architecture", expanded=False):
    st.markdown("""
    ```
    ┌─────────────────────────────────────────────────────────┐
    │                   Docker Network                        │
    │                                                         │
    │  CSV File ──► HDFS (NameNode + DataNode)                │
    │                    │                                    │
    │                    ▼                                    │
    │         Hadoop MapReduce (YARN)                         │
    │         ├── Job 1: Hourly Patterns                      │
    │         ├── Job 2: Daily Consumption                    │
    │         ├── Job 3: Monthly Summary                      │
    │         └── Job 4: Sub-metering Breakdown               │
    │                    │                                    │
    │                    ▼                                    │
    │              MongoDB :27017                             │
    │                    │                                    │
    │                    ▼                                    │
    │           Streamlit Dashboard :8501                     │
    └─────────────────────────────────────────────────────────┘
    ```
    **Services:**
    | Service | URL |
    |---------|-----|
    | Streamlit Dashboard | http://localhost:8501 |
    | Hadoop NameNode UI  | http://localhost:9870 |
    | YARN ResourceManager| http://localhost:8088 |
    | Job History Server  | http://localhost:8188 |
    | Mongo Express       | http://localhost:8081 (admin/admin123) |
    """)

# Auto-refresh when pipeline is running
if pipe_st == "running":
    time.sleep(1)
    st.rerun()
