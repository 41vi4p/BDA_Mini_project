import os
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
from pymongo import MongoClient

st.set_page_config(page_title="Sub-Metering | Energy Analytics", page_icon="🔌", layout="wide")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongodb:27017/")
MONGO_DB  = os.environ.get("MONGO_DB",  "energy_db")

st.markdown("""
<style>
.section-title{font-size:1.1rem;font-weight:600;color:#00D4FF;border-bottom:2px solid #00D4FF33;
  padding-bottom:.4rem;margin-bottom:1rem;}
.sub-card{background:#1E2130;border-radius:10px;padding:1rem 1.2rem;border:1px solid #ffffff11;}
.sub-card .name{font-size:.85rem;color:#8892a4;margin-bottom:.2rem;}
.sub-card .val{font-size:1.6rem;font-weight:700;}
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

SUB_COLORS = {
    "Kitchen (Sub-1)":    "#00D4FF",
    "Laundry (Sub-2)":   "#FF6B35",
    "HVAC/Water (Sub-3)":"#00FF88",
    "Other Loads":       "#FFB300",
}
SUB_MAP = {
    "sub1_kwh": "Kitchen (Sub-1)",
    "sub2_kwh": "Laundry (Sub-2)",
    "sub3_kwh": "HVAC/Water (Sub-3)",
    "other_kwh": "Other Loads",
}


@st.cache_resource(ttl=60)
def get_db():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)[MONGO_DB]


@st.cache_data(ttl=120)
def load_sub():
    db = get_db()
    rows = list(db["submetering_daily"].find({}, {"_id": 0}))
    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True) if rows else pd.DataFrame()


st.title("🔌 Sub-Metering Breakdown")
st.caption("Energy breakdown by circuit — Kitchen · Laundry · HVAC/Water · Other")

try:
    sub = load_sub()
except Exception as e:
    st.error(f"MongoDB error: {e}")
    st.stop()

if sub.empty:
    st.warning("No sub-metering data yet.")
    st.stop()

sub["datetime"] = pd.to_datetime(sub["date"])

# Sidebar filter
st.sidebar.header("Filters")
date_range = st.sidebar.date_input(
    "Date range",
    value=(sub["datetime"].min().date(), sub["datetime"].max().date()),
)
if len(date_range) == 2:
    sub = sub[(sub["datetime"].dt.date >= date_range[0]) &
              (sub["datetime"].dt.date <= date_range[1])]

# ── Overall totals ────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">Total Energy by Circuit</div>', unsafe_allow_html=True)
totals = {
    "Kitchen (Sub-1)":    sub["sub1_kwh"].sum(),
    "Laundry (Sub-2)":   sub["sub2_kwh"].sum(),
    "HVAC/Water (Sub-3)":sub["sub3_kwh"].sum(),
    "Other Loads":       sub["other_kwh"].sum(),
}
grand_total = sum(totals.values())

c1, c2, c3, c4, c5 = st.columns(5)
columns = [c1, c2, c3, c4]
for col, (name, val) in zip(columns, totals.items()):
    pct = val / grand_total * 100 if grand_total else 0
    color = SUB_COLORS[name]
    with col:
        st.markdown(f"""
        <div class="sub-card">
          <div class="name">{name}</div>
          <div class="val" style="color:{color};">{val:.1f} kWh</div>
          <div style="font-size:.8rem;color:#8892a4;">{pct:.1f}% of total</div>
        </div>""", unsafe_allow_html=True)

with c5:
    st.markdown(f"""
    <div class="sub-card">
      <div class="name">Grand Total</div>
      <div class="val" style="color:#FAFAFA;">{grand_total:.1f} kWh</div>
      <div style="font-size:.8rem;color:#8892a4;">100%</div>
    </div>""", unsafe_allow_html=True)

st.divider()

# Two cols: pie + stacked bar
col_l, col_r = st.columns([1, 2])

with col_l:
    st.markdown('<div class="section-title">Energy Distribution</div>', unsafe_allow_html=True)
    fig_pie = go.Figure(go.Pie(
        labels=list(totals.keys()),
        values=list(totals.values()),
        hole=0.55,
        marker=dict(colors=list(SUB_COLORS.values()), line=dict(color="#0E1117", width=2)),
        textinfo="label+percent",
        textfont=dict(size=12),
        hovertemplate="%{label}<br>%{value:.2f} kWh<br>%{percent}<extra></extra>",
    ))
    fig_pie.add_annotation(
        text=f"{grand_total:.0f}<br>kWh",
        x=0.5, y=0.5,
        showarrow=False,
        font=dict(size=16, color="#FAFAFA"),
    )
    fig_pie.update_layout(
        paper_bgcolor="#0E1117",
        font=dict(color="#FAFAFA"),
        showlegend=False,
        height=380,
        margin=dict(l=10, r=10, t=20, b=10),
    )
    st.plotly_chart(fig_pie, use_container_width=True)

with col_r:
    st.markdown('<div class="section-title">Monthly Sub-meter Breakdown</div>', unsafe_allow_html=True)
    sub_m = sub.copy()
    sub_m["year_month"] = sub_m["datetime"].dt.to_period("M").astype(str)
    monthly_sub = sub_m.groupby("year_month")[
        ["sub1_kwh", "sub2_kwh", "sub3_kwh", "other_kwh"]
    ].sum().reset_index()

    fig_stack = go.Figure()
    for col_name, display_name in SUB_MAP.items():
        fig_stack.add_trace(go.Bar(
            x=monthly_sub["year_month"],
            y=monthly_sub[col_name],
            name=display_name,
            marker_color=SUB_COLORS[display_name],
        ))
    fig_stack.update_layout(
        **PLOTLY_LAYOUT,
        barmode="stack",
        title="Monthly kWh by Circuit",
        height=380,
        xaxis_tickangle=-45,
        legend=dict(bgcolor="#1E2130", bordercolor="#00D4FF33"),
    )
    st.plotly_chart(fig_stack, use_container_width=True)

# ── Time-series for each sub-meter ────────────────────────────────────────────
st.markdown('<div class="section-title">Sub-meter Time Series (7-day rolling avg)</div>',
            unsafe_allow_html=True)

# Resample to weekly for readability
sub_weekly = sub.set_index("datetime").resample("W")[
    ["sub1_kwh", "sub2_kwh", "sub3_kwh", "other_kwh"]
].sum().reset_index()

fig_ts = go.Figure()
for col_name, display_name in SUB_MAP.items():
    fig_ts.add_trace(go.Scatter(
        x=sub_weekly["datetime"],
        y=sub_weekly[col_name],
        name=display_name,
        mode="lines",
        line=dict(color=SUB_COLORS[display_name], width=2),
    ))
fig_ts.update_layout(
    **PLOTLY_LAYOUT,
    title="Weekly kWh per Circuit",
    height=380,
    legend=dict(bgcolor="#1E2130", bordercolor="#00D4FF33"),
    yaxis_title="kWh",
)
st.plotly_chart(fig_ts, use_container_width=True)

# ── Correlation matrix ────────────────────────────────────────────────────────
st.markdown('<div class="section-title">Circuit Correlation</div>', unsafe_allow_html=True)
corr = sub[["sub1_kwh", "sub2_kwh", "sub3_kwh", "other_kwh"]].corr()
corr.index   = [SUB_MAP[c] for c in corr.index]
corr.columns = [SUB_MAP[c] for c in corr.columns]

fig_corr = go.Figure(go.Heatmap(
    z=corr.values,
    x=corr.columns.tolist(),
    y=corr.index.tolist(),
    colorscale="RdBu",
    zmid=0,
    zmin=-1, zmax=1,
    text=corr.round(2).values,
    texttemplate="%{text}",
    colorbar=dict(title="r"),
))
fig_corr.update_layout(
    paper_bgcolor="#0E1117",
    font=dict(color="#FAFAFA"),
    height=320,
    margin=dict(l=120, r=20, t=30, b=30),
    title="Pearson Correlation between Circuits",
)
st.plotly_chart(fig_corr, use_container_width=True)
