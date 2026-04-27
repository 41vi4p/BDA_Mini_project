import streamlit as st
import pandas as pd
from pymongo import MongoClient
import plotly.express as px

st.set_page_config(page_title="Energy Analytics Dashboard", layout="wide")

MONGO_URI = st.sidebar.text_input("Mongo URI", "mongodb://localhost:27017/")
DB_NAME = st.sidebar.text_input("Database", "energy_db")

@st.cache_resource
def get_db(uri, db_name):
    client = MongoClient(uri)
    return client[db_name]

def load_df(db, collection):
    docs = list(db[collection].find({}, {"_id": 0}))
    return pd.DataFrame(docs)

st.title("Real-time Energy Consumption Dashboard")

try:
    db = get_db(MONGO_URI, DB_NAME)
    collections = ["hourly_patterns", "daily_consumption", "monthly_summary", "submetering_daily"]

    counts = {}
    for c in collections:
        counts[c] = db[c].count_documents({})

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Hourly rows", counts["hourly_patterns"])
    c2.metric("Daily rows", counts["daily_consumption"])
    c3.metric("Monthly rows", counts["monthly_summary"])
    c4.metric("Submeter rows", counts["submetering_daily"])

    tab1, tab2, tab3, tab4 = st.tabs([
        "Hourly Patterns",
        "Daily Consumption",
        "Monthly Summary",
        "Sub-metering"
    ])

    with tab1:
        df = load_df(db, "hourly_patterns")
        if df.empty:
            st.warning("No hourly data found.")
        else:
            df = df.sort_values("hour")
            fig = px.line(
                df,
                x="hour",
                y=["avg_active_power", "avg_reactive_power"],
                markers=True,
                title="Average Power by Hour"
            )
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df, use_container_width=True)

    with tab2:
        df = load_df(db, "daily_consumption")
        if df.empty:
            st.warning("No daily data found.")
        else:
            df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
            df = df.sort_values("date")
            fig = px.line(
                df,
                x="date",
                y="total_kwh",
                markers=True,
                title="Daily Total kWh"
            )
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df, use_container_width=True)

    with tab3:
        df = load_df(db, "monthly_summary")
        if df.empty:
            st.warning("No monthly data found.")
        else:
            df["year_month_dt"] = pd.to_datetime(df["year_month"], format="%Y-%m", errors="coerce")
            df = df.sort_values("year_month_dt")
            fig = px.bar(
                df,
                x="year_month",
                y="total_kwh",
                title="Monthly Total kWh"
            )
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df.drop(columns=["year_month_dt"]), use_container_width=True)

    with tab4:
        df = load_df(db, "submetering_daily")
        if df.empty:
            st.warning("No sub-metering data found.")
        else:
            df["date_dt"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
            df = df.sort_values("date_dt")
            melt_df = df.melt(
                id_vars=["date_dt"],
                value_vars=["sub1_kwh", "sub2_kwh", "sub3_kwh", "other_kwh"],
                var_name="meter",
                value_name="kwh"
            )
            fig = px.line(
                melt_df,
                x="date_dt",
                y="kwh",
                color="meter",
                title="Sub-metering Components by Day"
            )
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df.drop(columns=["date_dt"]), use_container_width=True)

except Exception as e:
    st.error(f"Could not connect/read MongoDB: {e}")