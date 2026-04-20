# ⚡ Real-time Energy Consumption Analysis
**Big Data Analytics Mini Project**

Analyses ~1 million rows of household power consumption data using Apache Hadoop MapReduce, stores results in MongoDB, and visualises everything in a Streamlit dashboard — all running in Docker.

---

## Architecture

```
household_power_consumption.csv
        │
        ▼
  HDFS (NameNode + DataNode)
        │
        ▼
  Hadoop MapReduce (YARN)
  ├── Job 1: Hourly Patterns      → hourly_patterns
  ├── Job 2: Daily Consumption    → daily_consumption
  ├── Job 3: Monthly Summary      → monthly_summary
  └── Job 4: Sub-metering         → submetering_daily
        │
        ▼
    MongoDB :27017
        │
        ▼
  Streamlit Dashboard :8501
```

## Quick Start

```bash
./run.sh
```

Or manually:
```bash
docker compose up -d
docker logs -f pipeline   # watch MapReduce progress
```

## Service URLs

| Service | URL |
|---------|-----|
| Streamlit Dashboard | http://localhost:8501 |
| Hadoop NameNode UI  | http://localhost:9870 |
| YARN ResourceManager| http://localhost:8088 |
| Job History Server  | http://localhost:8188 |
| Mongo Express       | http://localhost:8081 (admin/admin123) |

## Dashboard Pages

1. **Home** — system status, dataset overview, KPIs
2. **Dashboard** — daily trend, hourly patterns, monthly bars
3. **Time Analysis** — day-of-week heatmap, rolling stats, year-over-year
4. **Sub-Metering** — kitchen/laundry/HVAC breakdown, correlation
5. **Reports** — tables, statistics, CSV export

## Dataset

`household_power_consumption.csv` — UCI Household Power Consumption
~1 M readings at 1-minute intervals, Dec 2006 – Dec 2008.

Columns: Date, Time, Global_active_power (kW), Global_reactive_power (kW),
Voltage (V), Global_intensity (A), Sub_metering_1/2/3 (Wh).

## Teardown

```bash
docker compose down -v   # -v also removes volumes
```
