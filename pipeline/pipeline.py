#!/usr/bin/env python3
"""
Energy Analytics Pipeline
Uploads CSV to HDFS, runs MapReduce jobs via Hadoop Streaming,
reads results, and stores aggregated data in MongoDB.
"""
import os
import sys
import time
import requests
import pymongo
import docker

NAMENODE_HOST = os.environ.get('NAMENODE_HOST', 'namenode')
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://mongodb:27017/')
MONGO_DB = os.environ.get('MONGO_DB', 'energy_db')

STREAMING_JAR = "/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"
HDFS_INPUT = "/user/energy/input/household_power_consumption.csv"
LOCAL_CSV = "/data/household_power_consumption.csv"


def log(msg):
    print(f"[PIPELINE] {msg}", flush=True)


# ── Readiness checks ──────────────────────────────────────────────────────────

def wait_for_hdfs(retries=90, delay=10):
    log("Waiting for HDFS NameNode...")
    for i in range(retries):
        try:
            r = requests.get(
                f"http://{NAMENODE_HOST}:9870/webhdfs/v1/?op=LISTSTATUS",
                timeout=5
            )
            if r.status_code == 200:
                log("HDFS is ready.")
                return True
        except Exception:
            pass
        log(f"  HDFS not ready, retry {i+1}/{retries}...")
        time.sleep(delay)
    return False


def wait_for_yarn(retries=60, delay=10):
    log("Waiting for YARN ResourceManager...")
    for i in range(retries):
        try:
            r = requests.get(
                "http://resourcemanager:8088/ws/v1/cluster/info",
                timeout=5
            )
            if r.status_code == 200:
                log("YARN is ready.")
                return True
        except Exception:
            pass
        log(f"  YARN not ready, retry {i+1}/{retries}...")
        time.sleep(delay)
    return False


def wait_for_mongo(retries=30, delay=5):
    log("Waiting for MongoDB...")
    for i in range(retries):
        try:
            client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
            client.server_info()
            log("MongoDB is ready.")
            return True
        except Exception:
            pass
        log(f"  MongoDB not ready, retry {i+1}/{retries}...")
        time.sleep(delay)
    return False


# ── Docker exec helpers ───────────────────────────────────────────────────────

def exec_namenode(docker_client, cmd):
    container = docker_client.containers.get('namenode')
    result = container.exec_run(['bash', '-c', cmd], demux=True)
    stdout = result.output[0].decode('utf-8', errors='replace') if result.output[0] else ''
    stderr = result.output[1].decode('utf-8', errors='replace') if result.output[1] else ''
    return result.exit_code, stdout, stderr


# ── HDFS setup ────────────────────────────────────────────────────────────────

def setup_hdfs(dc):
    log("Creating HDFS directories...")
    exec_namenode(dc, "hdfs dfs -mkdir -p /user/energy/input /user/energy/output")


def upload_csv(dc):
    code, out, _ = exec_namenode(dc, f"hdfs dfs -test -e {HDFS_INPUT} && echo EXISTS")
    if 'EXISTS' in out:
        log("CSV already in HDFS, skipping upload.")
        return

    log("Uploading CSV to HDFS (48 MB — may take a minute)...")
    code, out, err = exec_namenode(dc, f"hdfs dfs -put {LOCAL_CSV} {HDFS_INPUT}")
    if code != 0:
        log(f"Upload failed:\n{err}")
        raise RuntimeError("HDFS upload failed")
    log("CSV uploaded successfully.")


# ── MapReduce jobs ────────────────────────────────────────────────────────────

def run_job(dc, name, mapper, reducer, output_dir):
    out_path = f"/user/energy/output/{output_dir}"
    exec_namenode(dc, f"hdfs dfs -rm -r -f {out_path}")

    cmd = (
        f"hadoop jar {STREAMING_JAR} "
        f"-files /mapreduce/{mapper},/mapreduce/{reducer} "
        f"-mapper '/usr/bin/python3 {mapper}' "
        f"-reducer '/usr/bin/python3 {reducer}' "
        f"-input {HDFS_INPUT} "
        f"-output {out_path} "
        f"-numReduceTasks 4"
    )
    log(f"Running MapReduce job: {name} ...")
    code, out, err = exec_namenode(dc, cmd)
    if code == 0:
        log(f"  {name} — DONE")
        return True
    log(f"  {name} — FAILED\n{err[-3000:]}")
    return False


def read_output(dc, output_dir):
    code, out, err = exec_namenode(dc, f"hdfs dfs -cat /user/energy/output/{output_dir}/part-*")
    if code == 0:
        return out
    log(f"  Could not read {output_dir}: {err}")
    return ""


# ── Parsers ───────────────────────────────────────────────────────────────────

def parse_hourly(text):
    records = []
    for line in text.splitlines():
        p = line.split('\t')
        if len(p) != 2:
            continue
        try:
            hour = int(p[0])
            v = p[1].split(',')
            records.append({
                'hour': hour,
                'avg_active_power': float(v[0]),
                'avg_reactive_power': float(v[1]),
                'avg_voltage': float(v[2]),
                'avg_intensity': float(v[3]),
                'count': int(v[4]),
                'max_active_power': float(v[5]),
            })
        except (ValueError, IndexError):
            continue
    return records


def parse_daily(text):
    records = []
    for line in text.splitlines():
        p = line.split('\t')
        if len(p) != 2:
            continue
        try:
            v = p[1].split(',')
            records.append({
                'date': p[0],
                'total_kwh': float(v[0]),
                'avg_power': float(v[1]),
                'max_power': float(v[2]),
                'min_power': float(v[3]),
                'count': int(v[4]),
            })
        except (ValueError, IndexError):
            continue
    return records


def parse_monthly(text):
    records = []
    for line in text.splitlines():
        p = line.split('\t')
        if len(p) != 2:
            continue
        try:
            v = p[1].split(',')
            records.append({
                'year_month': p[0],
                'total_kwh': float(v[0]),
                'avg_power': float(v[1]),
                'max_power': float(v[2]),
                'count': int(v[3]),
            })
        except (ValueError, IndexError):
            continue
    return records


def parse_submetering(text):
    records = []
    for line in text.splitlines():
        p = line.split('\t')
        if len(p) != 2:
            continue
        try:
            v = p[1].split(',')
            records.append({
                'date': p[0],
                'sub1_kwh': float(v[0]),
                'sub2_kwh': float(v[1]),
                'sub3_kwh': float(v[2]),
                'other_kwh': float(v[3]),
            })
        except (ValueError, IndexError):
            continue
    return records


# ── MongoDB storage ───────────────────────────────────────────────────────────

def store(db, collection, records):
    col = db[collection]
    col.drop()
    if records:
        col.insert_many(records)
        log(f"  Stored {len(records)} records → {collection}")
    else:
        log(f"  No records for {collection}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log("=" * 55)
    log("  Real-time Energy Consumption Analysis Pipeline")
    log("=" * 55)

    if not wait_for_hdfs():
        sys.exit("HDFS never became available.")
    if not wait_for_mongo():
        sys.exit("MongoDB never became available.")

    mongo_client = pymongo.MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB]

    if db['pipeline_status'].find_one({'status': 'completed'}):
        log("Pipeline already completed. Nothing to do.")
        return

    dc = docker.from_env()

    db['pipeline_status'].delete_many({})
    db['pipeline_status'].insert_one({'status': 'running', 'started_at': time.time()})

    try:
        setup_hdfs(dc)
        upload_csv(dc)

        if not wait_for_yarn():
            sys.exit("YARN never became available.")

        jobs = [
            ('Hourly Patterns',   'mapper_hourly.py',      'reducer_hourly.py',      'hourly'),
            ('Daily Consumption', 'mapper_daily.py',        'reducer_daily.py',        'daily'),
            ('Monthly Summary',   'mapper_monthly.py',      'reducer_monthly.py',      'monthly'),
            ('Sub-metering',      'mapper_submetering.py',  'reducer_submetering.py',  'submetering'),
        ]

        parsers = {
            'hourly':      (parse_hourly,      'hourly_patterns'),
            'daily':       (parse_daily,       'daily_consumption'),
            'monthly':     (parse_monthly,     'monthly_summary'),
            'submetering': (parse_submetering, 'submetering_daily'),
        }

        for name, mapper, reducer, out_dir in jobs:
            ok = run_job(dc, name, mapper, reducer, out_dir)
            if ok:
                raw = read_output(dc, out_dir)
                parse_fn, col_name = parsers[out_dir]
                records = parse_fn(raw)
                store(db, col_name, records)
            else:
                log(f"Skipping MongoDB store for {name} due to job failure.")

        db['pipeline_status'].update_one(
            {'status': 'running'},
            {'$set': {'status': 'completed', 'completed_at': time.time()}}
        )
        log("\n✓ Pipeline completed successfully!")

    except Exception as exc:
        db['pipeline_status'].update_one(
            {'status': 'running'},
            {'$set': {'status': 'failed', 'error': str(exc)}}
        )
        log(f"Pipeline failed: {exc}")
        raise


if __name__ == '__main__':
    main()
