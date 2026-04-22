#!/usr/bin/env python3
"""
Quick test pipeline using small.csv
"""
import os
import sys
import time
import subprocess

NAMENODE_HOST = os.environ.get('NAMENODE_HOST', 'localhost')

def log(msg):
    print(f"[TEST] {msg}", flush=True)

def exec_namenode(cmd):
    result = subprocess.run(['docker', 'exec', 'namenode', 'bash', '-c', cmd], 
                           capture_output=True, text=True, timeout=300)
    return result.returncode, result.stdout, result.stderr

log("Setting up HDFS...")
exec_namenode("hdfs dfs -rm -r -f /user/energy")
exec_namenode("hdfs dfs -mkdir -p /user/energy/input /user/energy/output")

log("Uploading small.csv...")
code, out, err = exec_namenode("hdfs dfs -put /data/small.csv /user/energy/input/test.csv")
if code != 0:
    log(f"Upload failed: {err}")
    sys.exit(1)
log("✓ small.csv uploaded")

log("Verifying upload...")
code, out, err = exec_namenode("hdfs dfs -ls /user/energy/input/")
log(out)

STREAMING_JAR = "/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar"

cmd = (
    f"hadoop jar {STREAMING_JAR} "
    f"-files /mapreduce/mapper_hourly.py,/mapreduce/reducer_hourly.py "
    f"-mapper '/usr/bin/python3 mapper_hourly.py' "
    f"-reducer '/usr/bin/python3 reducer_hourly.py' "
    f"-input /user/energy/input/test.csv "
    f"-output /user/energy/output/test_hourly "
    f"-numReduceTasks 1"
)

log("Running MapReduce job...")
log(f"Command: {cmd}")
code, out, err = exec_namenode(cmd)

if code == 0:
    log("✓ MapReduce completed!")
    log("Reading output...")
    code, out, err = exec_namenode("hdfs dfs -cat /user/energy/output/test_hourly/part-*")
    log("Output:")
    log(out[:500])
else:
    log("✗ MapReduce failed!")
    log(err[-1000:])
