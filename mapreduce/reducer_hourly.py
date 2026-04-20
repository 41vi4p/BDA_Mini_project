#!/usr/bin/env python3
"""Reducer: Aggregate hourly power metrics — avg_active, avg_reactive, avg_voltage, avg_intensity, count, max_active."""
import sys


def emit(hour, totals, count, max_power):
    avg_a = totals[0] / count
    avg_r = totals[1] / count
    avg_v = totals[2] / count
    avg_i = totals[3] / count
    sys.stdout.write("{}\t{:.4f},{:.4f},{:.4f},{:.4f},{},{:.4f}\n".format(
        hour, avg_a, avg_r, avg_v, avg_i, count, max_power))
    sys.stdout.flush()


current_key = None
totals = [0.0, 0.0, 0.0, 0.0]
count = 0
max_power = 0.0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split('\t')
    if len(parts) != 2:
        continue

    try:
        key = int(parts[0])
        vals = parts[1].split(',')
        active = float(vals[0])
        reactive = float(vals[1])
        voltage = float(vals[2])
        intensity = float(vals[3])
        cnt = int(vals[4])
    except (ValueError, IndexError):
        continue

    if key == current_key:
        totals[0] += active
        totals[1] += reactive
        totals[2] += voltage
        totals[3] += intensity
        count += cnt
        if active > max_power:
            max_power = active
    else:
        if current_key is not None:
            emit(current_key, totals, count, max_power)
        current_key = key
        totals = [active, reactive, voltage, intensity]
        count = cnt
        max_power = active

if current_key is not None:
    emit(current_key, totals, count, max_power)
