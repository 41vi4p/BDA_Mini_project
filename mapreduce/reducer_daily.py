#!/usr/bin/env python3
"""Reducer: Aggregate daily consumption — total_kwh, avg_power, max_power, min_power, count."""
import sys


def emit(date, total_active, total_energy, max_p, min_p, count):
    avg_power = total_active / count
    sys.stdout.write("{}\t{:.4f},{:.4f},{:.4f},{:.4f},{}\n".format(
        date, total_energy, avg_power, max_p, min_p, count))
    sys.stdout.flush()


current_date = None
total_active = 0.0
total_energy = 0.0
max_p = 0.0
min_p = float('inf')
count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split('\t')
    if len(parts) != 2:
        continue

    try:
        date = parts[0]
        vals = parts[1].split(',')
        active = float(vals[0])
        energy = float(vals[1])
        cnt = int(vals[2])
    except (ValueError, IndexError):
        continue

    if date == current_date:
        total_active += active
        total_energy += energy
        count += cnt
        if active > max_p:
            max_p = active
        if active < min_p:
            min_p = active
    else:
        if current_date is not None:
            emit(current_date, total_active, total_energy, max_p, min_p, count)
        current_date = date
        total_active = active
        total_energy = energy
        count = cnt
        max_p = active
        min_p = active

if current_date is not None:
    emit(current_date, total_active, total_energy, max_p, min_p, count)
