#!/usr/bin/env python3
"""Reducer: Aggregate monthly consumption — total_kwh, avg_power, max_power, count."""
import sys


def emit(ym, total_active, total_energy, max_p, count):
    avg_power = total_active / count
    sys.stdout.write("{}\t{:.4f},{:.4f},{:.4f},{}\n".format(
        ym, total_energy, avg_power, max_p, count))
    sys.stdout.flush()


current_ym = None
total_active = 0.0
total_energy = 0.0
max_p = 0.0
count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split('\t')
    if len(parts) != 2:
        continue

    try:
        ym = parts[0]
        vals = parts[1].split(',')
        active = float(vals[0])
        energy = float(vals[1])
        cnt = int(vals[2])
    except (ValueError, IndexError):
        continue

    if ym == current_ym:
        total_active += active
        total_energy += energy
        count += cnt
        if active > max_p:
            max_p = active
    else:
        if current_ym is not None:
            emit(current_ym, total_active, total_energy, max_p, count)
        current_ym = ym
        total_active = active
        total_energy = energy
        count = cnt
        max_p = active

if current_ym is not None:
    emit(current_ym, total_active, total_energy, max_p, count)
