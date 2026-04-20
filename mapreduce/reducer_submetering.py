#!/usr/bin/env python3
"""Reducer: Aggregate daily sub-metering breakdown — sub1_kwh, sub2_kwh, sub3_kwh, other_kwh."""
import sys


def emit(date, s1, s2, s3, other):
    sys.stdout.write("{}\t{:.4f},{:.4f},{:.4f},{:.4f}\n".format(date, s1, s2, s3, other))
    sys.stdout.flush()


current_date = None
s1 = s2 = s3 = other = 0.0

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
        v1 = float(vals[0])
        v2 = float(vals[1])
        v3 = float(vals[2])
        vother = float(vals[3])
    except (ValueError, IndexError):
        continue

    if date == current_date:
        s1 += v1
        s2 += v2
        s3 += v3
        other += vother
    else:
        if current_date is not None:
            emit(current_date, s1, s2, s3, other)
        current_date = date
        s1, s2, s3, other = v1, v2, v3, vother

if current_date is not None:
    emit(current_date, s1, s2, s3, other)
