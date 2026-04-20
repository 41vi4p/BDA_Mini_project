#!/usr/bin/env python3
"""Mapper: Emit (YYYY-MM-DD, sub1+sub2+sub3+other) for sub-metering breakdown."""
import sys


def parse_date(date_str):
    parts = date_str.strip().split('/')
    return "{}-{}-{}".format(parts[2], parts[1], parts[0])


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line or line.startswith('Date'):
            continue

        parts = line.split(',')
        if len(parts) < 9:
            continue

        try:
            date_str = parts[0].strip()
            active = parts[2].strip()
            sub1 = parts[6].strip()
            sub2 = parts[7].strip()
            sub3 = parts[8].strip()

            if '?' in (active, sub1, sub2, sub3) or not active:
                continue

            date = parse_date(date_str)
            active = float(active)
            sub1 = float(sub1)
            sub2 = float(sub2)
            sub3 = float(sub3)

            total_kwh = active / 60.0
            sub1_kwh = sub1 / 60000.0
            sub2_kwh = sub2 / 60000.0
            sub3_kwh = sub3 / 60000.0
            other_kwh = max(0.0, total_kwh - sub1_kwh - sub2_kwh - sub3_kwh)

            sys.stdout.write("{}\t{:.6f},{:.6f},{:.6f},{:.6f},1\n".format(
                date, sub1_kwh, sub2_kwh, sub3_kwh, other_kwh))
            sys.stdout.flush()
        except (ValueError, IndexError):
            continue


if __name__ == '__main__':
    main()
