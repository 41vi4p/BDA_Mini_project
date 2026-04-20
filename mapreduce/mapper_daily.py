#!/usr/bin/env python3
"""Mapper: Emit (YYYY-MM-DD, energy_kwh+stats) for daily consumption analysis."""
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
        if len(parts) < 3:
            continue

        try:
            date_str = parts[0].strip()
            active = parts[2].strip()

            if active == '?' or not active:
                continue

            date = parse_date(date_str)
            active = float(active)
            energy_kwh = active / 60.0  # 1-minute intervals → kWh

            sys.stdout.write("{}\t{},{},1\n".format(date, active, energy_kwh))
            sys.stdout.flush()
        except (ValueError, IndexError):
            continue


if __name__ == '__main__':
    main()
