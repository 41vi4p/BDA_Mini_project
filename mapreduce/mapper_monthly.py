#!/usr/bin/env python3
"""Mapper: Emit (YYYY-MM, active_power+energy) for monthly summary analysis."""
import sys


def parse_year_month(date_str):
    parts = date_str.strip().split('/')
    return "{}-{}".format(parts[2], parts[1])


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

            year_month = parse_year_month(date_str)
            active = float(active)
            energy_kwh = active / 60.0

            sys.stdout.write("{}\t{},{},1\n".format(year_month, active, energy_kwh))
            sys.stdout.flush()
        except (ValueError, IndexError):
            continue


if __name__ == '__main__':
    main()
