#!/usr/bin/env python3
"""Mapper: Emit (hour_of_day, metrics) for hourly consumption pattern analysis."""
import sys


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line or line.startswith('Date'):
            continue

        parts = line.split(',')
        if len(parts) < 7:
            continue

        try:
            time_str = parts[1].strip()
            active = parts[2].strip()
            reactive = parts[3].strip()
            voltage = parts[4].strip()
            intensity = parts[5].strip()

            if '?' in (active, reactive, voltage, intensity) or not active:
                continue

            hour = int(time_str.split(':')[0])
            active = float(active)
            reactive = float(reactive)
            voltage = float(voltage)
            intensity = float(intensity)

            sys.stdout.write("{}\t{},{},{},{},1\n".format(
                hour, active, reactive, voltage, intensity))
            sys.stdout.flush()
        except (ValueError, IndexError):
            continue


if __name__ == '__main__':
    main()
