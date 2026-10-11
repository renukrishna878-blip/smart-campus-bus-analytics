#!/usr/bin/env python3
"""
Distributed Smart Campus Transportation Analytics Using Hadoop
MapReduce Analysis 1: Average Passenger Count Per Route

Reducer: reducer.py
Objective: Receive sorted Route<TAB>Passengers pairs, aggregate total
           passengers and trip counts per route, and emit:
           Route<TAB>Average_Passengers (rounded to 2 decimal places)

Hadoop Streaming compatibility:
- Reads from sys.stdin
- Emits to sys.stdout
"""

import sys


def main():
    """Aggregate passenger counts per route from sorted streaming input."""
    current_route = None
    total_passengers = 0
    record_count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split("\t")
        if len(parts) != 2:
            continue

        route, passenger_str = parts[0].strip(), parts[1].strip()
        try:
            passengers = int(passenger_str)
        except ValueError:
            # Skip non-integer values
            continue

        # If incoming key matches the currently aggregating route
        if current_route == route:
            total_passengers += passengers
            record_count += 1
        else:
            # When key changes, emit the computed average for previous route
            if current_route is not None and record_count > 0:
                avg_passengers = total_passengers / record_count
                print(f"{current_route}\t{avg_passengers:.2f}")

            # Reset aggregators for the new route
            current_route = route
            total_passengers = passengers
            record_count = 1

    # Emit the final aggregated route after EOF
    if current_route is not None and record_count > 0:
        avg_passengers = total_passengers / record_count
        print(f"{current_route}\t{avg_passengers:.2f}")


if __name__ == "__main__":
    main()
