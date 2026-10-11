#!/usr/bin/env python3
"""
Distributed Smart Campus Transportation Analytics Using Hadoop
MapReduce Analysis 1: Average Passenger Count Per Route

Mapper: mapper.py
Objective: Read campus bus trip records, extract Route and Passengers,
           and emit Key-Value pairs: Route<TAB>Passengers

Hadoop Streaming compatibility:
- Reads from sys.stdin
- Emits to sys.stdout
"""

import sys
import csv


def main():
    """Read lines from standard input and emit Route and Passengers."""
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        # Skip CSV header line
        if line.startswith("Bus_ID,") or line.startswith("Bus_ID"):
            continue

        try:
            # Parse CSV line safely
            row = next(csv.reader([line]))
            if len(row) < 4:
                continue

            # Schema:
            # row[0]: Bus_ID
            # row[1]: Route
            # row[2]: Timestamp
            # row[3]: Passengers
            route = row[1].strip()
            passengers = int(row[3].strip())

            # Emit Key-Value pair separated by TAB
            # Key: Route (e.g., R1), Value: Passengers (e.g., 42)
            print(f"{route}\t{passengers}")

        except (ValueError, IndexError):
            # Gracefully ignore any malformed or corrupted rows
            continue


if __name__ == "__main__":
    main()
