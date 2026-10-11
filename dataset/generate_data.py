#!/usr/bin/env python3
"""
Distributed Smart Campus Transportation Analytics Using Hadoop and MapReduce
Phase 1: Synthetic Dataset Generator

This script generates synthetic but realistic campus bus transportation data
for subsequent processing on Hadoop HDFS using Java MapReduce.

Author: Smart Campus Transportation Analytics Project
Phase: 1 (Dataset Generation & Validation)
"""

import argparse
import csv
from datetime import datetime, timedelta
from pathlib import Path
import random
import sys

# Constant Definitions
EXPECTED_COLUMNS = [
    "Bus_ID",
    "Route",
    "Timestamp",
    "Passengers",
    "Distance",
    "Travel_Time",
    "Delay",
    "Weather",
    "Traffic_Level"
]

BUS_IDS = [f"B{i:02d}" for i in range(1, 21)]  # B01 to B20
ROUTE_IDS = [f"R{i}" for i in range(1, 11)]     # R1 to R10
WEATHER_TYPES = ["Clear", "Cloudy", "Rain"]
TRAFFIC_LEVELS = ["Low", "Medium", "High"]

# Base distance in kilometers for each route (range 5 to 25 km)
ROUTE_BASE_DISTANCES = {
    "R1": 7.5,   # Inner Campus Loop
    "R2": 9.2,   # North Hostels - Main Academic Quad
    "R3": 11.5,  # South Gate - Science & Research Park
    "R4": 13.8,  # Campus Metro Station Connector
    "R5": 16.2,  # Suburban Campus Annex - Tech Block
    "R6": 8.4,   # Central Library - Sports Complex
    "R7": 18.5,  # City Center Express Feeder
    "R8": 12.0,  # Medical Campus - Pharmacy Complex
    "R9": 22.0,  # Railway Junction - University Terminal
    "R10": 10.5  # Faculty Enclave - Innovation Hub
}

# Date boundaries (September 2026, 30 days)
START_DATE = datetime(2026, 9, 1, 0, 0, 0)
END_DATE = datetime(2026, 9, 30, 23, 59, 59)
TOTAL_DAYS = 30

# Hourly weights to ensure the majority of transit activity occurs
# between 06:00 and 22:00, with clear morning (07:00-09:00) and evening (16:00-19:00) peaks.
HOUR_WEIGHTS = {
    0: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 2,       # Night (minimal shuttle runs)
    6: 6,                                      # Early morning buildup
    7: 15, 8: 16,                              # Morning Peak (07:00 - 09:00)
    9: 8, 10: 7, 11: 7,                        # Mid-day regular
    12: 10, 13: 9,                             # Lunchtime travel
    14: 7, 15: 8,                              # Afternoon regular
    16: 15, 17: 16, 18: 15,                    # Evening Peak (16:00 - 19:00)
    19: 8, 20: 7, 21: 5,                       # Night study/return
    22: 3, 23: 2                               # Late evening wind-down
}
HOURS_LIST = list(HOUR_WEIGHTS.keys())
HOURS_WEIGHTS_LIST = [HOUR_WEIGHTS[h] for h in HOURS_LIST]


def is_peak_hour(hour: int) -> bool:
    """Check if the given hour falls in morning or evening peak periods."""
    return (7 <= hour < 9) or (16 <= hour < 19)


def generate_single_record() -> dict:
    """
    Generate a single realistic transportation record applying domain rules:
    - Peak hours induce higher passenger volumes, heavier traffic, and longer delays.
    - Weather (Rain) elevates traffic, travel times, and delays.
    - Each route has realistic base distance with minor operational variations.
    """
    bus_id = random.choice(BUS_IDS)
    route = random.choice(ROUTE_IDS)

    # Date and timestamp generation
    day_offset = random.randint(0, TOTAL_DAYS - 1)
    hour = random.choices(HOURS_LIST, weights=HOURS_WEIGHTS_LIST, k=1)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    timestamp = START_DATE + timedelta(days=day_offset, hours=hour, minutes=minute, seconds=second)

    peak = is_peak_hour(hour)

    # Weather distribution: 65% Clear, 20% Cloudy, 15% Rain
    weather = random.choices(WEATHER_TYPES, weights=[65, 20, 15], k=1)[0]

    # Traffic level depending on peak period and weather conditions
    if peak:
        if weather == "Rain":
            traffic = random.choices(TRAFFIC_LEVELS, weights=[5, 25, 70], k=1)[0]
        else:
            traffic = random.choices(TRAFFIC_LEVELS, weights=[10, 35, 55], k=1)[0]
    elif 6 <= hour < 22:
        if weather == "Rain":
            traffic = random.choices(TRAFFIC_LEVELS, weights=[20, 50, 30], k=1)[0]
        else:
            traffic = random.choices(TRAFFIC_LEVELS, weights=[55, 35, 10], k=1)[0]
    else:
        # Off-peak night hours
        traffic = random.choices(TRAFFIC_LEVELS, weights=[85, 15, 0], k=1)[0]

    # Passenger count (bus capacity approx. 60, min 5)
    if peak:
        passengers = random.randint(35, 58)
        if weather == "Rain":
            passengers += random.randint(1, 4)
    elif 6 <= hour < 22:
        passengers = random.randint(15, 42)
        if weather == "Rain":
            passengers += random.randint(1, 4)
    else:
        passengers = random.randint(5, 18)

    passengers = max(5, min(60, passengers))

    # Route Distance (km)
    base_dist = ROUTE_BASE_DISTANCES[route]
    # Minor variance (+/- 0.6 km) due to campus detours or construction
    distance = round(base_dist + random.uniform(-0.6, 0.6), 1)
    distance = max(5.0, min(25.0, distance))

    # Travel Time (minutes)
    # Average baseline speed: 28 km/h without congestion
    base_minutes = (distance / 28.0) * 60.0

    traffic_multiplier = {
        "Low": random.uniform(0.95, 1.10),
        "Medium": random.uniform(1.25, 1.45),
        "High": random.uniform(1.65, 2.10)
    }[traffic]

    weather_multiplier = {
        "Clear": 1.0,
        "Cloudy": 1.02,
        "Rain": random.uniform(1.10, 1.25)
    }[weather]

    travel_time = int(round(base_minutes * traffic_multiplier * weather_multiplier))
    travel_time = max(8, travel_time)

    # Delay (minutes): realistic range 0 to approx 30 minutes
    # Base delay based on traffic
    if traffic == "Low":
        delay = random.randint(0, 4)
    elif traffic == "Medium":
        delay = random.randint(3, 12)
    else:  # High
        delay = random.randint(10, 22)

    # Delay added by peak congestion
    if peak:
        delay += random.randint(2, 6)

    # Delay added by rain
    if weather == "Rain":
        delay += random.randint(2, 6)

    # Small random variation
    delay += random.randint(-1, 2)
    delay = max(0, min(32, delay))

    return {
        "Bus_ID": bus_id,
        "Route": route,
        "Timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "Passengers": passengers,
        "Distance": distance,
        "Travel_Time": travel_time,
        "Delay": delay,
        "Weather": weather,
        "Traffic_Level": traffic
    }


def generate_dataset(num_records: int, output_path: Path) -> None:
    """Generate num_records, sort chronologically, and write to CSV."""
    print(f"Generating {num_records:,} synthetic campus transportation records...")
    records = []
    for _ in range(num_records):
        records.append(generate_single_record())

    # Sort records chronologically for realistic transit logging
    records.sort(key=lambda r: r["Timestamp"])

    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=EXPECTED_COLUMNS)
        writer.writeheader()
        writer.writerows(records)

    print(f"Data generation complete. Written to: {output_path}")


def validate_and_summarize(csv_path: Path, expected_count: int) -> bool:
    """
    Validate dataset quality and print analytical summary:
    1. Correct number of records.
    2. Correct column names.
    3. No missing values.
    4. No negative numeric values.
    5. Bus IDs are valid.
    6. Routes are valid.
    7. Weather values are valid.
    8. Traffic levels are valid.
    9. Timestamp format is valid.
    """
    print("\nStarting dataset quality validation...")

    if not csv_path.exists():
        print(f"Validation Error: File {csv_path} does not exist!")
        return False

    valid_buses = set(BUS_IDS)
    valid_routes = set(ROUTE_IDS)
    valid_weather = set(WEATHER_TYPES)
    valid_traffic = set(TRAFFIC_LEVELS)

    total_records = 0
    total_passengers = 0
    total_delay = 0
    total_travel_time = 0

    peak_records = 0
    high_traffic_records = 0
    rain_records = 0

    seen_buses = set()
    seen_routes = set()
    min_date = None
    max_date = None

    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # Check column names
        if reader.fieldnames != EXPECTED_COLUMNS:
            print(f"Validation Error: Column names mismatch. Found {reader.fieldnames}, expected {EXPECTED_COLUMNS}")
            return False

        for row_idx, row in enumerate(reader, start=1):
            total_records += 1

            # Check for missing values
            for col in EXPECTED_COLUMNS:
                if row[col] is None or row[col].strip() == "":
                    print(f"Validation Error at row {row_idx}: Missing value in column '{col}'")
                    return False

            # Validate Bus_ID
            bus_id = row["Bus_ID"]
            if bus_id not in valid_buses:
                print(f"Validation Error at row {row_idx}: Invalid Bus_ID '{bus_id}'")
                return False
            seen_buses.add(bus_id)

            # Validate Route
            route = row["Route"]
            if route not in valid_routes:
                print(f"Validation Error at row {row_idx}: Invalid Route '{route}'")
                return False
            seen_routes.add(route)

            # Validate Timestamp
            try:
                dt = datetime.strptime(row["Timestamp"], "%Y-%m-%d %H:%M:%S")
                if min_date is None or dt < min_date:
                    min_date = dt
                if max_date is None or dt > max_date:
                    max_date = dt
            except ValueError:
                print(f"Validation Error at row {row_idx}: Invalid Timestamp format '{row['Timestamp']}'")
                return False

            # Validate numeric values
            try:
                passengers = int(row["Passengers"])
                distance = float(row["Distance"])
                travel_time = int(row["Travel_Time"])
                delay = int(row["Delay"])
            except ValueError as e:
                print(f"Validation Error at row {row_idx}: Numeric conversion failure: {e}")
                return False

            if passengers < 0 or distance < 0.0 or travel_time < 0 or delay < 0:
                print(f"Validation Error at row {row_idx}: Negative values detected!")
                return False

            # Validate Weather
            weather = row["Weather"]
            if weather not in valid_weather:
                print(f"Validation Error at row {row_idx}: Invalid Weather '{weather}'")
                return False

            # Validate Traffic
            traffic = row["Traffic_Level"]
            if traffic not in valid_traffic:
                print(f"Validation Error at row {row_idx}: Invalid Traffic_Level '{traffic}'")
                return False

            # Aggregations for summary
            total_passengers += passengers
            total_travel_time += travel_time
            total_delay += delay

            if is_peak_hour(dt.hour):
                peak_records += 1
            if traffic == "High":
                high_traffic_records += 1
            if weather == "Rain":
                rain_records += 1

    # Validate count
    if total_records != expected_count:
        print(f"Validation Error: Expected {expected_count} records, but found {total_records}")
        return False

    # Summary Report
    avg_passengers = total_passengers / total_records if total_records > 0 else 0
    avg_delay = total_delay / total_records if total_records > 0 else 0
    avg_travel_time = total_travel_time / total_records if total_records > 0 else 0

    print("==================================================")
    print("Dataset generated successfully.")
    print("==================================================")
    print(f"Records: {total_records}")
    print(f"Buses: {len(seen_buses)} ({min(seen_buses)} to {max(seen_buses)})")
    print(f"Routes: {len(seen_routes)} ({', '.join(sorted(seen_routes, key=lambda x: int(x[1:])))})")
    if min_date and max_date:
        print(f"Date range: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
    print()
    print(f"Average passengers: {avg_passengers:.2f}")
    print(f"Average delay: {avg_delay:.2f} mins")
    print(f"Average travel time: {avg_travel_time:.2f} mins")
    print()
    print(f"Peak-hour records: {peak_records} ({peak_records / total_records * 100:.1f}%)")
    print(f"High-traffic records: {high_traffic_records} ({high_traffic_records / total_records * 100:.1f}%)")
    print(f"Rain records: {rain_records} ({rain_records / total_records * 100:.1f}%)")
    print()
    print(f"Output:\n{csv_path}")
    print("==================================================")
    print("All 9 validation checks passed successfully!")
    print("==================================================")
    return True


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    default_output = Path(__file__).resolve().parent / "bus_data.csv"

    parser = argparse.ArgumentParser(
        description="Generate synthetic campus bus transportation dataset."
    )
    parser.add_argument(
        "records",
        nargs="?",
        type=int,
        default=100000,
        help="Number of records to generate (default: 100000)"
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=str(default_output),
        help=f"Target output CSV file path (default: {default_output})"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)"
    )

    args = parser.parse_args()

    if args.records <= 0:
        parser.error("Record count must be a positive integer.")

    return args


def main():
    args = parse_arguments()
    if args.seed is not None:
        random.seed(args.seed)

    output_path = Path(args.output).resolve()
    generate_dataset(num_records=args.records, output_path=output_path)
    success = validate_and_summarize(csv_path=output_path, expected_count=args.records)

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
