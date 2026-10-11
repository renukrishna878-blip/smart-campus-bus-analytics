# Distributed Smart Campus Transportation Analytics Using Hadoop and MapReduce

## 1. Project Title
**Distributed Smart Campus Transportation Analytics Using Hadoop and MapReduce**

---

## 2. Project Objective
Universities operate dedicated bus transit networks shuttling thousands of students, faculty, and administrative staff across sprawling academic zones, hostel clusters, metro hubs, and research centers. Managing and analyzing transit delays, passenger bottlenecks, route efficiencies, and weather-induced disruptions becomes computationally demanding as data scales across fleets over full semesters.

The objective of this project is to develop a distributed transportation analytics pipeline using **Apache Hadoop** and **MapReduce** to process large-scale campus transit logs. The system will evaluate:
* Peak-hour ridership and passenger congestion patterns.
* Route-level delays and bottlenecks under varying traffic conditions.
* Weather impact (e.g., rain) on bus travel times and schedule adherence.
* Fleet utilization across different campus transit corridors.

---

## 3. Phase 1 Objective
**Phase 1** focuses on laying the foundational project architecture and building a standalone, synthetic campus transportation dataset generator in Python.

Key objectives of Phase 1:
* Establish a modular, academic project directory structure.
* Implement a realistic, domain-aware synthetic dataset generator using Python's standard library.
* Enforce realistic transit correlations (peak-hour ridership spikes, rain-induced slowdowns, route-specific distances, and traffic delays).
* Provide automatic validation verifying data integrity, non-negativity, schema compliance, and statistical distributions.
* Prepare dataset files ready for future ingestion into the Hadoop Distributed File System (HDFS).

> **Important Note**: Neither Apache Hadoop nor Java MapReduce jobs have been configured or executed in Phase 1. Hadoop integration, HDFS ingestion, and MapReduce analytical jobs will be developed in subsequent phases.

---

## 4. Project Directory Structure
```text
smart-campus-transport/
│
├── dataset/
│   ├── generate_data.py        # Python synthetic dataset generator and validator
│   └── bus_data.csv            # Generated transportation records (CSV)
│
├── hadoop/
│   ├── input/                  # Local staging area for HDFS input datasets
│   └── output/                 # Local staging area for HDFS MapReduce outputs
│
├── mapreduce/                  # Java MapReduce source code (to be built in later phase)
│
├── backend/                    # Analytical service / API layer (to be built in later phase)
│
├── frontend/                   # Dashboard / visualization interface (to be built in later phase)
│
├── results/                    # Aggregated analytics and report artifacts
│
├── docs/                       # Project documentation, architecture diagrams, viva notes
│
└── README.md                   # Project documentation and phase overview
```

---

## 5. Dataset Fields & Schema

The dataset represents trip logs recorded by campus shuttle transit buses. Each row represents a completed trip segment:

| Field Name | Data Type | Example Value | Description |
| :--- | :--- | :--- | :--- |
| `Bus_ID` | String | `B01` | Unique campus shuttle identifier (`B01` through `B20`). |
| `Route` | String | `R1` | Specific campus transit route (`R1` through `R10`). |
| `Timestamp` | String (`YYYY-MM-DD HH:MM:SS`) | `2026-09-01 07:30:00` | Date and time when the trip was logged. |
| `Passengers` | Integer | `42` | Passenger count recorded on the bus (5 to 60 capacity). |
| `Distance` | Float | `12.5` | Route trip distance in kilometers (5.0 to 25.0 km). |
| `Travel_Time` | Integer | `35` | Total trip duration in minutes. |
| `Delay` | Integer | `5` | Minutes delayed past the scheduled timetable (0 to ~30 mins). |
| `Weather` | String | `Clear` | Ambient weather condition (`Clear`, `Cloudy`, `Rain`). |
| `Traffic_Level` | String | `Medium` | Campus road congestion level (`Low`, `Medium`, `High`). |

---

## 6. Dataset Generation & Realism Rules

The generator script (`dataset/generate_data.py`) does not generate arbitrary numbers; it models real-world transportation physics and campus behaviors:

1. **Peak Periods**:
   * Morning peak: `07:00` to `09:00`
   * Evening peak: `16:00` to `19:00`
   * Peak hours trigger higher passenger load (35–60 riders), heavier traffic (higher probability of `High` traffic), and elevated delays.
2. **Traffic & Travel Time Coupling**:
   * Higher traffic increases travel time and delay proportionally.
   * `Low` traffic produces low delays (0–4 minutes).
3. **Weather Effects**:
   * `Rain` introduces road slowdowns (+10% to +25% travel time), increased traffic congestion, and added trip delay.
4. **Route Specificity**:
   * Each of the 10 routes (`R1` to `R10`) has distinct characteristic base distances (ranging between 7.5 km and 22.0 km) with minor operational trip variances.
5. **Operational Hours & Date Span**:
   * Spans 30 full days (`2026-09-01` to `2026-09-30`).
   * Over 90% of transport activity is concentrated between active hours (`06:00` to `22:00`), with minimal late-night shuttle runs.
6. **Integrity & Sanity Checks**:
   * Zero negative values for all metrics (`Passengers >= 5`, `Distance >= 5.0`, `Travel_Time >= 8`, `Delay >= 0`).
   * All records are automatically sorted chronologically.

---

## 7. How to Run the Dataset Generator

The generator script uses only Python standard library modules (`csv`, `random`, `datetime`, `argparse`, `pathlib`, `sys`). No third-party packages or external dependencies (such as pandas) are required.

### Execution from Project Root
```bash
# Navigate to the project root
cd smart-campus-transport

# Generate default 100,000 records
python dataset/generate_data.py

# Generate 10,000 records
python dataset/generate_data.py 10000

# Generate 1,000,000 records (for large-scale Hadoop stress testing)
python dataset/generate_data.py 1000000
```

### Execution from Dataset Directory
```bash
cd dataset
python generate_data.py 10000
```

### Configurable Command-Line Options
* Positional argument: `records` (Integer, default: `100000`).
* `--output`, `-o`: Custom CSV output path (default: `dataset/bus_data.csv`).
* `--seed`: Random seed for reproducible dataset generation (default: `42`).

---

## 8. Expected Output & Automatic Validation

After generating the dataset, the script performs **9 automated validation tests**:
1. Record count matches the requested argument exactly.
2. Column names match the required schema exactly.
3. No missing or null values in any column.
4. No negative values in numeric columns (`Passengers`, `Distance`, `Travel_Time`, `Delay`).
5. All `Bus_ID` values belong to the valid fleet set (`B01` - `B20`).
6. All `Route` values belong to valid routes (`R1` - `R10`).
7. All `Weather` values belong to (`Clear`, `Cloudy`, `Rain`).
8. All `Traffic_Level` values belong to (`Low`, `Medium`, `High`).
9. Timestamps conform to standard format (`YYYY-MM-DD HH:MM:SS`).

### Sample Terminal Output
```text
Generating 10,000 synthetic campus transportation records...
Data generation complete. Written to: .../smart-campus-transport/dataset/bus_data.csv

Starting dataset quality validation...
==================================================
Dataset generated successfully.
==================================================
Records: 10000
Buses: 20 (B01 to B20)
Routes: 10 (R1, R2, R3, R4, R5, R6, R7, R8, R9, R10)
Date range: 2026-09-01 to 2026-09-30

Average passengers: 35.63
Average delay: 11.15 mins
Average travel time: 40.26 mins

Peak-hour records: 4495 (45.0%)
High-traffic records: 3160 (31.6%)
Rain records: 1457 (14.6%)

Output:
.../smart-campus-transport/dataset/bus_data.csv
==================================================
All 9 validation checks passed successfully!
==================================================
```

---

## 9. Phase 2: First MapReduce Analysis (Average Passengers Per Route)

### Status: Completed (Local Hadoop Streaming Pipeline Simulation)
* **Mapper**: [`mapreduce/mapper.py`](mapreduce/mapper.py)
* **Reducer**: [`mapreduce/reducer.py`](mapreduce/reducer.py)
* **Output Artifact**: [`results/average_passengers_per_route.txt`](results/average_passengers_per_route.txt)

#### MapReduce Architecture
```text
dataset/bus_data.csv (10,000 records)
        │
        ▼ (stdin)
  mapper.py (extracts Route & Passengers, emits Route<TAB>Passengers)
        │
        ▼
  Sort / Shuffle (Sort-Object / MapReduce framework groups by Route key)
        │
        ▼ (stdin)
  reducer.py (aggregates sum(passengers) / count, emits Route<TAB>Avg)
        │
        ▼
  results/average_passengers_per_route.txt (10 routes calculated)
```

#### Local Execution Command
```powershell
Get-Content smart-campus-transport\dataset\bus_data.csv | python smart-campus-transport\mapreduce\mapper.py | Sort-Object | python smart-campus-transport\mapreduce\reducer.py
```

#### Verified Output (All 10 Routes)
```text
R1	35.91
R10	35.37
R2	35.44
R3	35.55
R4	35.81
R5	35.66
R6	36.12
R7	35.37
R8	35.43
R9	35.65
```

---

## 10. Roadmap: What Will Be Implemented in Later Phases

* **Phase 2B (Hadoop Cluster Deployment)**:
  * Optional deployment of existing `mapper.py` and `reducer.py` to live Hadoop cluster using `hadoop-streaming.jar` and HDFS once Hadoop runtime is chosen.
* **Phase 3: Additional MapReduce Analytics**:
  * Delay and traffic bottleneck analysis by route and weather conditions.
  * Bus fleet utilization and operational efficiency.
* **Phase 4: Results Extraction & Dashboard Preparation**:
  * Aggregating analytical text outputs into structured reports.
* **Phase 5: Lightweight Analytics Visualization**:
  * Visual representation of campus route transit metrics.

