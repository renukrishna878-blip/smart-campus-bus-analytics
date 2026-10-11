# MapReduce Analysis 1: Average Passenger Count Per Route

## 1. Objective
Calculate the average number of passengers carried on each campus bus route (`R1` through `R10`) across the entire 30-day operational log. This analysis identifies high-ridership corridors requiring additional bus frequency or larger fleet allocation.

---

## 2. Input Dataset & Schema
* **Source**: `dataset/bus_data.csv` (10,000 trip records)
* **Schema**:
  `Bus_ID,Route,Timestamp,Passengers,Distance,Travel_Time,Delay,Weather,Traffic_Level`

Relevant columns extracted:
* `Route` (e.g., `R1`, `R4`, `R10`) - used as the Key
* `Passengers` (integer, e.g., `42`) - used as the Value

---

## 3. Mapper Logic (`mapper.py`)
* Reads line-by-line from `sys.stdin` (standard Hadoop Streaming interface).
* Skips the header row (`Bus_ID,Route,...`).
* Parses each CSV record using Python's standard `csv.reader`.
* Extracts `Route` (column index 1) and `Passengers` (column index 3).
* Emits a tab-separated Key-Value pair to `sys.stdout`:
  ```text
  Route<TAB>Passengers
  ```
  *Example Mapper Output*:
  ```text
  R4	11
  R9	7
  R3	13
  ```

---

## 4. Reducer Logic (`reducer.py`)
* Reads sorted `Route<TAB>Passengers` pairs from `sys.stdin`.
* Groups incoming passenger counts by `Route` key.
* Accumulates:
  * `total_passengers += passengers`
  * `record_count += 1`
* When the key transitions (or upon end-of-file), computes:
  ```text
  average_passengers = total_passengers / record_count
  ```
* Emits the route and formatted average (rounded to 2 decimal places) to `sys.stdout`:
  ```text
  Route<TAB>Average_Passengers
  ```

---

## 5. Local Streaming Pipeline Test Command
You can test and demonstrate the complete MapReduce streaming execution in Windows PowerShell:

```powershell
Get-Content smart-campus-transport\dataset\bus_data.csv | python smart-campus-transport\mapreduce\mapper.py | Sort-Object | python smart-campus-transport\mapreduce\reducer.py
```

To redirect and save the output directly into `results/`:
```powershell
Get-Content smart-campus-transport\dataset\bus_data.csv | python smart-campus-transport\mapreduce\mapper.py | Sort-Object | python smart-campus-transport\mapreduce\reducer.py | Set-Content -Path 'smart-campus-transport\results\average_passengers_per_route.txt' -Encoding utf8
```

---

## 6. Expected Output Structure
The output contains one aggregated row per route, sorted lexicographically:

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

## 7. Hadoop Cluster Command (When Running on Apache Hadoop)
When deployed to a live Hadoop cluster via Hadoop Streaming, the corresponding execution command is:

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -input /campus_transport/input/bus_data.csv \
    -output /campus_transport/output/average_passengers \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -file smart-campus-transport/mapreduce/mapper.py \
    -file smart-campus-transport/mapreduce/reducer.py
```
