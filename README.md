# TU Berlin DAMS Lab Datasets Submission

# Task 1, 2 & 4 Instructions

Instructions and Solutions of this task are one folder below in ``DBahn-berlin``

# Task 3 Instructions

## Setup

### 1. Extract Dataset Archives
**Option A: Extract in container (Recommended if you use local mode, it makes things faster)**
```bash
# Start the Docker containers
docker compose up -d --scale spark-worker=X # Scale number of workers based on hardware

# Create directories for extraction
docker exec spark-master mkdir -p /tmp/data/timetables_extracted
docker exec spark-master mkdir -p /tmp/data/timetable_changes_extracted

# Extract timetables
docker exec spark-master bash -c 'for file in /opt/spark-apps/DBahn-berlin/timetables/*.tar.gz; do tar -xzf "$file" -C /tmp/data/timetables_extracted; done'

# Extract ALL timetable changes (loop through all archives)
docker exec spark-master bash -c 'for file in /opt/spark-apps/DBahn-berlin/timetable_changes/*.tar.gz; do tar -xzf "$file" -C /tmp/data/timetable_changes_extracted; done'
```

**Option B: Extract locally into DBahn-berlin directory (Necessary if you want to use cluster mode)**
```bash
# Create extraction directories
mkdir -p DBahn-berlin/timetables_extracted
mkdir -p DBahn-berlin/timetable_changes_extracted

# Extract timetables locally
tar -xzf DBahn-berlin/timetables/berlin_timetables_250902_250909.tar.gz -C DBahn-berlin/timetables_extracted

# Extract timetable changes locally
tar -xzf DBahn-berlin/timetable_changes/berlin_timetable_changes_250902_250909.tar.gz -C DBahn-berlin/timetable_changes_extracted
```

### 2. Adjust Path files
Before running, edit `3_1_pipeline.py` to set the correct paths based on your extraction method:

**If you used Option A (Extract in container):**
```python
TIMETABLES_PATH='/tmp/data/timetables_extracted'
CHANGES_PATH='/tmp/data/timetable_changes_extracted'
```

**If you used Option B (Extract locally):**
```python
TIMETABLES_PATH='/opt/spark-apps/DBahn-berlin/timetables_extracted'
CHANGES_PATH='/opt/spark-apps/DBahn-berlin/timetable_changes_extracted'
```

## Run

### Task 3.1: ETL Pipeline
- Adjust memory on demand
**Local Mode:**
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --driver-memory 6g \
  --packages com.databricks:spark-xml_2.12:0.18.0 \
  --conf spark.driver.maxResultSize=2g \
  /opt/spark-apps/task_3/3_1_pipeline.py
```

**Cluster Mode:**
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-memory 2g \
  --packages com.databricks:spark-xml_2.12:0.18.0 \
  --executor-memory 3g \
  --executor-cores 2 \
  --num-executors 2 \
  --conf spark.driver.maxResultSize=2g \
  /opt/spark-apps/task_3/3_1_pipeline.py
```

**Expected Output:**
- Parquet files written to `/opt/spark-apps/output/dbahn_berlin_spark_etl`
- Partitioned by snapshot_timestamp

### Task 3.2: Average Daily Delay
- Adjust memory on demand
**Local Mode:**
```bash
docker exec spark-master /opt/spark/bin/spark-submit \ 
  --packages com.databricks:spark-xml_2.12:0.18.0 \
  --driver-memory 6g \
  /opt/spark-apps/task_3/3_2_average_delays.py
```

**Cluster Mode:**
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --packages com.databricks:spark-xml_2.12:0.18.0 \
  --master spark://spark-master:7077 \
  --driver-memory 2g \
  --executor-memory 4g \
  /opt/spark-apps/task_3/3_2_average_delays.py
```

**Query a different station:** 
Edit `3_2_average_delays.py`:
```python
station_name = "Alexanderplatz" # change station name here
```

### Task 3.3: Execute: Rush Hour Departures
- Adjust memory on demand
- Will give out all station names

**Local Mode**
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --packages com.databricks:spark-xml_2.12:0.18.0 \
  --driver-memory 6g \
  /opt/spark-apps/task_3/3_3_rush_hour_departures.py
```

**Cluster Mode**
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --packages com.databricks:spark-xml_2.12:0.18.0 \
  --master spark://spark-master:7077 \
  --driver-memory 2g \
  --executor-memory 4g \
  /opt/spark-apps/task_3/3_3_rush_hour_departures.py
```