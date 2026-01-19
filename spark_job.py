from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import xml.etree.ElementTree as ET


# =============================================================================
# SPARK SESSION INITIALIZATION
# =============================================================================

# Initialize Spark session with optimizations for this ETL workload
spark = SparkSession.builder \
    .appName("DBahn-Berlin-ETL") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.files.maxPartitionBytes", "128MB") \
    .getOrCreate()


# =============================================================================
# SCHEMA DEFINITION
# =============================================================================

# Define the schema for our flattened event data
# We normalize the data by creating one row per event (arrival or departure)
# instead of having separate columns for arrival and departure in the same row
event_schema = StructType([
    # === Identifiers ===
    # Unique identifier for the stop (trip + station + index)
    # Format: <daily_trip_id>-<YYMMddHHmm>-<stop_index>
    StructField("stop_id", StringType(), False),
    
    # Timestamp of the snapshot folder (e.g., '2509021200' = 2025-09-02 12:00)
    StructField("snapshot_timestamp", StringType(), False),
    
    # Type of snapshot: 'timetable' (hourly) or 'change' (15-minute updates)
    StructField("snapshot_type", StringType(), False),
    
    # === Station Information ===
    # Station name as it appears in the XML (e.g., "Berlin Alexanderplatz")
    StructField("station_name", StringType(), True),
    
    # EVA station code - unique identifier for the station (e.g., 8011155)
    StructField("station_eva", LongType(), True),
    
    # === Trip Information ===
    # Trip category (e.g., "ICE", "RE", "RB", "S") - train type
    StructField("trip_category", StringType(), True),
    
    # Trip/train number (e.g., "73768", "3721")
    StructField("trip_number", StringType(), True),
    
    # Trip owner - railway company code (e.g., "OWRE", "800165")
    StructField("trip_owner", StringType(), True),
    
    # Trip type: p=passenger, e=empty, z=special, etc.
    StructField("trip_type", StringType(), True),
    
    # Filter flags for trip classification (e.g., "N" for normal)
    StructField("filter_flags", StringType(), True),
    
    # === Event Type ===
    # Whether this row represents an 'arrival' or 'departure' event
    StructField("event_type", StringType(), False),
    
    # === Event Details ===
    # Planned time in YYMMddHHmm format (e.g., '2509021220' = 2025-09-02 12:20)
    StructField("planned_time", StringType(), True),
    
    # Changed/actual time - only present if different from planned
    StructField("changed_time", StringType(), True),
    
    # Planned platform/track (e.g., "1", "2a")
    StructField("planned_platform", StringType(), True),
    
    # Changed platform - only present if platform was changed
    StructField("changed_platform", StringType(), True),
    
    # Planned status: p=planned, a=added, c=cancelled
    StructField("planned_status", StringType(), True),
    
    # Changed status - updates to the event status
    StructField("changed_status", StringType(), True),
    
    # Planned path - pipe-separated list of stations (e.g., "Berlin Hbf|Potsdam|Brandenburg")
    StructField("planned_path", StringType(), True),
    
    # Changed path - updated route if different from planned
    StructField("changed_path", StringType(), True),
    
    # Line indicator (e.g., "1", "S5", "45S")
    StructField("line", StringType(), True),
    
    # Hidden flag: 1 if event should not be displayed to passengers (0 or None otherwise)
    StructField("hidden", IntegerType(), True),
])

# =============================================================================
# XML PARSING FUNCTIONS
# =============================================================================

def parse_xml_file(file_path, snapshot_timestamp, snapshot_type):
    """
    Parse a single XML file (timetable or change) and extract all events.
    
    This function handles both timetable and change XML formats. The key difference:
    - Timetable XMLs: Full schedule with planned times (pt), platforms (pp), paths (ppth)
    - Change XMLs: Updates only, usually just changed times (ct), may lack trip labels
    
    Each <s> (stop) element can contain:
    - <tl>: Trip label with category, number, owner
    - <ar>: Arrival event with planned/changed times, platforms, paths
    - <dp>: Departure event with planned/changed times, platforms, paths
    
    We create ONE ROW PER EVENT, so a stop with both arrival and departure
    will produce 2 rows in our output.
    
    Args:
        file_path (str): Path to the XML file to parse
        snapshot_timestamp (str): Timestamp from folder name (e.g., '2509021200')
        snapshot_type (str): Either 'timetable' or 'change'
    
    Returns:
        list: List of dictionaries, each representing one event (arrival or departure)
    
    Example XML structure:
        <timetable station="Berlin Alexanderplatz" eva="8011155">
          <s id="3645252224070148387-2509021119-12">
            <tl f="N" t="p" o="OWRE" c="RE" n="73768" />
            <ar pt="2509021220" pp="1" l="1" ppth="Potsdam Hbf|Berlin Hbf|..." />
            <dp pt="2509021221" pp="1" l="1" ppth="Berlin Ostbahnhof|..." />
          </s>
        </timetable>
    """
    rows = []
    
    try:
        # Parse XML file into element tree
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        # Extract station information from the root <timetable> element
        # Station name is always at root level
        station_name = root.get('station', '')
        
        # EVA code can be at root level (common in change XMLs) or stop level
        station_eva = root.get('eva', None)
        
        # Iterate through all <s> (stop) elements
        # Each stop represents one train's visit to this station
        for stop in root.findall('s'):
            # Get the unique stop ID (format: <trip_id>-<date>-<stop_index>)
            stop_id = stop.get('id')
            
            # EVA can also be specified at stop level (overrides root level)
            stop_eva = stop.get('eva', station_eva)
            
            # === Extract Trip Label <tl> ===
            # The trip label contains metadata about the train/trip
            # Note: Change XMLs often DON'T have trip labels, only the stop ID
            tl = stop.find('tl')
            trip_category = None
            trip_number = None
            trip_owner = None
            trip_type = None
            filter_flags = None
            
            if tl is not None:
                trip_category = tl.get('c')    # c = category (e.g., "RE", "ICE")
                trip_number = tl.get('n')      # n = number (e.g., "73768")
                trip_owner = tl.get('o')       # o = owner (railway company)
                trip_type = tl.get('t')        # t = type (p=passenger, e=empty, etc.)
                filter_flags = tl.get('f')     # f = filter flags (e.g., "N" for normal)
            
            # === Process ARRIVAL Event <ar> ===
            # An arrival event represents a train arriving at this station
            ar = stop.find('ar')
            if ar is not None:
                # Create a row for the arrival event
                row = {
                    'stop_id': stop_id,
                    'snapshot_timestamp': snapshot_timestamp,
                    'snapshot_type': snapshot_type,
                    'station_name': station_name,
                    'station_eva': int(stop_eva) if stop_eva else None,
                    
                    # Trip information (may be None for change XMLs)
                    'trip_category': trip_category,
                    'trip_number': trip_number,
                    'trip_owner': trip_owner,
                    'trip_type': trip_type,
                    'filter_flags': filter_flags,
                    
                    # This is an arrival event
                    'event_type': 'arrival',
                    
                    # Event details
                    'planned_time': ar.get('pt'),        # pt = planned time
                    'changed_time': ar.get('ct'),        # ct = changed/actual time
                    'planned_platform': ar.get('pp'),    # pp = planned platform
                    'changed_platform': ar.get('cp'),    # cp = changed platform
                    'planned_status': ar.get('ps'),      # ps = planned status
                    'changed_status': ar.get('cs'),      # cs = changed status (c=cancelled, a=added)
                    'planned_path': ar.get('ppth'),      # ppth = planned path (pipe-separated stations)
                    'changed_path': ar.get('cpth'),      # cpth = changed path
                    'line': ar.get('l'),                 # l = line indicator
                    'hidden': int(ar.get('hi', 0)) if ar.get('hi') else None,  # hi = hidden (1 or 0)
                }
                rows.append(row)
            
            # === Process DEPARTURE Event <dp> ===
            # A departure event represents a train departing from this station
            dp = stop.find('dp')
            if dp is not None:
                # Create a row for the departure event
                # Same structure as arrival, just different event type
                row = {
                    'stop_id': stop_id,
                    'snapshot_timestamp': snapshot_timestamp,
                    'snapshot_type': snapshot_type,
                    'station_name': station_name,
                    'station_eva': int(stop_eva) if stop_eva else None,
                    
                    # Trip information (same as arrival)
                    'trip_category': trip_category,
                    'trip_number': trip_number,
                    'trip_owner': trip_owner,
                    'trip_type': trip_type,
                    'filter_flags': filter_flags,
                    
                    # This is a departure event
                    'event_type': 'departure',
                    
                    # Event details (same attributes as arrival)
                    'planned_time': dp.get('pt'),
                    'changed_time': dp.get('ct'),
                    'planned_platform': dp.get('pp'),
                    'changed_platform': dp.get('cp'),
                    'planned_status': dp.get('ps'),
                    'changed_status': dp.get('cs'),
                    'planned_path': dp.get('ppth'),
                    'changed_path': dp.get('cpth'),
                    'line': dp.get('l'),
                    'hidden': int(dp.get('hi', 0)) if dp.get('hi') else None,
                }
                rows.append(row)
                
    except Exception as e:
        # Log parsing errors but continue processing other files
        print(f"Error parsing {file_path}: {str(e)}")
    
    return rows


def process_all_snapshots(base_path, snapshot_type='timetable'):
    """
    Process all XML files across all snapshot folders.
    
    The data is organized in folders named by timestamp (e.g., '2509021200').
    This function:
    1. Discovers all snapshot folders in the base path
    2. Finds all XML files of the specified type in each folder
    3. Parses each XML file
    4. Aggregates all events into a single list
    
    Args:
        base_path (str): Path to the folder containing snapshot folders
                         e.g., 'timetable_250902_250909' or 'timetable_changes_250902_250909'
        snapshot_type (str): Either 'timetable' or 'change'
                           - 'timetable': Process files ending with '_timetable.xml'
                           - 'change': Process files ending with '_change.xml'
    
    Returns:
        list: List of all parsed events (dictionaries) from all snapshots
    
    Example folder structure:
        timetable_250902_250909/
            2509021200/
                alexanderplatz_timetable.xml
                berlin_hbf_timetable.xml
                ...
            2509021300/
                alexanderplatz_timetable.xml
                ...
    """
    import os
    import glob
    
    all_data = []
    
    # Get all snapshot folders (sorted to process chronologically)
    # Each folder name is a timestamp (e.g., '2509021200' = 2025-09-02 12:00)
    snapshot_folders = sorted([d for d in os.listdir(base_path) 
                               if os.path.isdir(os.path.join(base_path, d))])
    
    print(f"Processing {len(snapshot_folders)} snapshot folders from {base_path}...")
    
    # Process each snapshot folder
    for idx, snapshot_folder in enumerate(snapshot_folders):
        # The folder name IS the timestamp
        snapshot_timestamp = snapshot_folder
        folder_path = os.path.join(base_path, snapshot_folder)
        
        # Build glob pattern based on snapshot type
        # Timetable snapshots: *_timetable.xml
        # Change snapshots: *_change.xml
        if snapshot_type == 'timetable':
            xml_pattern = os.path.join(folder_path, '*_timetable.xml')
        else:  # change
            xml_pattern = os.path.join(folder_path, '*_change.xml')
        
        # Find all matching XML files in this snapshot
        xml_files = glob.glob(xml_pattern)
        
        # Parse each XML file and collect events
        for xml_file in xml_files:
            rows = parse_xml_file(xml_file, snapshot_timestamp, snapshot_type)
            all_data.extend(rows)
        
        # Progress indicator every 10 folders
        if (idx + 1) % 10 == 0:
            print(f"  Processed {idx + 1}/{len(snapshot_folders)} folders, {len(all_data)} events so far...")
    
    print(f"Finished processing. Total events: {len(all_data)}")
    return all_data


def deduplicate_to_latest(data):
    """
    Deduplicate events to keep only the latest snapshot for each stop.
    
    Since we parse both timetable (hourly) and change (15-min) snapshots,
    the same stop_id + event_type will appear multiple times with different
    snapshot_timestamps. This function keeps only the LATEST snapshot for each event.
    
    The deduplication key is: (stop_id, event_type)
    The selection criteria is: MAX(snapshot_timestamp)
    
    For data merging, we prefer:
    - Latest snapshot_timestamp
    - Non-null values from the latest snapshot
    - But we merge in planned values from earlier snapshots if they're missing
    
    Args:
        data (list): List of dictionaries, each representing one event
    
    Returns:
        list: Deduplicated list with one row per (stop_id, event_type)
    
    Example:
        Input: [
            {stop_id: "123", event_type: "arrival", snapshot_timestamp: "2509021200", planned_time: "2509021220", changed_time: None},
            {stop_id: "123", event_type: "arrival", snapshot_timestamp: "2509021215", planned_time: None, changed_time: "2509021225"}
        ]
        Output: [
            {stop_id: "123", event_type: "arrival", snapshot_timestamp: "2509021215", planned_time: "2509021220", changed_time: "2509021225"}
        ]
        # Keeps latest snapshot but merges in planned_time from earlier snapshot
    """
    from collections import defaultdict
    
    # Dictionary to track the latest event for each (stop_id, event_type) pair
    # Key: (stop_id, event_type)
    # Value: dictionary with event data
    latest_events = {}
    
    print(f"Deduplicating {len(data)} events...")
    
    for event in data:
        # Create unique key for this event
        key = (event['stop_id'], event['event_type'])
        
        # If we haven't seen this event before, or this snapshot is newer
        if key not in latest_events:
            # First time seeing this event
            latest_events[key] = event
        else:
            # We've seen this event before - check if current snapshot is newer
            existing = latest_events[key]
            current_timestamp = event['snapshot_timestamp']
            existing_timestamp = existing['snapshot_timestamp']
            
            if current_timestamp > existing_timestamp:
                # Current snapshot is newer - use it as base
                # But merge in any non-null values from the old snapshot that are missing
                merged = event.copy()
                
                # Merge non-null values from older snapshot if missing in newer one
                for field in event.keys():
                    if merged[field] is None and existing[field] is not None:
                        # Keep the old value if new one is null
                        merged[field] = existing[field]
                
                latest_events[key] = merged
            else:
                # Existing snapshot is newer or equal - but merge in non-null values from current
                for field in event.keys():
                    if existing[field] is None and event[field] is not None:
                        existing[field] = event[field]
    
    # Convert dictionary values back to list
    deduplicated_data = list(latest_events.values())
    
    print(f"After deduplication: {len(deduplicated_data)} unique events")
    print(f"Removed {len(data) - len(deduplicated_data)} duplicate snapshots")
    
    return deduplicated_data


def create_spark_dataframe(data, schema):
    """
    Convert parsed Python data into a Spark DataFrame.
    
    This is a simple wrapper that:
    1. Parallelizes the Python list into an RDD
    2. Converts the RDD to a DataFrame with the specified schema
    
    Args:
        data (list): List of dictionaries, each representing one event
        schema (StructType): The schema definition for the DataFrame
    
    Returns:
        DataFrame: Spark DataFrame with the specified schema
    """
    # Create RDD from the Python list
    # spark.sparkContext.parallelize() distributes the data across Spark partitions
    rdd = spark.sparkContext.parallelize(data)
    
    # Convert RDD to DataFrame with type checking and schema enforcement
    df = spark.createDataFrame(rdd, schema=schema)
    
    return df


# =============================================================================
# MAIN ETL PIPELINE
# =============================================================================

def main():
    """
    Main ETL pipeline that orchestrates the entire process:
    1. Parse timetable XML files (hourly snapshots)
    2. Parse change XML files (15-minute update snapshots)
    3. Combine both datasets
    4. Create Spark DataFrame
    5. Add partitioning columns
    6. Write to Parquet format
    7. Verify the output
    
    The output is a partitioned Parquet dataset optimized for analytical queries.
    Partitioning by snapshot_type, date, and hour allows efficient querying of
    specific time ranges without scanning the entire dataset.
    """
    print("=" * 80)
    print("DBahn Berlin ETL Pipeline - Started")
    print("=" * 80)
    
    # === STEP 1: Process Timetable Files ===
    # Timetable files contain the full planned schedule
    # They are captured hourly and contain complete information
    print("\n[1/4] Processing timetable files...")
    timetable_data = []
    
    import os
    
    # Process all extracted timetable directories
    # NOTE: Run extract_archives.py first to extract the tar.gz files
    timetables_dir = 'DBahn-berlin/timetables_extracted'
    if os.path.exists(timetables_dir):
        # Pass the extracted directory directly to process_all_snapshots
        # It will find all snapshot folders (2509021200, 2509021300, etc.) inside
        print(f"Processing timetables from: {timetables_dir}")
        timetable_data = process_all_snapshots(timetables_dir, 'timetable')
    else:
        print(f"Warning: {timetables_dir} not found. Run extract_archives.py first!")
    
    print(f"Total timetable events: {len(timetable_data)}")
    
    # === STEP 2: Process Change Files ===
    # Change files contain updates to the schedule (delays, cancellations, platform changes)
    # They are captured every 15 minutes and may only contain partial information
    print("\n[2/4] Processing change files...")
    change_data = []
    
    timetable_changes_dir = 'DBahn-berlin/timetable_changes_extracted'
    if os.path.exists(timetable_changes_dir):
        # Pass the extracted directory directly to process_all_snapshots
        # It will find all snapshot folders (2509021600, 2509021615, etc.) inside
        print(f"Processing changes from: {timetable_changes_dir}")
        change_data = process_all_snapshots(timetable_changes_dir, 'change')
    else:
        print(f"Warning: {timetable_changes_dir} not found. Run extract_archives.py first!")
    
    print(f"Total change events: {len(change_data)}")
    
    # === STEP 3: Combine Datasets ===
    # Merge both lists into a single unified dataset
    # This gives us both planned schedules and real-time updates in one place
    print("\n[3/5] Combining datasets...")
    all_data = timetable_data + change_data
    print(f"Total combined events (with duplicates): {len(all_data)}")
    
    # === STEP 4: Deduplicate to Latest State ===
    # Keep only the most recent snapshot for each (stop_id, event_type)
    # This ensures we have ONE ROW per actual train event with the latest information
    print("\n[4/5] Deduplicating to latest snapshot per event...")
    deduplicated_data = deduplicate_to_latest(all_data)
    
    # === STEP 5: Create Spark DataFrame ===
    # Convert the Python list into a distributed Spark DataFrame
    # This enables distributed processing and efficient querying
    print("\n[5/5] Creating Spark DataFrame...")
    df = create_spark_dataframe(deduplicated_data, event_schema)
    
    print("\nDataFrame created successfully!")
    print(f"Total rows: {df.count()}")
    
    # Show schema and sample data for verification
    print("\nSchema:")
    df.printSchema()
    
    print("\nSample data:")
    df.show(10, truncate=False)
    
    # === STEP 6: Write to Parquet ===
    # Parquet is a columnar storage format optimized for analytics
    output_path = "output/dbahn_berlin_events_parquet"
    print(f"\n[6/6] Writing to Parquet: {output_path}")
    
    # Add partitioning columns extracted from snapshot_timestamp
    # Partitioning by time allows Spark to skip irrelevant data when querying specific time ranges
    df_with_partitions = df \
        .withColumn("date", substring("snapshot_timestamp", 1, 6)) \
        .withColumn("hour", substring("snapshot_timestamp", 7, 2))
    
    # Write as partitioned Parquet with Snappy compression
    # Partitioning structure: date=250902/hour=12/part-xxxxx.snappy.parquet
    # This creates one folder per date, with subfolders per hour
    df_with_partitions.write \
        .mode("overwrite") \
        .partitionBy("date", "hour") \
        .parquet(output_path, compression="snappy")
    
    print(f"✓ Data written successfully!")
    
    # === STEP 7: Verification ===
    # Read back the Parquet and show statistics
    print("\n" + "=" * 80)
    print("Verification")
    print("=" * 80)
    
    df_read = spark.read.parquet(output_path)
    print(f"Records in Parquet: {df_read.count()}")
    
    # Show distribution of event types
    print("\nEvent type distribution:")
    df_read.groupBy("event_type").count().show()
    
    # Show distribution of snapshot types
    print("\nSnapshot type distribution:")
    df_read.groupBy("snapshot_type").count().show()
    
    # Show busiest stations
    print("\nTop 10 stations by event count:")
    df_read.groupBy("station_name").count() \
        .orderBy(col("count").desc()) \
        .show(10, truncate=False)
    
    print("\n" + "=" * 80)
    print("ETL Pipeline Complete!")
    print("=" * 80)
    
    return df_read


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    df_final = main()

