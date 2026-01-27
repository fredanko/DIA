from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, max as spark_max, broadcast
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    IntegerType, ArrayType
)
import xml.etree.ElementTree as ET
import os
import sys


# Windows-specific Spark configuration
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def create_spark_session():
    """Create and configure Spark session with optimized settings for ETL workload."""
    spark = SparkSession.builder \
        .appName("DBahn-Berlin-ETL-Pipeline") \
        .master("local[4]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .config("spark.sql.shuffle.partitions", "50") \
        .config("spark.driver.memory", "6g") \
        .getOrCreate()
    
    # Set log level to reduce verbose output
    spark.sparkContext.setLogLevel("WARN")
    return spark


# Schema for parsed events (arrival/departure)
EVENT_SCHEMA = StructType([
    StructField("stop_id", StringType(), False),
    StructField("snapshot_timestamp", StringType(), False),
    StructField("snapshot_type", StringType(), False),
    StructField("station_name", StringType(), True),
    StructField("station_eva", LongType(), True),
    StructField("trip_category", StringType(), True),
    StructField("trip_number", StringType(), True),
    StructField("trip_owner", StringType(), True),
    StructField("trip_type", StringType(), True),
    StructField("filter_flags", StringType(), True),
    StructField("event_type", StringType(), False),
    StructField("planned_time", StringType(), True),
    StructField("changed_time", StringType(), True),
    StructField("planned_platform", StringType(), True),
    StructField("changed_platform", StringType(), True),
    StructField("planned_status", StringType(), True),
    StructField("changed_status", StringType(), True),
    StructField("planned_path", StringType(), True),
    StructField("changed_path", StringType(), True),
    StructField("line", StringType(), True),
    StructField("hidden", IntegerType(), True),
])

def parse_xml_content(xml_content, snapshot_timestamp, snapshot_type):
    """
    Parse XML timetable content and extract arrival/departure events.
    Runs in parallel across Spark workers.
    
    Returns list of tuples with event data.
    """
    events = []
    
    try:
        # Parse XML string into an element tree
        root = ET.fromstring(xml_content)
        
        # Extract station info from root <timetable> element
        station_name = root.get('station', '')
        station_eva = root.get('eva')
        
        # Process each <s> (stop) element
        for stop in root.findall('s'):
            stop_id = stop.get('id')
            stop_eva = stop.get('eva', station_eva)
            
            # Extract trip label <tl> if present
            tl = stop.find('tl')
            trip_category = tl.get('c') if tl is not None else None
            trip_number = tl.get('n') if tl is not None else None
            trip_owner = tl.get('o') if tl is not None else None
            trip_type = tl.get('t') if tl is not None else None
            filter_flags = tl.get('f') if tl is not None else None
            
            # Process arrival event <ar>
            ar = stop.find('ar')
            if ar is not None:
                events.append((
                    stop_id,
                    snapshot_timestamp,
                    snapshot_type,
                    station_name,
                    int(stop_eva) if stop_eva else None,
                    trip_category,
                    trip_number,
                    trip_owner,
                    trip_type,
                    filter_flags,
                    'arrival',
                    ar.get('pt'),
                    ar.get('ct'),
                    ar.get('pp'),
                    ar.get('cp'),
                    ar.get('ps'),
                    ar.get('cs'),
                    ar.get('ppth'),
                    ar.get('cpth'),
                    ar.get('l'),
                    int(ar.get('hi', 0)) if ar.get('hi') else None,
                ))
            
            dp = stop.find('dp')
            if dp is not None:
                events.append((
                    stop_id,
                    snapshot_timestamp,
                    snapshot_type,
                    station_name,
                    int(stop_eva) if stop_eva else None,
                    trip_category,
                    trip_number,
                    trip_owner,
                    trip_type,
                    filter_flags,
                    'departure',
                    dp.get('pt'),
                    dp.get('ct'),
                    dp.get('pp'),
                    dp.get('cp'),
                    dp.get('ps'),
                    dp.get('cs'),
                    dp.get('ppth'),
                    dp.get('cpth'),
                    dp.get('l'),
                    int(dp.get('hi', 0)) if dp.get('hi') else None,
                ))
                
    except ET.ParseError as e:
        print(f"XML Parse Error: {e}")
        return []
    except Exception as e:
        print(f"Error parsing XML: {e}")
        return []
    
    return events

def extract_xml_files(spark, base_path, snapshot_type):

    """
    Read XML files using Spark's binaryFile reader with recursive lookup.
    Returns DataFrame with file paths and binary content.
    """
    file_pattern = "*_timetable.xml" if snapshot_type == 'timetable' else "*_change.xml"
    
    print(f"Reading files from {base_path} using binaryFile reader...")
    
    raw_df = spark.read.format("binaryFile") \
        .option("pathGlobFilter", file_pattern) \
        .option("recursiveFileLookup", "true") \
        .load(base_path)
        
    print(f"Binary DataFrame created.")
    return raw_df

def transform_to_dataframe(spark, raw_df, snapshot_type):
    """Transform binary DataFrame to parsed event DataFrame."""
    
    balanced_df = raw_df
    
    def process_row(row):
        filepath = row.path
        binary_content = row.content
        
        try:
            xml_string = binary_content.decode('utf-8')
        except:
            print(f"Error decoding file: {filepath}")
            return []

        # Extract timestamp from path (e.g., .../2509021200/file.xml)
        path_parts = filepath.replace('\\', '/').split('/')
        snapshot_timestamp = None
        for part in reversed(path_parts):
            if len(part) == 10 and part.isdigit():
                snapshot_timestamp = part
                break
        if not snapshot_timestamp:
            snapshot_timestamp = 'unknown'

        return parse_xml_content(xml_string, snapshot_timestamp, snapshot_type)

    print("Parsing XML content in parallel...")
    events_rdd = balanced_df.rdd.flatMap(process_row)
    df = spark.createDataFrame(events_rdd, schema=EVENT_SCHEMA)
    
    return df

def deduplicate_and_merge(timetable_df, change_df):
    """
    Deduplicate changes (keep latest) and broadcast join with timetables.
    Returns merged DataFrame with planned data from timetables and changed data from updates.
    """
    print("Deduplicating changes and merging with cached timetables...")
    
    print("  Deduplicating changes...")
    latest_changes = change_df \
        .orderBy(col("snapshot_timestamp").desc()) \
        .dropDuplicates(["stop_id", "event_type"])
    
    print("  Broadcast joining changes to timetables...")
    merged = timetable_df.alias("t").join(
        broadcast(latest_changes).alias("c"),
        (col("t.stop_id") == col("c.stop_id")) & 
        (col("t.event_type") == col("c.event_type")),
        "left"  # Keep ALL timetables, add changes if they exist
    ).select(
        col("t.stop_id"),
        col("t.event_type"),
        when(col("c.snapshot_timestamp").isNotNull(), col("c.snapshot_timestamp"))
            .otherwise(col("t.snapshot_timestamp")).alias("snapshot_timestamp"),
        when(col("c.snapshot_type").isNotNull(), col("c.snapshot_type"))
            .otherwise(col("t.snapshot_type")).alias("snapshot_type"),
        col("t.station_name"),
        col("t.station_eva"),
        col("t.trip_category"),
        col("t.trip_number"),
        col("t.trip_owner"),
        col("t.trip_type"),
        col("t.filter_flags"),
        col("t.planned_time"),
        col("t.planned_platform"),
        col("t.planned_status"),
        col("t.planned_path"),
        col("c.changed_time"),
        col("c.changed_platform"),
        col("c.changed_status"),
        col("c.changed_path"),
        col("t.line"),
        col("t.hidden")
    )
    
    print("  Deduplication complete")
    return merged

def write_to_parquet(df, output_path):
    """
    Write DataFrame to Parquet with snapshot timestamp partitioning.
    Enables efficient querying by snapshot.
    """
    print(f"\nWriting to Parquet: {output_path}")
    
    # Count unique snapshots to determine optimal partition count
    print("   Counting unique snapshots...") # 4062
    num_snapshots = df.select("snapshot_timestamp").distinct().count()
    print(f"   Found {num_snapshots} unique snapshots")
    
    # Repartition: 1 partition per snapshot = 1 file per snapshot folder
    print(f"   Repartitioning to {num_snapshots} partitions before write...")
    df_optimized = df.repartition(num_snapshots, "snapshot_timestamp")
    
    df_optimized.write \
        .mode("overwrite") \
        .partitionBy("snapshot_timestamp") \
        .option("compression", "snappy") \
        .parquet(output_path)
    
    print(f"✓ Successfully written to {output_path}")

def main():
    """Main ETL pipeline: Extract (read XML) → Transform (parse, dedupe) → Load (Parquet)"""
    print("=" * 80)
    print("DBahn Berlin - True End-to-End Spark ETL Pipeline")
    print("=" * 80)
    
    # Initialize Spark
    print("\n[1/5] Initializing Spark Session...")
    spark = create_spark_session()
    print(f"   Spark version: {spark.version}")
    print(f"   App name: {spark.sparkContext.appName}")
    
    timetables_path = '/tmp/data/timetables_extracted'
    changes_path = '/tmp/data/timetable_changes_extracted'
    output_path = '/opt/spark-apps/output/dbahn_berlin_spark_etl'
    
    print("\n[2/5] EXTRACT: Reading XML files with Spark...")
    print(f"\n   Processing timetables from: {timetables_path}")
    timetable_rdd = extract_xml_files(spark, timetables_path, 'timetable')
    timetable_df = transform_to_dataframe(spark, timetable_rdd, 'timetable')
    
    print("   Caching timetables in memory for broadcast join...")
    timetable_df.cache()
    print(f"   ✓ Timetables ready (will materialize on first use)")
   
    print(f"\n   Processing changes from: {changes_path}")
    change_rdd = extract_xml_files(spark, changes_path, 'change')
    change_df = transform_to_dataframe(spark, change_rdd, 'change')
    
    print("\n[3/5] TRANSFORM: Deduplicating and merging...")
    deduped_df = deduplicate_and_merge(timetable_df, change_df)
    print("   ✓ Deduplication and merge complete")
    timetable_df.unpersist()
    
    print("\n[4/5] TRANSFORM: Final dataset ready...")
    
    print("\n[5/5] LOAD: Writing to Parquet...")
    write_to_parquet(deduped_df, output_path)
    
    print("\n" + "=" * 80)
    print("ETL PIPELINE COMPLETE!")
    print("=" * 80)
    print(f"\n📁 Output location: {output_path}")
    print("\nTo query the data:")
    print("   df = spark.read.parquet('" + output_path + "')")
    print("   df.show()")


if __name__ == "__main__":
    main()
