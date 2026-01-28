from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, broadcast, regexp_extract, row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType
)
import xml.etree.ElementTree as ET

def create_spark_session():
    """
    Spark session config (config used from spark docu, optimized for local development since workers take more time)
    """
    spark = SparkSession.builder \
        .appName("DBahn-Berlin-ETL-Pipeline") \
        .master("local[4]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .config("spark.driver.memory", "6g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# Schema for timetable events  
TIMETABLE_SCHEMA = StructType([
    StructField("stop_id", StringType(), False),
    StructField("snapshot_timestamp", StringType(), False),
    StructField("snapshot_type", StringType(), False),
    StructField("station_name", StringType(), True),
    StructField("event_type", StringType(), False),
    StructField("planned_time", StringType(), False),
])

# Schema for change events
CHANGE_SCHEMA = StructType([
    StructField("stop_id", StringType(), False),
    StructField("snapshot_timestamp", StringType(), False),
    StructField("snapshot_type", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("changed_time", StringType(), True),
    StructField("changed_status", StringType(), True),
])

TIMETABLES_PATH='/tmp/data/timetables_extracted'
CHANGES_PATH='/tmp/data/timetable_changes_extracted'
OUTPUT_PATH='/opt/spark-apps/output/dbahn_berlin_spark_etl'

def parse_timetable_xml(xml_content, snapshot_timestamp, snapshot_type):
    """
    Parse timetable data from xml content. also we perform missing value imputation in case e.g. station names are missing
    """
    events = []
    
    try:
        root = ET.fromstring(xml_content)
        station_name = root.get('station') or 'Unknown Station'
        
        for stop in root.findall('s'):
            stop_id = stop.get('id')
            
            ar = stop.find('ar')
            if ar is not None: # other case handled later during filtering for data quality
                events.append((
                    stop_id,
                    snapshot_timestamp,
                    snapshot_type,
                    station_name,
                    'arrival',
                    ar.get('pt'),
                ))
            
            dp = stop.find('dp')
            if dp is not None:
                events.append((
                    stop_id,
                    snapshot_timestamp,
                    snapshot_type,
                    station_name,
                    'departure',
                    dp.get('pt'),
                ))
                
    except Exception as e:
        print(f"An error occured in tree parsing: {e}")
        return []
    
    return events

def parse_change_xml(xml_content, snapshot_timestamp, snapshot_type):
    """Parse timetable data from xml content."""

    events = []
    
    try:
        root = ET.fromstring(xml_content)
        
        for stop in root.findall('s'):
            stop_id = stop.get('id')
            
            ar = stop.find('ar')
            if ar is not None: # other case handeled later during filtering for data quality
                events.append((
                    stop_id,
                    snapshot_timestamp,
                    snapshot_type,
                    'arrival',
                    ar.get('ct'),
                    ar.get('cs'),
                ))
            
            dp = stop.find('dp')
            if dp is not None:
                events.append((
                    stop_id,
                    snapshot_timestamp,
                    snapshot_type,
                    'departure',
                    dp.get('ct'),
                    dp.get('cs'),
                ))
                
    except Exception as e:
        print(f"Eror occured during parsing XML: {e}")
        return []
    
    return events

def extract_xml_files(spark, base_path, snapshot_type):
    """
    Read xml files with spark and return unprocessed as binary files
    """
    file_pattern = "*_timetable.xml" if snapshot_type == 'timetable' else "*_change.xml"
        
    raw_df = spark.read.format("binaryFile") \
        .option("pathGlobFilter", file_pattern) \
        .option("recursiveFileLookup", "true") \
        .load(base_path)
        
    return raw_df

def filter_missing_fields(df, snapshot_type):
    """
    Filter out records with missing critical fields to increase data quality
    """
    if snapshot_type == 'timetable':
        df = df.filter(
            col("stop_id").isNotNull() & 
            col("event_type").isNotNull() &
            col("planned_time").isNotNull()
        )
    else:
        df = df.filter(
            col("stop_id").isNotNull() & 
            col("event_type").isNotNull()
        )
    return df

def transform_to_dataframe(spark, raw_df, snapshot_type):
    """Transform binary DataFrame to parsed event DataFrame using the appropriate schema"""
    if snapshot_type == 'timetable':
        parse_func = parse_timetable_xml
        schema = TIMETABLE_SCHEMA
    else:
        parse_func = parse_change_xml
        schema = CHANGE_SCHEMA
    
    # Extract timestamp with spark
    raw_df = raw_df.withColumn(
        "snapshot_timestamp",
        regexp_extract(col("path"), r"/(\d{10})/", 1)
    )
    
    def process_row(row):
        """we apply this to every element / row in the df to extract the xml content"""
        binary_content = row.content
        snapshot_timestamp = row.snapshot_timestamp if row.snapshot_timestamp else 'unknown'
        
        xml_string = binary_content.decode('utf-8')
        return parse_func(xml_string, snapshot_timestamp, snapshot_type)

    # we use flatmap to flatten the list while processing every row. we then receive one flat collection, which we can create a spark df from
    events_rdd = raw_df.rdd.flatMap(process_row)
    df = spark.createDataFrame(events_rdd, schema=schema)
    
    # Filter out records with fields that may not miss to ensure data quality
    df = filter_missing_fields(df, snapshot_type)
    
    return df

def deduplicate_and_merge(timetable_df, change_df):
    """
    Remove old change records, keep only the newst one. then left join timetable with change df
    """    
    # drop old change records, keep only the most recent once
    window_spec = Window.partitionBy("stop_id", "event_type").orderBy(col("snapshot_timestamp").desc())
    latest_changes = change_df \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")

    # join over id and event type
    merged = timetable_df.alias("t").join(
        broadcast(latest_changes).alias("c"),
        (col("t.stop_id") == col("c.stop_id")) & 
        (col("t.event_type") == col("c.event_type")),
        "left"
    ).select(
        col("t.stop_id"),
        col("t.event_type"),
        when(col("c.snapshot_timestamp").isNotNull(), col("c.snapshot_timestamp"))
            .otherwise(col("t.snapshot_timestamp")).alias("snapshot_timestamp"),
        col("t.station_name"),
        col("t.planned_time"),
        col("c.changed_time"),
        col("c.changed_status")
    )
    
    return merged

def write_to_parquet(df, output_path):
    """
    Write df to parquet with snapshot timestamp partitioning.
    """    
    sc = df.sparkSession.sparkContext

    # Count unique snapshots to determine optimal partition count. earlier we marked for cache, so it will cache here (otherwise in write we would do it all over again)")
    sc.setJobDescription("#1 Everything up to caching and counting distint snapshots")
    num_snapshots = df.select("snapshot_timestamp").distinct().count() # 4062
    print("-" * 60)
    print("Transformation finished, about to write to parquet...")
    print("-" * 60)

    # repartition to number of distinct snapshots
    df_optimized = df.repartition(num_snapshots, "snapshot_timestamp")
    
    sc.setJobDescription("#2 Writing partitioned Parquet files")
    df_optimized.write \
        .mode("overwrite") \
        .partitionBy("snapshot_timestamp") \
        .option("compression", "snappy") \
        .parquet(output_path)
    
    print("-" * 60)
    print("Write to parquet completed")
    print("-" * 60)

def main():
    """We extraxt XML, transform it and then load it into parquet"""
    print("-" * 60)
    print("Pipeline started")
    print("-" * 60)
    
    spark = create_spark_session()
    print("-" * 60)
    print("\nSpark session initialized")
    print("-" * 60)


    # Process timetable dataset first. extraxt, then transform to df
    timetable_rdd = extract_xml_files(spark, TIMETABLES_PATH, 'timetable')
    timetable_df = transform_to_dataframe(spark, timetable_rdd, 'timetable')
   
    # Process changetimetable dataset. extraxt, then transform to df
    change_rdd = extract_xml_files(spark, CHANGES_PATH, 'change')
    change_df = transform_to_dataframe(spark, change_rdd, 'change')
    
    # we merge both datasets and then deduplicate (join)
    deduped_df = deduplicate_and_merge(timetable_df, change_df)

    # cache as checkpoint since we are gonna built on this in two operations and dont wanna trigger rerun. Then write to parquet
    deduped_df.cache()
    write_to_parquet(deduped_df, OUTPUT_PATH)
    deduped_df.unpersist()
    
    print("-" * 60)
    print("Pipeline completed")
    print("-" * 60)

if __name__ == "__main__":
    main()
