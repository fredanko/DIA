from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, broadcast, regexp_extract, row_number, lit, explode, input_file_name,
    to_timestamp, concat, substring
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

def create_spark_session():
    """
    spark session config setup
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

TIMETABLES_PATH='/tmp/data/timetables_extracted'
CHANGES_PATH='/tmp/data/timetable_changes_extracted'
OUTPUT_PATH='/opt/spark-apps/output/dbahn_berlin_spark_etl'

# Schema for both datasets avoids initial schema parsing and saves time. we already know the structure
TIMETABLE_XML_SCHEMA = StructType([
    StructField("_station", StringType(), True),
    StructField("s", ArrayType(StructType([
        StructField("_id", StringType(), True), 
        StructField("ar", StructType([
            StructField("_pt", StringType(), True),  
        ]), True),
        StructField("dp", StructType([
            StructField("_pt", StringType(), True),
        ]), True),
    ])), True),
])

CHANGE_XML_SCHEMA = StructType([
    StructField("_id", StringType(), True),
    StructField("ar", StructType([
        StructField("_ct", StringType(), True),
        StructField("_cs", StringType(), True),
    ]), True),
    StructField("dp", StructType([
        StructField("_ct", StringType(), True), 
        StructField("_cs", StringType(), True),
    ]), True),
])

def parse_dbahn_timestamp(df, input_col, output_col=None):
    """
    Parse db timestamp format to spark compatible timestamp format (also helpful for spark queries later)
    """
    if output_col is None:
        output_col = input_col
    
    return df.withColumn(
        output_col,
        to_timestamp(
            concat(
                lit("20"),                           
                substring(col(input_col), 1, 2),
                lit("-"),
                substring(col(input_col), 3, 2),
                lit("-"),
                substring(col(input_col), 5, 2),
                lit(" "),
                substring(col(input_col), 7, 2),
                lit(":"),
                substring(col(input_col), 9, 2)
            ),
            "yyyy-MM-dd HH:mm"
        )
    )

def read_timetable_xml(spark):
    """
    Read timetable XML files using spark-xml
    """
    # define layout, see https://spark.apache.org/docs/latest/sql-data-sources-xml.html
    df = spark.read.format("com.databricks.spark.xml") \
        .option("rowTag", "timetable") \
        .option("attributePrefix", "_") \
        .option("valueTag", "_value") \
        .schema(TIMETABLE_XML_SCHEMA) \
        .load(f"{TIMETABLES_PATH}/*/*.xml")
    
    # Extract snapshot timestamp
    df = df.withColumn("_file_path", input_file_name())
    df = df.withColumn(
        "snapshot_timestamp",
        regexp_extract(col("_file_path"), r"/(\d{10})/", 1)
    )
    
    # reduce partition count since sparkxml creates a bunch of partitions and switching between so many tasks is expensive
    df = df.coalesce(200)
    
    # spread out into separate rows
    df = df.select(
        col("_station").alias("station_name"),
        col("snapshot_timestamp"),
        explode(col("s")).alias("stop")
    )
    
    # Create arrival events, filter nulls for data quality
    arrivals = df.filter(col("stop.ar").isNotNull()).select(
        col("stop._id").alias("stop_id"),
        col("snapshot_timestamp"),
        lit("timetable").alias("snapshot_type"),
        col("station_name"),
        col("stop.ar._pt").alias("planned_time"),
        lit("arrival").alias("event_type")
    )
    
    # Create departure events, filter nulls for data quality
    departures = df.filter(col("stop.dp").isNotNull()).select(
        col("stop._id").alias("stop_id"),
        col("snapshot_timestamp"),
        lit("timetable").alias("snapshot_type"),
        col("station_name"),
        col("stop.dp._pt").alias("planned_time"),
        lit("departure").alias("event_type")
    )
    
    # Union arrivals and departures, filter nulls for data quality
    result = arrivals.union(departures).filter(
        col("stop_id").isNotNull() & 
        col("planned_time").isNotNull() &
        col("station_name").isNotNull()
    )
    
    # Parse time fomat
    result = parse_dbahn_timestamp(result, "planned_time")
    
    return result

def read_change_xml(spark):
    """
    Read change XML files using spark-xml
    """
    # define layout, see https://spark.apache.org/docs/latest/sql-data-sources-xml.html
    df = spark.read.format("com.databricks.spark.xml") \
        .option("rowTag", "s") \
        .option("rootTag", "timetable") \
        .option("attributePrefix", "_") \
        .option("valueTag", "_value") \
        .schema(CHANGE_XML_SCHEMA) \
        .load(f"{CHANGES_PATH}/*/*.xml")
    
    # Extract snapshot timestamp
    df = df.withColumn("_file_path", input_file_name())
    df = df.withColumn(
        "snapshot_timestamp",
        regexp_extract(col("_file_path"), r"/(\d{10})/", 1)
    )
    
    # reduce partition count since sparkxml creates a bunch of partitions and switching between so many tasks is expensive
    df = df.coalesce(200)
    
    # Create arrival change events, filter nulls for data quality
    arrivals = df.filter(col("ar").isNotNull()).select(
        col("_id").alias("stop_id"),
        col("snapshot_timestamp"),
        lit("change").alias("snapshot_type"),
        lit("arrival").alias("event_type"),
        col("ar._ct").alias("changed_time"),
        col("ar._cs").alias("changed_status")
    )
    
    # Create departure change events, filter nulls for data quality
    departures = df.filter(col("dp").isNotNull()).select(
        col("_id").alias("stop_id"),
        col("snapshot_timestamp"),
        lit("change").alias("snapshot_type"),
        lit("departure").alias("event_type"),
        col("dp._ct").alias("changed_time"),
        col("dp._cs").alias("changed_status")
    )
    
    # Union arrivals and departures, filter nulls
    result = arrivals.union(departures).filter(
        col("stop_id").isNotNull()
    )
    
    # parse time format
    result = parse_dbahn_timestamp(result, "changed_time")
    
    return result

def deduplicate_and_merge(timetable_df, change_df):
    """
    Remove old change records, keep only the newest one. Then join timetable with change df.
    """    
    # Drop old change records
    window_spec = Window.partitionBy("stop_id", "event_type").orderBy(col("snapshot_timestamp").desc())
    latest_changes = change_df \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")

    # Join over id and event type
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

def write_to_parquet(df):
    """
    Write df to parquet with snapshot timestamp partitioning.
    """    
    sc = df.sparkSession.sparkContext

    # Count unique snapshots to determine optimal partition count. earlier we marked for cache, so it will cache here (otherwise in write we would do it all over again)")
    sc.setJobDescription("#1 Everything up to caching and counting distint snapshots")
    num_snapshots = df.select("snapshot_timestamp").distinct().count()
    print("-" * 60)
    print("Transformation finished, about to write to parquet...")
    print("-" * 60)

    # repartition to number of distinct snapshots
    df_optimized = df.repartition(num_snapshots, "snapshot_timestamp")
    
    sc.setJobDescription("#2 Repartitioning and writing partitioned Parquet files")
    df_optimized.write \
        .mode("overwrite") \
        .partitionBy("snapshot_timestamp") \
        .option("compression", "snappy") \
        .parquet(OUTPUT_PATH)
    
    print("-" * 60)
    print("Write to parquet completed")
    print("-" * 60)

def main():
    """
    Etl pipeline code
    """
    print("-" * 60)
    print("Pipeline started")
    print("-" * 60)
    
    spark = create_spark_session()
    print("-" * 60)
    print("Spark session initialized")
    print("-" * 60)

    # Read timetable XMLs using spark-xml
    timetable_df = read_timetable_xml(spark)
    print(f"Timetable events loaded")
   
    # Read change XMLs using spark-xml
    change_df = read_change_xml(spark)
    print(f"Change events loaded")
    
    # Deduplicate changes and merge with timetables using DataFrame operations
    deduped_df = deduplicate_and_merge(timetable_df, change_df)

    # Cache as checkpoint since we build on this in two operations
    deduped_df.cache()
    write_to_parquet(deduped_df)
    deduped_df.unpersist()

    spark.stop()
    
    print("-" * 60)
    print("Pipeline completed")
    print("-" * 60)

if __name__ == "__main__":
    main()
