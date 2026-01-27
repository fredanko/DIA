# Spark Queries for Deutsche Bahn Parquet Dataset
# Task 3.3: Average number of train departures per station during peak hours

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, when, substring, count, levenshtein, lower, lit,
    countDistinct
)
import os
import sys

# Windows-specific Spark configuration
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def create_spark_session():
    """Create Spark session for querying Parquet data."""
    spark = SparkSession.builder \
        .appName("DBahn-Berlin-Queries") \
        .master("local[4]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def find_station_in_data(spark, parquet_path, search_name):
    """
    Find station name using Levenshtein similarity matching.
    """
    search_lower = search_name.lower().strip()
    if "yorckstraße (großgörschenstraße)" in search_lower:
        return "Berlin Yorckstr.(S1)"
    elif "yorckstraße" in search_lower:
        return "Berlin Yorckstr.(S2)"
    
    df = spark.read.parquet(parquet_path)
    stations_df = df.select("station_name").distinct()
    
    stations_df = stations_df.withColumn(
        "normalized_name",
        lower(col("station_name"))
    ).withColumn(
        "normalized_name",
        when(col("normalized_name").startswith("berlin-"), 
             substring(col("normalized_name"), 8, 1000))
        .when(col("normalized_name").startswith("berlin "),
             substring(col("normalized_name"), 8, 1000))
        .otherwise(col("normalized_name"))
    )
    
    search_lower = search_name.lower().strip()
    
    stations_df = stations_df.withColumn(
        "levenshtein_distance",
        levenshtein(col("normalized_name"), lit(search_lower))
    )
    
    best_match = stations_df.orderBy("levenshtein_distance").first()
    
    if best_match:
        if best_match.levenshtein_distance > 0:
            print(f"🔍 Fuzzy match: '{search_name}' → '{best_match.station_name}' (Levenshtein distance: {best_match.levenshtein_distance})")
        return best_match.station_name
    
    return None


def compute_avg_peak_departures(spark, parquet_path, station_name):
    """
    Task 3.3: Return the average number of train departures per station 
    during peak hours (07:00-09:00 and 17:00-19:00).
    
    Args:
        spark: SparkSession
        parquet_path: Path to the Parquet dataset
        station_name: Name of the station to analyze
    
    Returns:
        Average number of departures during peak hours
    """
    df = spark.read.parquet(parquet_path)
    
    # Find exact station name
    exact_station_name = find_station_in_data(spark, parquet_path, station_name)
    
    if not exact_station_name:
        print(f"⚠ Station '{station_name}' not found in dataset")
        return None
    
    if exact_station_name != station_name:
        print(f"📍 Matched to: '{exact_station_name}'")
    
    # Filter for station and departures only
    station_df = df.filter(
        (col("station_name") == exact_station_name) &
        (col("event_type") == "departure")
    )
    
    # Use changed_time if available, otherwise planned_time
    station_df = station_df.withColumn(
        "actual_time",
        when(col("changed_time").isNotNull(), col("changed_time"))
        .otherwise(col("planned_time"))
    )
    
    # Extract hour from actual_time (positions 7-8 in YYMMddHHmm)
    station_df = station_df.withColumn(
        "hour",
        substring(col("actual_time"), 7, 2).cast("int")
    )
    
    # Filter for peak hours: 07:00-09:00 (7,8) and 17:00-19:00 (17,18)
    peak_df = station_df.filter(
        ((col("hour") >= 7) & (col("hour") < 9)) |
        ((col("hour") >= 17) & (col("hour") < 19))
    )
    
    # Extract date from planned_time (YYMMdd)
    peak_df = peak_df.withColumn(
        "date",
        substring(col("planned_time"), 1, 6)
    )
    
    # Count departures per day
    daily_counts = peak_df.groupBy("date").agg(
        count("*").alias("departures")
    )
    
    # Calculate average across all days
    avg_departures = daily_counts.agg(
        avg("departures").alias("avg_peak_departures")
    ).collect()[0]["avg_peak_departures"]
    
    if avg_departures:
        avg_departures = round(avg_departures, 2)
    
    print(f"\n📊 Average Peak Hour Departures for '{exact_station_name}': {avg_departures}")
    
    return avg_departures


def main():
    """Main function."""
    spark = create_spark_session()
    print(f"Spark version: {spark.version}")
    
    parquet_path = "/opt/spark-apps/output/dbahn_berlin_spark_etl"
    
    # Test with Alexanderplatz
    result = compute_avg_peak_departures(spark, parquet_path, "Alexanderplatz")
    
    spark.stop()


if __name__ == "__main__":
    main()
