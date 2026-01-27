# Spark Queries for Deutsche Bahn Parquet Dataset
# Task 3.2: Compute average daily delay for a given station

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, when, substring, to_timestamp, unix_timestamp,
    round as spark_round, concat, lit, count, levenshtein, lower
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


def normalize_station_name(name):
    """
    Normalize station name: lowercase and remove 'berlin' or 'berlin-' prefix.
    
    Args:
        name: Station name string
    
    Returns:
        Normalized station name (lowercase, without berlin prefix)
    """
    if not name:
        return ""
    
    normalized = name.lower().strip()
    
    # Remove 'berlin-' or 'berlin ' prefix
    if normalized.startswith("berlin-"):
        normalized = normalized[7:]  # Remove "berlin-"
    elif normalized.startswith("berlin "):
        normalized = normalized[7:]  # Remove "berlin "
    
    return normalized


def find_station_in_data(spark, parquet_path, search_name):
    """
    Find station name using Levenshtein similarity matching.
    Similar to PostgreSQL's fuzzystrmatch extension.
    
    Workflow:
    1. Get unique station names from Parquet
    2. Lowercase everything for comparison
    3. Normalize station names to remove 'berlin' and 'berlin-' prefixes
    4. Lowercase search query (don't normalize - keep as-is)
    5. Find best match using Levenshtein distance
    
    Args:
        spark: SparkSession
        parquet_path: Path to the Parquet dataset
        search_name: Station name to search for
    
    Returns:
        Exact station name from dataset, or None if not found
    """
    # Special case handling for ambiguous station names
    search_lower = search_name.lower().strip()
    if "yorckstraße (großgörschenstraße)" in search_lower:
        return "Berlin Yorckstr.(S1)"
    elif "yorckstraße" in search_lower:
        return "Berlin Yorckstr.(S2)"
    
    # Step 1: Get unique station names from Parquet
    df = spark.read.parquet(parquet_path)
    stations_df = df.select("station_name").distinct()
    
    # Step 2 & 3: Lowercase and normalize (remove berlin prefix from station names)
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
    
    # Step 4: Just lowercase the search query (don't normalize)
    search_lower = search_name.lower().strip()
    
    # Step 5: Find best match using Levenshtein distance
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


def parse_dbahn_timestamp(df, input_col, output_col):
    """
    Parse Deutsche Bahn timestamp format (YYMMddHHmm) to proper timestamp.
    Example: '2509021628' -> 2025-09-02 16:28:00
    
    Args:
        df: DataFrame with the timestamp column
        input_col: Name of the input column with YYMMddHHmm format
        output_col: Name of the output timestamp column
    
    Returns:
        DataFrame with parsed timestamp column added
    """
    # Convert YYMMddHHmm to yyyy-MM-dd HH:mm format
    # YY -> 20YY, MM -> month, dd -> day, HH -> hour, mm -> minute
    return df.withColumn(
        output_col,
        to_timestamp(
            concat(
                lit("20"),                           # Century prefix
                substring(col(input_col), 1, 2),     # YY (year)
                lit("-"),
                substring(col(input_col), 3, 2),     # MM (month)
                lit("-"),
                substring(col(input_col), 5, 2),     # dd (day)
                lit(" "),
                substring(col(input_col), 7, 2),     # HH (hour)
                lit(":"),
                substring(col(input_col), 9, 2)      # mm (minute)
            ),
            "yyyy-MM-dd HH:mm"
        )
    )


def compute_average_daily_delay(spark, parquet_path, station_name):
    """
    Task 3.2: Compute the average daily delay for a given station.
    
    Delay is calculated as the difference between changed_time (ct) and 
    planned_time (pt) in minutes. Only events with actual delays (changed_time 
    exists and differs from planned_time) are considered.
    
    Args:
        spark: SparkSession
        parquet_path: Path to the Parquet dataset
        station_name: Name of the station to analyze (e.g., "Alexanderplatz" or "Berlin Alexanderplatz")
    
    Returns:
        DataFrame with columns: date, avg_delay_minutes, total_events, delayed_events
    """
    print(f"\n{'='*80}")
    print(f"Task 3.2: Average Daily Delay for Station: {station_name}")
    print(f"{'='*80}")
    
    # Read Parquet dataset
    print(f"\nReading data from: {parquet_path}")
    df = spark.read.parquet(parquet_path)
    
    # Find exact station name in dataset (handles naming variations)
    exact_station_name = find_station_in_data(spark, parquet_path, station_name)
    
    if not exact_station_name:
        print(f"\n⚠ Station '{station_name}' not found in dataset")
        print("\nSuggestion: Try one of these station names:")
        stations = df.select("station_name").distinct().orderBy("station_name").limit(20)
        for row in stations.collect():
            print(f"   • {row.station_name}")
        print("   ...")
        return None
    
    if exact_station_name != station_name:
        print(f"📍 Matched to: '{exact_station_name}'")
    
    # Filter for the exact station name from the dataset
    station_df = df.filter(col("station_name") == exact_station_name)
    
    # Count total records for this station
    total_records = station_df.count()
    print(f"Total events for '{exact_station_name}': {total_records}")
    
    if total_records == 0:
        print(f"\n⚠ No data found for station '{exact_station_name}'")
        return None
    
    events_df = station_df
    
    # Parse timestamps - parse both planned and changed (if exists)
    events_df = parse_dbahn_timestamp(events_df, "planned_time", "planned_ts")
    
    # For changed_time: if NULL, use planned_time (0 delay)
    events_df = events_df.withColumn(
        "changed_time_filled",
        when(col("changed_time").isNotNull(), col("changed_time"))
        .otherwise(col("planned_time"))
    )
    events_df = parse_dbahn_timestamp(events_df, "changed_time_filled", "changed_ts")
    
    # Calculate delay in minutes (will be 0 if changed_time was NULL)
    events_df = events_df.withColumn(
        "delay_minutes",
        (unix_timestamp(col("changed_ts")) - unix_timestamp(col("planned_ts"))) / 60
    )
    
    # Extract date from planned_time (first 6 characters: YYMMdd)
    events_df = events_df.withColumn(
        "date",
        concat(
            lit("20"),
            substring(col("planned_time"), 1, 2),  # YY
            lit("-"),
            substring(col("planned_time"), 3, 2),  # MM
            lit("-"),
            substring(col("planned_time"), 5, 2)   # dd
        )
    )
    
    # Compute average daily delay (including 0-delay events)
    daily_avg_df = events_df.groupBy("date").agg(
        avg("delay_minutes").alias("avg_delay_minutes")
    )
    
    # Calculate the average of daily averages
    overall_avg = daily_avg_df.agg(
        spark_round(avg("avg_delay_minutes"), 2).alias("average_daily_delay")
    ).collect()[0]["average_daily_delay"]
    
    print(f"\n📊 Average Daily Delay for '{exact_station_name}': {overall_avg} minutes")
    
    return overall_avg


def list_available_stations(spark, parquet_path):
    """List all available stations in the dataset."""
    df = spark.read.parquet(parquet_path)
    print("\n📍 Available Stations:")
    print("-" * 40)
    stations = df.select("station_name").distinct().orderBy("station_name")
    stations.show(200, truncate=False)
    return stations


def main():
    """Main function to run delay analysis queries."""
    print("=" * 80)
    print("DBahn Berlin - Spark Analytical Queries")
    print("=" * 80)
    
    # Initialize Spark
    spark = create_spark_session()
    print(f"Spark version: {spark.version}")
    
    # Path to Parquet dataset
    # Docker path: /opt/spark-apps/output/dbahn_berlin_spark_etl
    # Local path: output/dbahn_berlin_spark_etl
    parquet_path = "/opt/spark-apps/output/dbahn_berlin_spark_etl"
    
    # Example: Query with fuzzy string matching (similar to PostgreSQL's fuzzystrmatch)
    # Works with:
    # - Names from station_data.json (e.g., "Alexanderplatz")
    # - Timetable names (e.g., "Berlin Alexanderplatz")
    # - Typos and variations (e.g., "Alexandrplatz", "Alex")
    
    print("\n" + "-" * 80)
    print("Example 1: Exact match with 'Berlin' prefix (timetable format)")
    result = compute_average_daily_delay(spark, parquet_path, "Alexanderplatz")
    
    # Optionally list all available stations
    # list_available_stations(spark, parquet_path)
    
    print("\n" + "=" * 80)
    print("Query execution complete!")
    print("=" * 80)
    
    spark.stop()


if __name__ == "__main__":
    main()
