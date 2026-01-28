from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, hour, to_date, when,
    round as spark_round
)

PARQUET_PATH = "/opt/spark-apps/output/dbahn_berlin_spark_etl"

def create_spark_session():
    """
    spark session config setup
    """
    spark = SparkSession.builder \
        .appName("DBahn-Berlin-3-3") \
        .master("local[4]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .config("spark.driver.memory", "6g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def compute_avg_peak_departures_all_stations():
    """
    Returns average number of train departures during peak hours for all stations
    """
    spark = create_spark_session()
    sc = spark.sparkContext
    
    sc.setJobDescription("#1 Reading Parquet data")
    df = spark.read.parquet(PARQUET_PATH)

    # Count total distinct dates in the entire dataset
    dates_df = df.select("planned_time") \
        .withColumn("date", to_date(col("planned_time"))) \
        .select("date").distinct().orderBy("date")
    
    sc.setJobDescription("#2 Counting distinct dates")
    total_dates = dates_df.count()
    
    departures_df = df.filter(col("event_type") == "departure")
    
    # Filter out cancelled trains
    departures_df = departures_df.filter(
        (col("changed_status").isNull()) | (col("changed_status") != "c")
    )
    
    # if a train departs past midnight due to a change that counts towards the next day (very unlikely though cause of the hours)
    departures_df = departures_df.withColumn(
        "adjusted_time",
        when(col("changed_time").isNotNull(), col("changed_time")).otherwise(col("planned_time"))
    )
    
    # Extract hour to compare with peak hours
    departures_df = departures_df.withColumn(
        "departure_hour",
        hour(col("adjusted_time"))
    )
    
    # Filter for peak hours
    peak_df = departures_df.filter(
        ((col("departure_hour") >= 7) & (col("departure_hour") < 9)) |
        ((col("departure_hour") >= 17) & (col("departure_hour") < 19))
    )
    
    # Extract date from planned_time
    peak_df = peak_df.withColumn(
        "date",
        to_date(col("planned_time"))
    )
    
    # Sum total departures per station during peak hours
    total_departures_by_station = peak_df.groupBy("station_name").agg(
        count("*").alias("total_peak_departures")
    )
    
    # Calculate average (using total dates)
    sc.setJobDescription("#3 Filtering, then computing averages")
    avg_by_station = total_departures_by_station.withColumn(
        "avg_peak_departures",
        spark_round(col("total_peak_departures") / total_dates, 2)
    ).orderBy(col("avg_peak_departures").desc())
    
    print("-" * 60)
    print("Average Peak Hour Departures by Station:\n")
    avg_by_station.select("station_name", "avg_peak_departures").show(1000, truncate=False)
    print("-" * 60)
    
    spark.stop()
    
    return avg_by_station

def main():
    """
    Runs the query
    """
    print("-" * 60)
    print("Query 3.3 started")
    print("-" * 60)
    
    compute_avg_peak_departures_all_stations()
    
    print("-" * 60)
    print("Query 3.3 completed")
    print("-" * 60)


if __name__ == "__main__":
    main()
