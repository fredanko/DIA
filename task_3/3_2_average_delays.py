from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, when, unix_timestamp, substring,
    round as spark_round, lit, levenshtein, lower, to_date
)

PARQUET_PATH = "/opt/spark-apps/output/dbahn_berlin_spark_etl"

def create_spark_session():
    """
    spark session config setup
    """
    spark = SparkSession.builder \
        .appName("DBahn-Berlin-3-2") \
        .master("local[4]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .config("spark.driver.memory", "6g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def find_station_in_data(spark, search_name):
    """
    Assuming the station names are from the station json we perform a levensthein search to find the closest one.
    For this we get all unique station names from the dataset and pick the most similar one
    """
    # Extra case as per forum post, although we changed this in station_data, will just leave it in here
    search_lower = search_name.lower().strip()
    if "yorckstraße (großgörschenstraße)" in search_lower:
        return "Berlin Yorckstr.(S1)"
    elif "yorckstraße" in search_lower:
        return "Berlin Yorckstr.(S2)"
    
    sc = spark.sparkContext
    sc.setJobDescription("#1 Reading Parquet data to find station")
    df = spark.read.parquet(PARQUET_PATH)

    # get unique station names from dataset
    stations_df = df.select("station_name").distinct()
    
    # preprocess unique station names
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
    
    # find closest match in levnshtein distance
    stations_df = stations_df.withColumn(
        "levenshtein_distance",
        levenshtein(col("normalized_name"), lit(search_lower))
    )
    
    best_match = stations_df.orderBy("levenshtein_distance").first()
    return best_match.station_name

def compute_average_daily_delay(station_name):
    """
    computes average daily delay over all days for a given station name.
    If there are no events for a station on a given day, that day counts as 0 delay
    """
    spark = create_spark_session()
    sc = spark.sparkContext

    # Read Parquet dataset
    df = spark.read.parquet(PARQUET_PATH)
    
    # match to proper station name
    exact_station_name = find_station_in_data(spark, station_name)
    
    if exact_station_name != station_name:
        print(f"\n'{station_name}' matched to '{exact_station_name}'.")
    
    # filter for rows of our station
    station_df = df.filter(col("station_name") == exact_station_name)
    
    # Filter out cancelled trains
    station_df = station_df.filter(
        (col("changed_status").isNull()) | (col("changed_status") != "c")
    )
    
    # if a train departs /arrives past midnight due to a change that counts towards the next day
    station_df = station_df.withColumn(
        "adjusted_time",
        when(col("changed_time").isNotNull(), col("changed_time"))
        .otherwise(col("planned_time"))
    )
    
    # Calculate delay in minutes, will be 0 if adjustedtime = plannedtime
    station_df = station_df.withColumn(
        "delay_minutes",
        (unix_timestamp(col("adjusted_time")) - unix_timestamp(col("planned_time"))) / 60
    )
    
    # extract date 
    station_df = station_df.withColumn(
        "date",
        to_date(col("planned_time"))
    )
    
    # Compute average daily delay
    daily_avg_df = station_df.groupBy("date").agg(
        avg("delay_minutes").alias("avg_delay_minutes")
    )
    
    # Calculate the average of daily averages
    sc.setJobDescription("#2 Filtering and computing averages")
    overall_avg = daily_avg_df.agg(
        spark_round(avg("avg_delay_minutes"), 2).alias("average_daily_delay")
    ).collect()[0]["average_daily_delay"]

    spark.stop()
    
    return overall_avg

def main():
    """
    Runs the query, please adjust the station name if you want to try a different one
    """
    print("-" * 60)
    print("Query 3.2 started")
    print("-" * 60)
    
    station_name = "Alexanderplatz" # change station name here
    result = compute_average_daily_delay(station_name)

    print("-" * 60)
    print(f"Average Daily Delay for '{station_name}': {result} minutes")
    print("-" * 60)

    print("-" * 60)
    print("Query 3.2 completed")
    print("-" * 60)


if __name__ == "__main__":
    main()
