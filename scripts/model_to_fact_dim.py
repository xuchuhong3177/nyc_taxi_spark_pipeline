# scripts/model_fact_dim.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, hour
import os

def get_spark_session(app_name="NYC Taxi Model Builder"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def build_dimensions_and_fact(input_parquet, output_dir):
    spark = get_spark_session()

    df = spark.read.parquet(input_parquet)

    # --- DIM DATETIME ---
    dim_datetime = df.select(
        col("tpep_pickup_datetime").alias("datetime"),
        year("tpep_pickup_datetime").alias("year"),
        month("tpep_pickup_datetime").alias("month"),
        dayofmonth("tpep_pickup_datetime").alias("day"),
        hour("tpep_pickup_datetime").alias("hour")
    ).dropDuplicates()

    dim_datetime = dim_datetime.withColumn("datetime_id", col("datetime").cast("long"))
    dim_datetime.write.mode("overwrite").parquet(os.path.join(output_dir, "dim_datetime"))

    # --- DIM LOCATION ---
    dim_location = df.select(
        col("PULocationID").alias("location_id")
    ).union(
        df.select(col("DOLocationID").alias("location_id"))
    ).dropDuplicates()

    dim_location.write.mode("overwrite").parquet(os.path.join(output_dir, "dim_location"))

    # --- TRIP FACT ---
    trip_fact = df.select(
        col("tpep_pickup_datetime").cast("long").alias("datetime_id"),
        col("PULocationID").alias("pickup_location_id"),
        col("DOLocationID").alias("dropoff_location_id"),
        col("passenger_count"),
        col("trip_distance"),
        col("fare_amount"),
        col("tip_amount"),
        col("total_amount")
    )

    trip_fact.write.mode("overwrite").parquet(os.path.join(output_dir, "trip_fact"))
    print("Saving trip_fact to:", os.path.join(output_dir, "trip_fact"))

    print("✅ Dimension & Fact tables written to:", output_dir)

if __name__ == "__main__":
    input_parquet = os.path.join("output", "clean_tripdata.parquet")
    output_dir = os.path.join("output", "dim_fact")
    build_dimensions_and_fact(input_parquet, output_dir)
