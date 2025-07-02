# scripts/transform_tripdata.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

def get_spark_session(app_name="NYC Taxi Transform"):
    return SparkSession.builder.appName(app_name).config("spark.sql.shuffle.partitions","4").getOrCreate()

def transform_tripdata(spark,input_csv,output_parquet):
    df = spark.read.option("header",True).csv(input_csv)

    df_clean = df.dropna() \
        .withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))) \
        .withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime"))) \
        .withColumn("passenger_count", col("passenger_count").cast("int")) \
        .withColumn("trip_distance", col("trip_distance").cast("float")) \
        .withColumn("fare_amount", col("fare_amount").cast("float")) \
        .withColumn("tip_amount", col("tip_amount").cast("float")) \
        .withColumn("PULocationID", col("PULocationID").cast("int")) \
        .withColumn("DOLocationID", col("DOLocationID").cast("int")) \
        .withColumn("total_amount", col("fare_amount")+col("tip_amount"))


    df_clean.write.mode("overwrite").parquet(output_parquet)
    print(f"✅ Transformed data written to: {output_parquet}")
