# scripts/data_quality_check.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, count, concat_ws
import os
import json
import datetime

def get_spark_session(app_name="Data Quality Check"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def check_data_quality(parquet_input_path, log_output_dir):
    spark = get_spark_session()
    df = spark.read.parquet(parquet_input_path)

    result = {
        "summary": {},
        "null_check": {},
        "range_check": {},
        "value_check": {},
        "uniqueness_check": {}
    }

    # --- Summary ---
    result["summary"]["total_rows"] = df.count()
    result["summary"]["total_columns"] = len(df.columns)
    result["summary"]["generated_at"] = datetime.datetime.now().isoformat()

    # --- Null / NaN check ---
    for column in df.columns:
        if df.schema[column].dataType.simpleString() in ["float", "double"]:
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
        else:
            null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            result["null_check"][column] = null_count

    # --- Range check ---
    result["range_check"]["passenger_count"] = df.filter((col("passenger_count") < 1) | (col("passenger_count") > 6)).count()
    result["range_check"]["trip_distance"] = df.filter(col("trip_distance") < 0).count()

    # --- Value check ---
    allowed_payment_types = ["Credit card", "Cash", "No charge", "Dispute", "Unknown", "Voided trip"]
    invalid_payment_type_count = df.filter(~col("payment_type").isin(allowed_payment_types)).count()
    result["value_check"]["payment_type_invalid_count"] = invalid_payment_type_count

    # --- Uniqueness check ---
    df_with_key = df.withColumn("composite_key", concat_ws("_", col("tpep_pickup_datetime").cast("string"), col("PULocationID").cast("string")))
    total_keys = df_with_key.count()
    distinct_keys = df_with_key.select("composite_key").distinct().count()
    result["uniqueness_check"]["duplicate_composite_keys"] = total_keys - distinct_keys

    # --- Write result ---
    os.makedirs(log_output_dir, exist_ok=True)
    output_path = os.path.join(log_output_dir, "data_quality_report.json")
    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"✅ Data quality report written to {output_path}")

if __name__ == "__main__":
    parquet_input_path = os.path.join("output", "clean_tripdata.parquet")
    log_output_dir = os.path.join("logs", "quality")
    check_data_quality(parquet_input_path, log_output_dir)