# scripts/run_pipeline.py

import os
from transform_tripdata import transform_tripdata, get_spark_session

def run_pipeline():
    spark = get_spark_session("NYC Taxi Pipeline")
    input_csv = os.path.join("data", "yellow_tripdata_sample.csv")
    output_parquet = os.path.join("output", "clean_tripdata.parquet")


    print("🚀 Starting NYC Taxi Data Pipeline ...")
    transform_tripdata(spark,input_csv, output_parquet)
    print("✅ Pipeline finished.")

if __name__ == '__main__':
    run_pipeline()




