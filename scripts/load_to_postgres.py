import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),"..")))

import psycopg
from psycopg.rows import dict_row
from utils.spark_session import get_spark_session

# PostgreSQL Conn_params
conn_params = {
    "host": "localhost",
    "port": 5432,
    "dbname": "nyc_taxi",
    "user": "postgres",
    "password": '000000'
}


# ✅ 测试连接函数（可选）
def test_connection():
    try:
        with psycopg.connect(**conn_params, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                result = cur.fetchone()
                print("✅ PostgreSQL Connected! Version:", result["version"])
    except Exception as e:
        print("❌ Connection failed:",e)

# ✅ Spark → PostgreSQL 写入函数
def load_parquet_to_postgres(parquet_path:str, table_name:str):
    spark = get_spark_session("Spark -> PostgreSQL Loader")

    # Read parquet
    df = spark.read.parquet(parquet_path)

    # Set JDBC Driver JAR Path
    spark._jsc.hadoopConfiguration().set(
        "spark.driver.extraClassPath",
        "jars/postgresql-42.7.1.jar"
    )

    # Write into PostgresSQL

    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{conn_params['host']}:{conn_params['port']}/{conn_params['dbname']}") \
        .option("dbtable", table_name) \
        .option("user", conn_params["user"]) \
        .option("password", conn_params["password"]) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print(f"✅ Successfully loaded data to table: {table_name}")

# Example usage
if __name__=="__main__":
    test_connection()
    load_parquet_to_postgres("output/clean_tripdata.parquet", "trip_fact")



