from pyspark.sql import SparkSession
import os

def get_spark_session(app_name="NYC Taxi Project") -> SparkSession:
    # ✅ 强制设置环境变量（用于 override PySpark 默认行为）
    os.environ["SPARK_HOME"] = "/opt/homebrew/opt/apache-spark/libexec"
    os.environ["PYSPARK_PYTHON"] = os.environ.get("VIRTUAL_ENV", "") + "/bin/python"

    # 获取当前目录下 jars 路径
    jars_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "jars")
    jdbc_jar = os.path.join(jars_dir, "postgresql-42.7.7.jar")

    # 构建 SparkSession
    spark = SparkSession.builder.appName(app_name).config("spark.sql.shuffle.partitions","4").config("spark.jars", jdbc_jar).getOrCreate()
    return spark