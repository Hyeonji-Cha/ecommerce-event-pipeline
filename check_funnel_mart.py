from pyspark.sql import SparkSession
import os

RUN_ID = os.environ["RUN_ID"]
BASE_PATH = f"data/runs/{RUN_ID}"

spark = (
    SparkSession.builder
    .appName("CheckFunnelMart")
    .master("local[1]")
    .getOrCreate()
)

df = spark.read.parquet(f"{BASE_PATH}/gold/mart_funnel_stats")
df.orderBy("event_date", "step_name").show(50, truncate=False)
df.printSchema()

spark.stop()