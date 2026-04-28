from pyspark.sql import SparkSession
import os

RUN_ID = os.environ["RUN_ID"]
BASE_PATH = f"data/runs/{RUN_ID}"

spark = (
    SparkSession.builder
    .appName("CheckSilverData")
    .master("local[1]")
    .getOrCreate()
)

df = spark.read.parquet(f"{BASE_PATH}/silver/clean_events")

df.show(20, truncate=False)
df.printSchema()

spark.stop()