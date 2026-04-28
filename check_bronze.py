from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

RUN_ID = os.environ["RUN_ID"]
BASE_PATH = f"data/runs/{RUN_ID}"


spark = (
    SparkSession.builder
    .appName("CheckBronzeData")
    .master("local[1]")
    .getOrCreate()
)

df = spark.read.parquet(f"{BASE_PATH}/bronze/raw_events")

df.orderBy(col("kafka_timestamp").desc(), col("offset").desc()).show(20, truncate=False)
df.printSchema()

spark.stop()