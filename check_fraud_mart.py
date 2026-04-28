from pyspark.sql import SparkSession
import os

RUN_ID = os.environ["RUN_ID"]
BASE_PATH = f"data/runs/{RUN_ID}"

spark = (
    SparkSession.builder
    .appName("CheckFraudMart")
    .master("local[1]")
    .getOrCreate()
)

df = spark.read.parquet(f"{BASE_PATH}/gold/mart_fraud_alerts")
df.orderBy("event_date", "event_time").show(50, truncate=False)
df.printSchema()

spark.stop()