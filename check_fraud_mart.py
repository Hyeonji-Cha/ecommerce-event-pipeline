from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CheckFraudMart")
    .master("local[1]")
    .getOrCreate()
)

df = spark.read.parquet("data/gold/mart_fraud_alerts")

df.orderBy("event_date", "event_time").show(50, truncate=False)
df.printSchema()

spark.stop()