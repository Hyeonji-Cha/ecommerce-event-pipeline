from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CheckFunnelMart")
    .master("local[1]")
    .getOrCreate()
)

df = spark.read.parquet("data/gold/mart_funnel_stats")

df.orderBy("event_date", "step_name").show(50, truncate=False)
df.printSchema()

spark.stop()