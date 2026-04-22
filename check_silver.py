from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CheckSilverData")
    .master("local[1]")
    .getOrCreate()
)

df = spark.read.parquet("data/silver/clean_events")

df.show(20, truncate=False)
df.printSchema()

spark.stop()