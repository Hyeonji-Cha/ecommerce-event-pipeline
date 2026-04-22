from pyspark.sql import SparkSession
from pyspark.sql.functions import col

BRONZE_PATH = "data/bronze/raw_events"   

spark = (
    SparkSession.builder
    .appName("CheckBronzeData")
    .master("local[1]")
    .getOrCreate()
)

df = spark.read.parquet(BRONZE_PATH)

df.orderBy(col("kafka_timestamp").desc(), col("offset").desc()).show(20, truncate=False)
df.printSchema()

spark.stop()