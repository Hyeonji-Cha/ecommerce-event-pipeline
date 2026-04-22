from pyspark.sql import SparkSession
from pyspark.sql.functions import col

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "raw-events"

BRONZE_OUTPUT_PATH = "data/bronze/raw_events"
BRONZE_CHECKPOINT_PATH = "data/checkpoints/bronze_raw_events"

spark = (
    SparkSession.builder
    .appName("EcommerceBronzeIngestion")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

bronze_df = (
    raw_df.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("key").cast("string").alias("message_key"),
        col("value").cast("string").alias("raw_json")
    )
)

query = (
    bronze_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", BRONZE_OUTPUT_PATH)
    .option("checkpointLocation", BRONZE_CHECKPOINT_PATH)
    .start()
)

query.awaitTermination()