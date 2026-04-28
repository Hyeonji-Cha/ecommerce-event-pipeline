from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    to_date,
    row_number
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)
import os

RUN_ID = os.environ["RUN_ID"]
BASE_PATH = f"data/runs/{RUN_ID}"

BRONZE_PATH = f"{BASE_PATH}/bronze/raw_events"
SILVER_PATH = f"{BASE_PATH}/silver/clean_events"

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("search_query", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("country", StringType(), True),
])

VALID_EVENT_TYPES = ["view", "search", "add_to_cart", "purchase"]

spark = (
    SparkSession.builder
    .appName("BronzeToSilverTransform")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# 1. Bronze 읽기
bronze_df = spark.read.parquet(BRONZE_PATH)

# 2. raw_json 파싱
parsed_df = (
    bronze_df
    .select(
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("kafka_timestamp"),
        from_json(col("raw_json"), EVENT_SCHEMA).alias("event")
    )
    .select(
        col("event.event_id").alias("event_id"),
        to_timestamp(col("event.event_time")).alias("event_time"),
        col("event.event_type").alias("event_type"),
        col("event.user_id").alias("user_id"),
        col("event.session_id").alias("session_id"),
        col("event.product_id").alias("product_id"),
        col("event.search_query").alias("search_query"),
        col("event.amount").alias("amount"),
        col("event.country").alias("country"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
    )
)

# 3. 기본 정제
clean_df = (
    parsed_df
    .filter(col("event_id").isNotNull())
    .filter(col("event_time").isNotNull())
    .filter(col("event_type").isin(VALID_EVENT_TYPES))
    .filter(col("user_id").isNotNull())
    .filter(col("session_id").isNotNull())
    .withColumn("event_date", to_date(col("event_time")))
)

# 4. 중복 제거 (같은 event_id가 여러 번 있으면 가장 최신 Kafka offset만 유지)
window_spec = Window.partitionBy("event_id").orderBy(col("kafka_offset").desc())

dedup_df = (
    clean_df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# 5. Silver 저장
(
    dedup_df
    .write
    .mode("overwrite")
    .partitionBy("event_date")
    .parquet(SILVER_PATH)
)

print("✅ Silver 저장 완료:", SILVER_PATH)
print("총 row 수:", dedup_df.count())

spark.stop()