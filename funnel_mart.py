from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, lit, round as spark_round

SILVER_PATH = "data/silver/clean_events"
GOLD_PATH = "data/gold/mart_funnel_stats"

spark = (
    SparkSession.builder
    .appName("BuildFunnelMart")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(SILVER_PATH)

# 1. 단계별 일자별 유저 수 집계
view_df = (
    df.filter(col("event_type") == "view")
    .groupBy("event_date")
    .agg(countDistinct("user_id").alias("user_count"))
    .withColumn("step_name", lit("view"))
)

search_df = (
    df.filter(col("event_type") == "search")
    .groupBy("event_date")
    .agg(countDistinct("user_id").alias("user_count"))
    .withColumn("step_name", lit("search"))
)

cart_df = (
    df.filter(col("event_type") == "add_to_cart")
    .groupBy("event_date")
    .agg(countDistinct("user_id").alias("user_count"))
    .withColumn("step_name", lit("add_to_cart"))
)

purchase_df = (
    df.filter(col("event_type") == "purchase")
    .groupBy("event_date")
    .agg(countDistinct("user_id").alias("user_count"))
    .withColumn("step_name", lit("purchase"))
)

funnel_df = view_df.unionByName(search_df).unionByName(cart_df).unionByName(purchase_df)

# 2. 이전 단계 유저 수 연결
view_base = view_df.select(
    col("event_date"),
    col("user_count").alias("view_users")
)

search_base = search_df.select(
    col("event_date"),
    col("user_count").alias("search_users")
)

cart_base = cart_df.select(
    col("event_date"),
    col("user_count").alias("cart_users")
)

result_df = (
    funnel_df
    .join(view_base, on="event_date", how="left")
    .join(search_base, on="event_date", how="left")
    .join(cart_base, on="event_date", how="left")
)

result_df = (
    result_df
    .withColumn(
        "prev_step_user_count",
        col("user_count")  # 임시값
    )
)

result_df = (
    result_df
    .withColumn(
        "prev_step_user_count",
        col("view_users")
    )
)

result_df = (
    result_df
    .withColumn(
        "prev_step_user_count",
        col("prev_step_user_count")
    )
)

result_df = (
    result_df
    .withColumn(
        "prev_step_user_count",
        col("prev_step_user_count")
    )
)

# step별 이전 단계 매핑
result_df = result_df.withColumn(
    "prev_step_user_count",
    col("prev_step_user_count")
)

result_df = (
    result_df
    .withColumn(
        "prev_step_user_count",
        col("view_users")
    )
)

from pyspark.sql.functions import when

result_df = (
    result_df
    .withColumn(
        "prev_step_user_count",
        when(col("step_name") == "view", None)
        .when(col("step_name") == "search", col("view_users"))
        .when(col("step_name") == "add_to_cart", col("search_users"))
        .when(col("step_name") == "purchase", col("cart_users"))
    )
    .withColumn(
        "conversion_rate_from_prev",
        when(col("step_name") == "view", None)
        .otherwise(
            spark_round((col("user_count") / col("prev_step_user_count")) * 100, 2)
        )
    )
    .select(
        "event_date",
        "step_name",
        "user_count",
        "prev_step_user_count",
        "conversion_rate_from_prev"
    )
)

(
    result_df
    .write
    .mode("overwrite")
    .partitionBy("event_date")
    .parquet(GOLD_PATH)
)

print("✅ Funnel mart 저장 완료:", GOLD_PATH)
result_df.orderBy("event_date", "step_name").show(truncate=False)

spark.stop()