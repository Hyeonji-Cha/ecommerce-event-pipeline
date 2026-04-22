from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    lag,
    lit,
    when,
    unix_timestamp,
    format_string,
    concat_ws,
    sha2,
    current_timestamp,
)

SILVER_PATH = "data/silver/clean_events"
GOLD_PATH = "data/gold/mart_fraud_alerts"

SHORT_TIME_SEC = 300          # 10분
HIGH_AMOUNT_THRESHOLD = 480.0

spark = (
    SparkSession.builder
    .appName("BuildFraudMart")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(SILVER_PATH)

purchase_df = df.filter(col("event_type") == "purchase")

window_spec = Window.partitionBy("user_id").orderBy("event_time")

base_df = (
    purchase_df
    .withColumn("prev_purchase_time", lag("event_time").over(window_spec))
    .withColumn("prev_country", lag("country").over(window_spec))
    .withColumn(
        "seconds_from_prev_purchase",
        unix_timestamp(col("event_time")) - unix_timestamp(col("prev_purchase_time"))
    )
)

# 1) 짧은 시간 내 다중 결제
short_time_df = (
    base_df
    .filter(col("prev_purchase_time").isNotNull())
    .filter(col("seconds_from_prev_purchase") <= SHORT_TIME_SEC)
    .select(
        col("event_date"),
        col("event_time"),
        col("event_id"),
        col("user_id"),
        col("session_id"),
        lit("multiple_purchase_short_time").alias("rule_name"),
        when(col("seconds_from_prev_purchase") <= 120, lit("high"))
        .otherwise(lit("medium"))
        .alias("severity"),
        format_string(
            "previous_purchase_gap_sec=%d",
            col("seconds_from_prev_purchase")
        ).alias("evidence"),
        col("amount"),
        col("country"),
    )
)

# 2) 고액 결제
high_amount_df = (
    base_df
    .filter(col("amount") >= HIGH_AMOUNT_THRESHOLD)
    .select(
        col("event_date"),
        col("event_time"),
        col("event_id"),
        col("user_id"),
        col("session_id"),
        lit("high_amount_purchase").alias("rule_name"),
        when(col("amount") >= 480, lit("high"))
        .otherwise(lit("medium"))
        .alias("severity"),
        format_string(
            "amount=%.2f threshold=%.2f",
            col("amount"),
            lit(HIGH_AMOUNT_THRESHOLD)
        ).alias("evidence"),
        col("amount"),
        col("country"),
    )
)

# 3) 국가 변경 결제
country_change_df = (
    base_df
    .filter(col("prev_country").isNotNull())
    .filter(col("country") != col("prev_country"))
    .select(
        col("event_date"),
        col("event_time"),
        col("event_id"),
        col("user_id"),
        col("session_id"),
        lit("country_changed_purchase").alias("rule_name"),
        lit("high").alias("severity"),
        format_string(
            "prev_country=%s current_country=%s",
            col("prev_country"),
            col("country")
        ).alias("evidence"),
        col("amount"),
        col("country"),
    )
)

alerts_df = short_time_df.unionByName(high_amount_df).unionByName(country_change_df)

result_df = (
    alerts_df
    .withColumn(
        "alert_id",
        sha2(
            concat_ws(
                "||",
                col("event_id"),
                col("rule_name"),
                col("event_time").cast("string")
            ),
            256
        )
    )
    .withColumn("detected_at", current_timestamp())
    .select(
        "alert_id",
        "event_date",
        "event_time",
        "event_id",
        "user_id",
        "session_id",
        "rule_name",
        "severity",
        "evidence",
        "amount",
        "country",
        "detected_at",
    )
)

(
    result_df
    .write
    .mode("overwrite")
    .partitionBy("event_date")
    .parquet(GOLD_PATH)
)

print("✅ Fraud mart 저장 완료:", GOLD_PATH)
result_df.orderBy("event_date", "event_time").show(50, truncate=False)

spark.stop()