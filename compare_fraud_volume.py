import duckdb

SILVER_PATH = "data/silver/clean_events/*/*.parquet"
FRAUD_PATH = "data/gold/mart_fraud_alerts/*/*.parquet"

con = duckdb.connect()

query = f"""
WITH purchases AS (
    SELECT event_date, event_id, user_id, amount
    FROM read_parquet('{SILVER_PATH}', hive_partitioning = true)
    WHERE event_type = 'purchase'
),
alerts AS (
    SELECT event_date, event_id, user_id, rule_name
    FROM read_parquet('{FRAUD_PATH}', hive_partitioning = true)
),
latest_date AS (
    SELECT MAX(event_date) AS dt FROM purchases
),
purchase_stats AS (
    SELECT
        event_date,
        COUNT(*) AS purchase_count,
        COUNT(DISTINCT user_id) AS purchase_users,
        ROUND(SUM(amount), 2) AS total_purchase_amount
    FROM purchases
    WHERE event_date = (SELECT dt FROM latest_date)
    GROUP BY 1
),
alert_stats AS (
    SELECT
        event_date,
        COUNT(*) AS alert_count,
        COUNT(DISTINCT event_id) AS alerted_purchase_count,
        COUNT(DISTINCT user_id) AS alerted_users
    FROM alerts
    WHERE event_date = (SELECT dt FROM latest_date)
    GROUP BY 1
)
SELECT
    p.event_date,
    p.purchase_count,
    p.purchase_users,
    p.total_purchase_amount,
    COALESCE(a.alert_count, 0) AS alert_count,
    COALESCE(a.alerted_purchase_count, 0) AS alerted_purchase_count,
    COALESCE(a.alerted_users, 0) AS alerted_users,
    ROUND(COALESCE(a.alert_count, 0) * 100.0 / NULLIF(p.purchase_count, 0), 2) AS alert_per_purchase_pct,
    ROUND(COALESCE(a.alerted_purchase_count, 0) * 100.0 / NULLIF(p.purchase_count, 0), 2) AS alerted_purchase_pct
FROM purchase_stats p
LEFT JOIN alert_stats a
ON p.event_date = a.event_date;
"""

result = con.execute(query)
print(result.fetchall())
print([d[0] for d in result.description])

query_rule = f"""
WITH alerts AS (
    SELECT *
    FROM read_parquet('{FRAUD_PATH}', hive_partitioning = true)
),
latest_date AS (
    SELECT MAX(event_date) AS dt FROM alerts
)
SELECT
    event_date,
    rule_name,
    COUNT(*) AS alert_count,
    COUNT(DISTINCT event_id) AS purchase_events,
    COUNT(DISTINCT user_id) AS users
FROM alerts
WHERE event_date = (SELECT dt FROM latest_date)
GROUP BY 1, 2
ORDER BY alert_count DESC, rule_name;
"""

result2 = con.execute(query_rule)
print(result2.fetchall())
print([d[0] for d in result2.description])

con.close()