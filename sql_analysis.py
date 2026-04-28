import os
import duckdb

RUN_ID = os.environ["RUN_ID"]
BASE_PATH = f"data/runs/{RUN_ID}"

FUNNEL_PATH = f"{BASE_PATH}/gold/mart_funnel_stats/*/*.parquet"
FRAUD_PATH = f"{BASE_PATH}/gold/mart_fraud_alerts/*/*.parquet"


def print_result(title, result):
    columns = [desc[0] for desc in result.description]
    rows = result.fetchall()

    print("\n" + "=" * 110)
    print(title)
    print("=" * 110)
    print(" | ".join(columns))
    print("-" * 110)

    if not rows:
        print("(no rows)")
        return

    for row in rows:
        print(" | ".join(str(v) for v in row))


con = duckdb.connect()

# 1) 최신 날짜 funnel 요약
query_funnel = f"""
WITH funnel AS (
    SELECT *
    FROM read_parquet('{FUNNEL_PATH}', hive_partitioning = true)
),
latest_date AS (
    SELECT MAX(event_date) AS dt
    FROM funnel
)
SELECT
    event_date,
    step_name,
    user_count,
    prev_step_user_count,
    conversion_rate_from_prev
FROM funnel
WHERE event_date = (SELECT dt FROM latest_date)
ORDER BY
    CASE step_name
        WHEN 'view' THEN 1
        WHEN 'search' THEN 2
        WHEN 'add_to_cart' THEN 3
        WHEN 'purchase' THEN 4
        ELSE 99
    END;
"""

# 2) 최신 날짜 fraud rule 요약
query_fraud_summary = f"""
WITH alerts AS (
    SELECT *
    FROM read_parquet('{FRAUD_PATH}', hive_partitioning = true)
),
latest_date AS (
    SELECT MAX(event_date) AS dt
    FROM alerts
)
SELECT
    event_date,
    rule_name,
    COUNT(*) AS alert_count,
    COUNT(DISTINCT event_id) AS purchase_events,
    COUNT(DISTINCT user_id) AS users,
    ROUND(SUM(COALESCE(amount, 0)), 2) AS total_amount
FROM alerts
WHERE event_date = (SELECT dt FROM latest_date)
GROUP BY 1, 2
ORDER BY alert_count DESC, rule_name;
"""

# 3) 최신 날짜 suspicious users TOP 5
query_top_users = f"""
WITH alerts AS (
    SELECT *
    FROM read_parquet('{FRAUD_PATH}', hive_partitioning = true)
),
latest_date AS (
    SELECT MAX(event_date) AS dt
    FROM alerts
),
scored AS (
    SELECT
        event_date,
        user_id,
        COUNT(*) AS alert_count,
        COUNT(DISTINCT rule_name) AS rule_diversity,
        SUM(CASE WHEN severity = 'high' THEN 2 ELSE 1 END) AS severity_score,
        ROUND(SUM(COALESCE(amount, 0)), 2) AS total_amount
    FROM alerts
    WHERE event_date = (SELECT dt FROM latest_date)
    GROUP BY 1, 2
),
ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY event_date
            ORDER BY severity_score DESC, alert_count DESC, total_amount DESC
        ) AS risk_rank
    FROM scored
)
SELECT
    event_date,
    risk_rank,
    user_id,
    alert_count,
    rule_diversity,
    severity_score,
    total_amount
FROM ranked
WHERE risk_rank <= 5
ORDER BY risk_rank, user_id;
"""

print_result("1) Latest Funnel Summary", con.execute(query_funnel))
print_result("2) Latest Fraud Rule Summary", con.execute(query_fraud_summary))
print_result("3) Latest Suspicious Users TOP 5", con.execute(query_top_users))

con.close()