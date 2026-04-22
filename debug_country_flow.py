import duckdb

SILVER_PATH = "data/silver/clean_events/*/*.parquet"

con = duckdb.connect()

query = f"""
WITH purchases AS (
    SELECT
        event_date,
        event_time,
        event_id,
        user_id,
        session_id,
        amount,
        country,
        LAG(country) OVER (
            PARTITION BY user_id
            ORDER BY event_time
        ) AS prev_country
    FROM read_parquet('{SILVER_PATH}', hive_partitioning = true)
    WHERE event_type = 'purchase'
),
latest_date AS (
    SELECT MAX(event_date) AS dt
    FROM purchases
)
SELECT
    event_date,
    event_time,
    user_id,
    session_id,
    prev_country,
    country AS current_country,
    amount
FROM purchases
WHERE event_date = (SELECT dt FROM latest_date)
  AND prev_country IS NOT NULL
  AND prev_country <> country
ORDER BY user_id, event_time
LIMIT 20;
"""

result = con.execute(query)
print([d[0] for d in result.description])
for row in result.fetchall():
    print(row)

con.close()