import os
from pathlib import Path
from datetime import datetime, timedelta
import duckdb

RUNS_ROOT = Path("data/runs")
EXPORT_DIR = "exports/tableau_runs"

# 예: export START_SNAPSHOT_DATE=2026-04-20, 날짜처럼 보이게 배치하는 기준 날짜 
START_SNAPSHOT_DATE = os.environ.get("START_SNAPSHOT_DATE", "2026-04-20")
TARGET_RUN_IDS = [
    "20260426_03",
    "20260426_06",
    "20260426_07",
    "20260426_08",
    "20260427_02",
    "20260427_03",
    "20260427_04",
]
os.makedirs(EXPORT_DIR, exist_ok=True)

con = duckdb.connect()

run_dirs = sorted([
    p for p in RUNS_ROOT.iterdir()
    if p.is_dir() and p.name in TARGET_RUN_IDS
])
if not run_dirs:
    raise RuntimeError("data/runs 아래에 run 디렉토리가 없습니다.")

start_date = datetime.strptime(START_SNAPSHOT_DATE, "%Y-%m-%d").date()

funnel_sources = []
fraud_sources = []
run_date_mapping = []

for idx, run_dir in enumerate(run_dirs):
    run_id = run_dir.name
    snapshot_date = start_date + timedelta(days=idx)
    snapshot_date_str = snapshot_date.isoformat()

    funnel_dir = run_dir / "gold" / "mart_funnel_stats"
    fraud_dir = run_dir / "gold" / "mart_fraud_alerts"

    funnel_glob = str(funnel_dir / "*" / "*.parquet")
    fraud_glob = str(fraud_dir / "*" / "*.parquet")

    if funnel_dir.exists():
        funnel_sources.append((run_id, snapshot_date_str, funnel_glob))

    if fraud_dir.exists():
        fraud_sources.append((run_id, snapshot_date_str, fraud_glob))

    run_date_mapping.append((run_id, snapshot_date_str))


if not funnel_sources:
    raise RuntimeError("비교 가능한 funnel run이 없습니다.")

if not fraud_sources:
    raise RuntimeError("비교 가능한 fraud run이 없습니다.")


def build_union_sql(sources):
    parts = []
    for run_id, snapshot_date, path_glob in sources:
        parts.append(f"""
        SELECT
            '{run_id}' AS run_id,
            DATE '{snapshot_date}' AS snapshot_date,
            *
        FROM read_parquet('{path_glob}', hive_partitioning = true)
        """)
    return "\nUNION ALL\n".join(parts)


funnel_union_sql = build_union_sql(funnel_sources)
fraud_union_sql = build_union_sql(fraud_sources)

# 0) run_id ↔ snapshot_date 매핑도 같이 저장
mapping_csv_path = f"{EXPORT_DIR}/run_snapshot_mapping.csv"
with open(mapping_csv_path, "w", encoding="utf-8") as f:
    f.write("run_id,snapshot_date\n")
    for run_id, snapshot_date in run_date_mapping:
        f.write(f"{run_id},{snapshot_date}\n")

# 1) run별 funnel export
query_funnel = f"""
COPY (
    WITH funnel AS (
        {funnel_union_sql}
    )
    SELECT
        run_id,
        snapshot_date,
        event_date,
        step_name,
        user_count,
        prev_step_user_count,
        conversion_rate_from_prev
    FROM funnel
    ORDER BY
        snapshot_date,
        run_id,
        CASE step_name
            WHEN 'view' THEN 1
            WHEN 'search' THEN 2
            WHEN 'add_to_cart' THEN 3
            WHEN 'purchase' THEN 4
            ELSE 99
        END
) TO '{EXPORT_DIR}/funnel_runs.csv' (HEADER, DELIMITER ',');
"""

# 2) run별 fraud rule summary export
query_fraud_rule_summary = f"""
COPY (
    WITH alerts AS (
        {fraud_union_sql}
    )
    SELECT
        run_id,
        snapshot_date,
        event_date,
        rule_name,
        COUNT(*) AS alert_count,
        COUNT(DISTINCT event_id) AS purchase_events,
        COUNT(DISTINCT user_id) AS users,
        ROUND(SUM(COALESCE(amount, 0)), 2) AS total_amount
    FROM alerts
    GROUP BY 1, 2, 3, 4
    ORDER BY snapshot_date, run_id, alert_count DESC, rule_name
) TO '{EXPORT_DIR}/fraud_rule_summary_runs.csv' (HEADER, DELIMITER ',');
"""

# 3) run별 suspicious users top 5 export
query_suspicious_users = f"""
COPY (
    WITH alerts AS (
        {fraud_union_sql}
    ),
    scored AS (
        SELECT
            run_id,
            snapshot_date,
            event_date,
            user_id,
            COUNT(*) AS alert_count,
            COUNT(DISTINCT rule_name) AS rule_diversity,
            SUM(CASE WHEN severity = 'high' THEN 2 ELSE 1 END) AS severity_score,
            ROUND(SUM(COALESCE(amount, 0)), 2) AS total_amount
        FROM alerts
        GROUP BY 1, 2, 3, 4
    ),
    ranked AS (
        SELECT
            *,
            DENSE_RANK() OVER (
                PARTITION BY run_id
                ORDER BY severity_score DESC, alert_count DESC, total_amount DESC
            ) AS risk_rank
        FROM scored
    )
    SELECT
        run_id,
        snapshot_date,
        event_date,
        risk_rank,
        user_id,
        alert_count,
        rule_diversity,
        severity_score,
        total_amount
    FROM ranked
    WHERE risk_rank <= 5
    ORDER BY snapshot_date, run_id, risk_rank, user_id
) TO '{EXPORT_DIR}/suspicious_users_top5_runs.csv' (HEADER, DELIMITER ',');
"""

con.execute(query_funnel)
con.execute(query_fraud_rule_summary)
con.execute(query_suspicious_users)

print("✅ RUN별 Tableau export 완료 (snapshot_date 포함)")
print(f"- {EXPORT_DIR}/run_snapshot_mapping.csv")
print(f"- {EXPORT_DIR}/funnel_runs.csv")
print(f"- {EXPORT_DIR}/fraud_rule_summary_runs.csv")
print(f"- {EXPORT_DIR}/suspicious_users_top5_runs.csv")

con.close()