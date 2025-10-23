import time
import psycopg2

REFRESH_INTERVAL = 8

conn = psycopg2.connect(
    host = "localhost",
    port = 5432,
    dbname = 'churn_db',
    user = 'admin',
    password = 'admin'
)
cur = conn.cursor()

views = [
    'daily_signups_summary',
    'plan_distribution_summary',
    'signups_by_region_device',
    'daily_active_users',
    'avg_session_length_per_plan',
    'support_summary',
    'daily_revenue_summary',
    'customer_lifetime_value',
    'weekly_retention',
    'daily_arpu',
    'support_load',
    'churn_summary',
    'churned_user_activity',
    'churned_billing_summary',
    'churned_support_summary',
    'churn_insights'
]

while True:
    for view in views:
        print(f"Refreshing view: {view};")
        cur.execute(f"REFRESH MATERIALIZED VIEW {view}")
    conn.commit()
    print("All refreshed. Waiting for the next cycle...")
    time.sleep(REFRESH_INTERVAL)