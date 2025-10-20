import time
import psycopg2

REFRESH_INTERVAL = 60

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
    'signups_by_region_device'
]

while True:
    for view in views:
        print(f"Refreshing view: {view};")
        cur.execute(f"REFRESH MATERIALIZED VIEW {view}")
    conn.commit()
    print("All refreshed. Waiting for the next cycle...")
    time.sleep(REFRESH_INTERVAL)