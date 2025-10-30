import time
import psycopg2
from psycopg2 import sql

REFRESH_INTERVAL = 8

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname='churn_db',
    user='admin',
    password='admin'
)
conn.autocommit = False

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

def refresh_views():
    cur = conn.cursor()
    failed_views = []
    
    for view in views:
        try:
            print(f"Refreshing view: {view}")
            cur.execute(sql.SQL("REFRESH MATERIALIZED VIEW {}").format(sql.Identifier(view)))
            conn.commit()
        except psycopg2.errors.DivisionByZero as e:
            conn.rollback()
            print(f"❌ ERROR in {view}: Division by zero - {e}")
            failed_views.append((view, "DivisionByZero", str(e)))
        except Exception as e:
            conn.rollback()
            print(f"❌ ERROR in {view}: {type(e).__name__} - {e}")
            failed_views.append((view, type(e).__name__, str(e)))
    
    cur.close()
    
    if failed_views:
        print("\n⚠️ FAILED VIEWS:")
        for view, error_type, error_msg in failed_views:
            print(f"  - {view}: {error_type}")
        return False
    else:
        print("✅ All views refreshed successfully")
        return True

# Primera ejecución con diagnóstico detallado
print("=" * 60)
print("INITIAL REFRESH - DIAGNOSING ISSUES")
print("=" * 60)

try:
    refresh_views()
except Exception as e:
    print(f"Critical error: {e}")

print("\n" + "=" * 60)
print("Starting refresh loop...")
print("=" * 60 + "\n")

# Loop continuo
while True:
    try:
        refresh_views()
        print(f"Waiting {REFRESH_INTERVAL} seconds...\n")
        time.sleep(REFRESH_INTERVAL)
    except KeyboardInterrupt:
        print("\n🛑 Stopping refresh process...")
        break
    except Exception as e:
        print(f"Unexpected error: {e}")
        time.sleep(REFRESH_INTERVAL)

conn.close()