from datetime import datetime, timedelta
import time
from signups_producer import produce_signups
from activity_producer import produce_activity
from support_producer import produce_support
from billing_producer import produce_billing
from churn_updater import update_churn

TOTAL_SIMULATION_DAYS = 365
START_DATE = datetime(2025, 1, 1)

customers = []

for day_index in range(TOTAL_SIMULATION_DAYS):
    simulated_day = START_DATE + timedelta(days=day_index)
    print(f"[Day {day_index+1}] {simulated_day.date()}")

    # Signups
    produce_signups(simulated_day)
    time.sleep(2)
    # Update target-churn
    update_churn(simulated_day)

    # Create activity
    produce_activity(simulated_day)

    # Create support
    produce_support(simulated_day)

    # Create billing
    produce_billing(simulated_day)

    time.sleep(6)