import uuid
import random
import time
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# ---------- PARAMETERS ----------
BATCH_SIZE = 10        # how many customers per batch
SLEEP_TIME = 5         # seconds between batches (simulates streaming)
START_DATE = datetime(2024, 1, 1)
CURRENT_DATE = datetime.now()

# Drift parameters (will change over time)
plan_distribution = {
    "2024": {"free": 0.6, "basic": 0.3, "premium": 0.1},
    "2025": {"free": 0.4, "basic": 0.4, "premium": 0.2}
}

# ---------- FUNCTIONS ----------
def generate_customer(signup_date: datetime):
    """Generate a single synthetic customer record with churn logic."""

    # Plan type drift based on year
    year = str(signup_date.year)
    plans = list(plan_distribution[year].keys())
    probs = list(plan_distribution[year].values())
    plan_type = np.random.choice(plans, p=probs)

    # Monthly spend depends on plan
    if plan_type == "free":
        monthly_spend = 0
    elif plan_type == "basic":
        monthly_spend = round(np.random.normal(20, 5), 2)
    else:  # premium
        monthly_spend = round(np.random.normal(50, 10), 2)

    # Support tickets (Poisson distribution)
    support_tickets = np.random.poisson(1 if plan_type != "premium" else 0.5)

    # Churn probability depends on plan + tickets
    churn_prob = 0.2 if plan_type == "free" else 0.1 if plan_type == "basic" else 0.05
    churn_prob += min(0.3, 0.05 * support_tickets)  # more tickets → higher churn risk
    churned = int(np.random.rand() < churn_prob)

    # Last active date (closer to signup if churned)
    if churned:
        last_active = signup_date + timedelta(days=np.random.randint(30, 180))
    else:
        last_active = CURRENT_DATE - timedelta(days=np.random.randint(1, 30))

    return {
        "customer_id": str(uuid.uuid4()),
        "signup_date": signup_date.date().isoformat(),
        "last_active_date": last_active.date().isoformat(),
        "plan_type": plan_type,
        "monthly_spend": monthly_spend,
        "support_tickets": support_tickets,
        "churned": churned
    }

def generate_batch(n, signup_date):
    """Generate a batch of customers"""
    return [generate_customer(signup_date) for _ in range(n)]


# ---------- MAIN LOOP ----------
if __name__ == "__main__":
    signup_date = START_DATE
    while True:
        batch = generate_batch(BATCH_SIZE, signup_date)
        df = pd.DataFrame(batch)
        print(df.head())  # preview (later send to Kafka/DB)

        # Save to CSV (append mode)
        df.to_csv("synthetic_churn_data.csv", mode="a", header=False, index=False)

        # Move signup date forward (simulate time progression)
        signup_date += timedelta(days=1)

        time.sleep(SLEEP_TIME)  # wait before generating next batch
