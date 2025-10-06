import uuid
import os
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
CURRENT_DATE = datetime(2025, 12, 1)

# Drift parameters (will modify over time)
plan_distribution = {
    "2024": {"free": 0.6, "basic": 0.3, "premium":0.1},
    "2025": {"free": 0.3, "basic": 0.4, "premium":0.3}
}
regions = ["NA", "EU", "LATAM", "APAC"]
region_probs = [0.4, 0.3, 0.2, 0.1]
device_types = ["mobile", "desktop"]
device_probs = [0.7, 0.3]

def plan_probs_with_drift(day_index):
    """
    Gradual drift: interpolate form initial distribution to new distribution over N days
    """
    p0 = np.array([0.6, 0.3, 0.1])  # start distribution (free, basic, premium)
    p1 = np.array([0.3, 0.4, 0.3])  # end distribution after drift
    alpha = min(1.0, day_index / 200)  # full drift after 200 days
    return (1 - alpha) * p0 + alpha * p1

def seasonal_multiplier(day_index):
    """
    Add seasonal effect to montly spend and session length
    """
    return 1 + 0.1 * np.sin(2 * np.pi * (day_index % 365) / 365)

def generate_customer(signup_date, day_index):
    # Plan
    probs = plan_probs_with_drift(day_index)
    plan_type = np.random.choice(["free", "basic", "premium"], p=probs)

    # Monthly spend conditional on plan
    if plan_type == "free":
        monthly_spend = 0
    elif plan_type == "basic":
        monthly_spend = round(np.random.normal(20, 5), 2)
    else:
        monthly_spend = round(np.random.normal(50, 10), 2)

    monthly_spend *= seasonal_multiplier(day_index)
    monthly_spend = round(monthly_spend, 2)

    # Support tickets
    lam = 1.5 if plan_type == "free" else 1.0 if plan_type == "basic" else 0.5
    support_tickets = np.random.poisson(lam)

    # Region
    region = np.random.choice(regions, p=region_probs)

    # Device
    device_type = np.random.choice(device_types, p=device_probs)

    # Average session length
    if plan_type == "free":
        avg_session_length = round(np.random.normal(5, 2), 2)
    elif plan_type == "basic":
        avg_session_length = round(np.random.normal(10, 3), 2)
    else:
        avg_session_length = round(np.random.normal(15, 5), 2)

    if device_type == "desktop":
        avg_session_length *= 1.2 # desktop users spend more time
    avg_session_length *= seasonal_multiplier(day_index)
    avg_session_length = round(avg_session_length, 2)

    # Churn probability
    base_churn = {"free": 0.25, "basic": 0.12, "premium": 0.66}
    region_mult = {"NA": 1.0, "EU": 0.9, "LATAM": 1.2, "APAC": 1.1}
    device_mult = {"mobile": 1.1, "desktop": 0.9}

    logit = np.log(base_churn[plan_type]/(1 - base_churn[plan_type])) + 0.25 * support_tickets - 0.02 * avg_session_length + np.log(region_mult[region]) + np.log(device_mult[device_type])
    prob_churn = 1 / (1 + np.exp(-logit))
    churned = int(np.random.rand() < prob_churn)

    # Last active date
    last_active = signup_date + timedelta(days=np.random.randint(1, 180)) if churned else datetime.now()

    return {
        "customer_id": str(uuid.uuid4()),
        "signup_date": signup_date.date().isoformat(),
        "last_active_date": last_active.date().isoformat(),
        "plan_type": plan_type,
        "monthly_spend": monthly_spend,
        "support_tickets": support_tickets,
        "region": region,
        "device_type": device_type,
        "avg_session_length": avg_session_length,
        "churned": churned
    }

def clear_console():
    os.system('cls' if os.name == 'nt' else 'clear')

def print_batch_metrics(df, day_index):
    """
    Print live metrics dashboard to console
    """
    clear_console()
    print(f"--- Day {day_index} Metrics ---")
    print(f"Total customers: {len(df)}")
    
    churn_rate = df['churned'].mean()
    print(f"Churn rate: {churn_rate:.2f}")

    print("\nPlan distribution:")
    print(df['plan_type'].value_counts(normalize=True))

    print("\nRegion distribution:")
    print(df['region'].value_counts(normalize=True))

    avg_session = df['avg_session_length'].mean()
    print(f"\nAvg session length: {avg_session:.2f} min")

    print("-------------------------------\n")



if __name__ == "__main__":
    signup_date = START_DATE
    day_index = 0
    while True:
        batch = [generate_customer(signup_date, day_index) for _ in range(BATCH_SIZE)]
        df = pd.DataFrame(batch)
        print_batch_metrics(df, day_index)

        df.to_csv("synthetic_churn_data.csv", mode="a", header=False, index=False)

        signup_date += timedelta(days=1)
        day_index += 1

        time.sleep(SLEEP_TIME)
