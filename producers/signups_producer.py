import uuid
import json
import time
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
from confluent_kafka import Producer

# -----------------------
# Configuration
# -----------------------
TOPIC = 'signups_topic'
DELAY = 1
MINUTES_PER_DAY = 1          # 1 real minute = 1 simulated day
TOTAL_SIMULATION_DAYS = 30   # run for 30 simulated days

fake = Faker()
producer = Producer({'bootstrap.servers': 'localhost:9092'})

plans = ["free", "basic", "premium"]
devices = ["mobile", "desktop"]
regions = ["NA", "EU", "LATAM", "APAC"]

START_DATE = datetime(2024, 1, 1)

# ----------------------
# Helper functions
# ----------------------
def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered message to {msg.topic()} [{msg.partition()}]")

def plan_probs_with_drift(day_index):
    """Gradually drift from initial to target plan distribution"""
    p0 = np.array([0.5, 0.3, 0.2])
    p1 = np.array([0.3, 0.4, 0.3])
    alpha = min(1.0, day_index / TOTAL_SIMULATION_DAYS)
    return (1 - alpha) * p0 + alpha * p1

def churn_spike(day_index):
    """Introduce occasonal churn spikes."""
    # Every 10th day, temporary higher churn chance
    return 1.5 if day_index % 10 == 0 else 1.0

def signup_multiplier(day_index):
    """Simulate campaings or anomalies in signups."""
    if day_index % 7 == 0: # Weekly campaign
        return 2.0
    return 1.0

print("Starting Signups producer ...")

day_index = 0
while day_index < TOTAL_SIMULATION_DAYS:
    simulated_day = START_DATE + timedelta(days=day_index)

    # Determine plan probabilities with drift
    probs = plan_probs_with_drift(day_index)

    # Base lambda for Poisson signups
    base_lam = 50 + (day_index / TOTAL_SIMULATION_DAYS) * 10  # average 25 signups per day
    lam = base_lam * signup_multiplier(day_index)
    num_signups = np.random.poisson(lam)

    # Generate customers
    for _ in range(num_signups):
        customer_id = str(uuid.uuid4())
        plan = np.random.choice(plans, p=probs)
        region = np.random.choice(regions)
        device = np.random.choice(devices)

        # Apply churn spike effect for future metrics
        churn_factor = churn_spike(day_index)

        event = {
            "customer_id": customer_id,
            "signup_date": simulated_day.isoformat(),
            "email": fake.email(),
            "region": region,
            "plan": plan,
            "device": device,
            "churn_factor": churn_factor
        }

        producer.produce(TOPIC, json.dumps(event), callback=delivery_report)
        print(f"[Day {day_index+1}] Sent signup: {customer_id} ({plan}, {region}, {device})")

    producer.flush()

    day_index += 1
    time.sleep(MINUTES_PER_DAY * 60)  # 1 min = 1 simulated day

print("Simulation complete!")