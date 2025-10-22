# producers/signups.py
import uuid
import json
import numpy as np
from faker import Faker
from confluent_kafka import Producer
from datetime import timedelta, datetime

SEED = 42
np.random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

producer = Producer({'bootstrap.servers': 'localhost:29092'})
TOPIC = 'signups_topic'
TOTAL_SIMULATION_DAYS = 365

plans = ["free", "basic", "premium"]
devices = ["mobile", "desktop"]
regions = ["NA", "EU", "LATAM", "APAC"]

# ---------- Helper functions ----------
def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        pass  # Can remove printing for speed

def plan_probs_with_drift(day_index):
    """Gradually drift from initial to target plan distribution"""
    p0 = np.array([0.5, 0.3, 0.2])
    p1 = np.array([0.3, 0.4, 0.3])
    alpha = min(1.0, day_index / TOTAL_SIMULATION_DAYS)
    return (1 - alpha) * p0 + alpha * p1

def signup_multiplier(day_index):
    """Simulate campaigns or anomalies"""
    if day_index % 7 == 0:  # Weekly campaign
        return 2.0
    return 1.0

# ---------- Main function ----------
def produce_signups(simulated_day):
    day_index = (simulated_day - datetime(2025,1,1)).days
    probs = plan_probs_with_drift(day_index)
    base_lam = 50 + (day_index / TOTAL_SIMULATION_DAYS) * 10
    lam = base_lam * signup_multiplier(day_index)
    num_signups = np.random.poisson(lam)

    for _ in range(num_signups):
        customer_id = str(uuid.uuid4())
        plan = np.random.choice(plans, p=probs)
        region = np.random.choice(regions)
        device = np.random.choice(devices)

        event = {
            "customer_id": customer_id,
            "signup_date": simulated_day.isoformat(),
            "email": fake.email(),
            "region": region,
            "plan": plan,
            "device": device,
            "churned": False
        }

        producer.produce(TOPIC, json.dumps(event), callback=delivery_report)
        
    producer.flush()
    print(f"[Day {day_index+1}] Produced {num_signups} signups")
