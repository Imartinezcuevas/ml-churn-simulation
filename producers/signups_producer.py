import uuid
import json
import time
import numpy as np
from datetime import datetime
from faker import Faker
from confluent_kafka import Producer

# -----------------------
# Configuration
# -----------------------
TOPIC = 'signups_topic'
DELAY = 1

fake = Faker()
producer = Producer({'bootstrap.servers': 'localhost:9092'})

# ----------------------
# CLIENT GENERATION
# ----------------------
def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered message to {msg.topic()} [{msg.partition()}]")


plans = ["free", "basic", "premium"]
plan_probs = [0.5, 0.3, 0.2]
devices = ["mobile", "desktop"]
regions = ["NA", "EU", "LATAM", "APAC"]

print("Starting Signups producer ...")

while True:
    hour = datetime.now().hour
    lam = 0.3 if 8 <= hour <= 20 else 0.05
    num_singups = np.random.poisson(lam)

    for _ in range(num_singups):
        customer_id = str(uuid.uuid4())
        plan = np.random.choice(plans, p=plan_probs)
        event = {
            "customer_id": customer_id,
            "signup_date": datetime.now().isoformat(),
            "email": fake.email(),
            "region": np.random.choice(regions),
            "plan": plan,
            "device": np.random.choice(devices)
        }

        producer.produce(TOPIC, json.dumps(event), callback=delivery_report)
        print(f"Sent singup: {customer_id}")

    producer.flush()
    time.sleep(DELAY)