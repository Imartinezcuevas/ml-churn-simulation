import uuid
import json
import time
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
devices = ["mobile", "desktop"]

print("Starting Signups producer ...")

while True:
    customer_id = str(uuid.uuid4())
    event = {
        "customer_id": customer_id,
        "signup_date": datetime.now().isoformat(),
        "email": fake.email(),
        "region": fake.random_element(elements=["NA", "EU", "LATAM", "APAC"]),
        "plan": fake.random_element(elements=plans),
        "device": fake.random_element(elements=devices)
    }

    producer.produce(TOPIC, json.dumps(event), callback=delivery_report)
    producer.flush()
    print(f"Sent singup: {customer_id}")

    time.sleep(DELAY)