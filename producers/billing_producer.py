import json
import time
import numpy as np
from datetime import datetime, timedelta
from confluent_kafka import Producer
import psycopg2
from psycopg2.extras import RealDictCursor

BILLING_TOPIC = "billing_topic"
MINUTES_PER_DAY = 1          # 1 minuto real = 1 día simulado
TOTAL_SIMULATION_DAYS = 30

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "churn_db",
    "user": "admin",
    "password": "admin"
}

producer = Producer({"bootstrap.servers": "localhost:9092"})

PLAN_PRICES = {"free": 0, "basic": 10, "premium": 25}
PLAN_PAYMENT_PROB = {"free": 0.0, "basic": 0.95, "premium": 0.98}
PLAN_FAIL_SPIKE = {"basic": 0.10, "premium": 0.05} 

# ---------------------
# Helper functions
# ---------------------
def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered message to {msg.topic()} [{msg.partition()}]")

def fetch_customers():
    """Fetch all customers from DB"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT customer_id, plan, signup_date FROM customers;")
    customers = cur.fetchall()
    cur.close()
    conn.close()
    return customers

def simulate_billing(customer, simulated_date):
    plan = customer["plan"]
    if plan == "free":
        return None  # free plan doesn't pay

    # Just a payment per month
    signup_date = customer["signup_date"]
    days_since_signup = (simulated_date - signup_date).days
    if days_since_signup < 0 or days_since_signup % 30 != 0:
        return None

    # Simulate payment failure (churn potencial)
    pay_success = np.random.rand() < PLAN_PAYMENT_PROB[plan]
    if not pay_success and np.random.rand() < PLAN_FAIL_SPIKE[plan]:
        status = "failed"
    else:
        status = "paid" if pay_success else "pending"

    return {
        "customer_id": customer["customer_id"],
        "payment_date": simulated_date.isoformat(),
        "amount": PLAN_PRICES[plan],
        "status": status
    }

# ---------------------
# Simulation loop
# ---------------------
START_DATE = datetime(2024, 1, 1)
day_index = 0

while day_index < TOTAL_SIMULATION_DAYS:
    simulated_day = START_DATE + timedelta(days=day_index)
    customers = fetch_customers()
    print(f"[Day {day_index + 1}] Simulating billing for {len(customers)} customers ...")

    for customer in customers:
        signup_date = customer["signup_date"]

        # Avoid billing before signup
        if signup_date > simulated_day:
            continue

        billing_event = simulate_billing(customer, simulated_day)
        if billing_event:
            producer.produce(BILLING_TOPIC, json.dumps(billing_event), callback=delivery_report)

    producer.flush()
    day_index += 1
    time.sleep(MINUTES_PER_DAY * 60)
