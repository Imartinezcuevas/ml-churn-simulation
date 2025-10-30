import json
import numpy as np
from datetime import datetime
from confluent_kafka import Producer
import psycopg2
from psycopg2.extras import RealDictCursor

BILLING_TOPIC = "billing_topic"
SEED = 42
np.random.seed(SEED)

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "churn_db",
    "user": "admin",
    "password": "admin"
}

producer = Producer({"bootstrap.servers": "localhost:29092"})

PLAN_PRICES = {"free": 0, "basic": 10, "premium": 25}
PLAN_PAYMENT_PROB = {"free": 0.0, "basic": 0.45, "premium": 0.78}
PLAN_FAIL_SPIKE = {"basic": 0.25, "premium": 0.1}

# -------------------
# Helper functions
# -------------------
def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        pass

def fetch_active_customers(simulated_day):
    """Get all customers who signed up on or before simulated_day"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT customer_id, plan, signup_date, churned
        FROM customers
        WHERE signup_date <= %s
    """, (simulated_day,))
    customers = cur.fetchall()
    cur.close()
    conn.close()
    return [c for c in customers if not c.get("churned", False)]

def simulate_billing(customer, simulated_day):
    plan = customer["plan"]
    if plan == "free":
        return None  # free plan doesn't pay

    signup_date = customer["signup_date"]
    days_since_signup = (simulated_day - signup_date).days
    # Monthly payment: every 30 days since signup
    if days_since_signup < 0 or days_since_signup % 30 != 0:
        return None

    # Simulate payment success/failure
    pay_success = np.random.rand() < PLAN_PAYMENT_PROB[plan]
    # Occasionally fail beyond normal probability
    if not pay_success and np.random.rand() < PLAN_FAIL_SPIKE[plan]:
        status = "failed"
    else:
        status = "paid" if pay_success else "pending"

    return {
        "customer_id": customer["customer_id"],
        "payment_date": simulated_day.isoformat(),
        "amount": PLAN_PRICES[plan],
        "status": status
    }

# -------------------
# Main function
# -------------------
def produce_billing(simulated_day):
    day_index = (simulated_day - datetime(2025,1,1)).days
    customers = fetch_active_customers(simulated_day)
    print(f"[Day {day_index+1}] Producing billing events for {len(customers)} customers")

    for customer in customers:
        billing_event = simulate_billing(customer, simulated_day)
        if billing_event:
            producer.produce(BILLING_TOPIC, json.dumps(billing_event), callback=delivery_report)
    producer.flush()
