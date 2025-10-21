import json
import numpy as np
from datetime import datetime
from confluent_kafka import Producer
import psycopg2
from psycopg2.extras import RealDictCursor

ACTIVITY_TOPIC = "activity_topic"
SEED = 42
np.random.seed(SEED)

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "churn_db",
    "user": "admin",
    "password": "admin"
}

producer = Producer({"bootstrap.servers": "localhost:9092"})

PLAN_SESSION_MEAN = {"free": 1, "basic": 3, "premium": 5}
PLAN_SESSION_LENGTH = {"free": 5, "basic": 10, "premium": 15}
PLAN_ACTIVE_PROB = {"free": 0.3, "basic": 0.6, "premium": 0.8}

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        pass  # silently succeed

def fetch_active_customers(simulated_day):
    """Get all customers who signed up on or before simulated_day"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT customer_id, plan, device, signup_date, churned
        FROM customers
        WHERE signup_date <= %s
    """, (simulated_day,))
    customers = cur.fetchall()
    cur.close()
    conn.close()
    return [c for c in customers if not c.get("churned", False)]

def simulate_sessions(customer, simulated_day):
    plan = customer["plan"]
    if np.random.rand() > PLAN_ACTIVE_PROB[plan]:
        return []

    lam = PLAN_SESSION_MEAN[plan] * np.random.uniform(0.8, 1.2)

    # Weekly spike for Monday-Tuesday
    if simulated_day.weekday() in [0,1]:
        lam *= 1.3

    num_sessions = np.random.poisson(lam)
    sessions = []

    for _ in range(num_sessions):
        session_length = max(1, np.random.normal(PLAN_SESSION_LENGTH[plan], 2))
        sessions.append({
            "customer_id": customer["customer_id"],
            "session_start": simulated_day.isoformat(),
            "session_length": round(session_length,2),
            "device": customer["device"]
        })
    return sessions

def produce_activity(simulated_day):
    customers = fetch_active_customers(simulated_day)
    print(f"[{simulated_day.date()}] Producing activity for {len(customers)} customers")

    for customer in customers:
        sessions = simulate_sessions(customer, simulated_day)
        for session in sessions:
            producer.produce(ACTIVITY_TOPIC, json.dumps(session), callback=delivery_report)
    producer.flush()
