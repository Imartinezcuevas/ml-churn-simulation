import json
import time
import numpy as np
from datetime import datetime, timedelta
from confluent_kafka import Producer
import psycopg2
from psycopg2.extras import RealDictCursor

ACTIVITY_TOPIC = "activity_topic"
MINUTES_PER_DAY = 1          # 1 real minute = 1 simulated day
TOTAL_SIMULATION_DAYS = 30

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "churn_db",
    "user": "admin",
    "password": "admin"
}

producer = Producer({"bootstrap.servers": "localhost:9092"})

# Session patterns
PLAN_SESSION_MEAN = {"free": 1, "basic": 3, "premium": 5}
PLAN_SESSION_LENGTH = {"free": 5, "basic": 10, "premium": 15}
PLAN_ACTIVE_PROB = {"free": 0.3, "basic": 0.6, "premium": 0.8}

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
    cur = conn.cursor(cursor_factory = RealDictCursor)
    cur.execute("SELECT customer_id, plan, devie, region, signup_date FROM customers;")
    customers = cur.fetchall()
    cur.close()
    conn.close()
    return customers

def simulate_sessions(customer, simulated_date):
    """Generate session for a single customer on a given date"""
    plan = customer["plan"]
    if np.random.rand() > PLAN_ACTIVE_PROB[plan]:
        return [] # inactive today
    
    lam = PLAN_SESSION_MEAN * np.random.uniform(0.8, 1.2)

    # Weekly spike
    if simulated_date.weekday() in [0, 1]: # Mon-Tue higher usage
        lam *= 1.3

    num_sessions = np.random.poisson(lam)
    sessions = []

    for _ in range(num_sessions):
        avg_len = PLAN_SESSION_LENGTH[plan]
        session_lenght = max(1, np.random.normal(avg_len, 2))
        session.append({
            "customer_id": customer["customer_id"],
            "session_start": simulated_date.isoformat(),
            "session_length": round(session_lenght, 2),
            "device": customer["device"]
        })
    return sessions
    

# ---------------------
# Simulation loop
# ---------------------
START_DATE = datetime(2024, 1, 1)
day_index = 0

while day_index < TOTAL_SIMULATION_DAYS:
    simulated_day = START_DATE + timedelta(days=day_index)
    customers = fetch_customers()
    print(f"[Day {day_index + 1}] Simulating activity for {len(customers)} customers ...")

    for customer in customers:
        signup_date = customer["signup_date"]

        # Skip customers who signed up after this simulated day
        if signup_date > simulated_day:
            continue

        sessions = simulate_sessions(customer, simulated_day)
        for session in sessions:
            producer.produce(ACTIVITY_TOPIC, json.dumps(session), callback=delivery_report)

        producer.flush()
        day_index += 1
        time.sleep(MINUTES_PER_DAY * 60)