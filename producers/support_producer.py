import json
import time
import numpy as np
from datetime import datetime, timedelta
from confluent_kafka import Producer
import psycopg2
from psycopg2.extras import RealDictCursor

SEED = 42
np.random.seed(SEED)

SUPPORT_TOPIC = "support_topic"
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

ISSUE_TYPES = ["billing", "technical", "usabilty", "other"]
PLAN_TICKET_PROB = {"free": 0.05, "basic": 0.03, "premium": 0.01}
PLAN_RESOLVE_PROB = {"free": 0.6, "basic": 0.8, "premium": 0.9}

# -----------------
# Helper functions
# -----------------
def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered message to {msg.topic()} [{msg.partition()}]")

def fetch_customers():
    """Fetch all customers from DB"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT customer_id, plan, region, signup_date FROM customers;")
    customers = cur.fetchall()
    cur.close()
    conn.close()
    return customers

def simulate_support(customer, simulated_date):
    """Simulate support tickets for a given customer and day"""
    plan = customer["plan"]

    if np.random.rand() > PLAN_TICKET_PROB[plan]:
        return []
    
    # Weekly spike
    if simulated_date.weekday() == 0:
        prob_multiplier = 1.5
    else:
        prob_multiplier = 1.0

    num_tickets = np.random.poisson(PLAN_TICKET_PROB[plan] * 20 * prob_multiplier)
    tickets = []

    for _ in range(num_tickets):
        issue_type = np.random.choice(ISSUE_TYPES, p=[0.3, 0.4, 0.2, 0.1])
        resolved = np.random.rand() < PLAN_RESOLVE_PROB[plan]
        tickets.append({
            "customer_id": customer["customer_id"],
            "ticket_date": simulated_date.isoformat(),
            "issue_type": issue_type,
            "resolved": resolved
        })
    return tickets

# ---------------------
# Simulation loop
# ---------------------
START_DATE = datetime(2024, 1, 1)
day_index = 0

while day_index < TOTAL_SIMULATION_DAYS:
    simulated_day = START_DATE + timedelta(days=day_index)
    customers = fetch_customers()
    print(f"[Day {day_index + 1}] Simulating support tickets for {len(customers)} customers ...")

    for customer in customers:
        signup_date = customer["signup_date"]

        #Skip customers who signed up after this simulated day
        if signup_date > simulated_day:
            continue

        tickets = simulate_support(customer, simulated_day)
        for ticket in tickets:
            producer.produce(SUPPORT_TOPIC, json.dumps(ticket), callback=delivery_report)
    
    producer.flush()
    day_index += 1
    time.sleep(MINUTES_PER_DAY * 60)