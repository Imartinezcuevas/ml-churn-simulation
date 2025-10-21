import json
import numpy as np
from datetime import datetime
from confluent_kafka import Producer
import psycopg2
from psycopg2.extras import RealDictCursor

SUPPORT_TOPIC = "support_topic"
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

ISSUE_TYPES = ["billing", "technical", "usability", "other"]
PLAN_TICKET_PROB = {"free": 0.05, "basic": 0.03, "premium": 0.01}
PLAN_RESOLVE_PROB = {"free": 0.6, "basic": 0.8, "premium": 0.9}

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
        SELECT customer_id, plan, region, device, signup_date, churned
        FROM customers
        WHERE signup_date <= %s
    """, (simulated_day,))
    customers = cur.fetchall()
    cur.close()
    conn.close()
    return [c for c in customers if not c.get("churned", False)]

def simulate_support(customer, simulated_day):
    """Simulate support tickets for a customer on a given day"""
    plan = customer["plan"]

    # Skip if no ticket today
    if np.random.rand() > PLAN_TICKET_PROB[plan]:
        return []

    # Weekly spike: Monday
    prob_multiplier = 1.5 if simulated_day.weekday() == 0 else 1.0
    num_tickets = np.random.poisson(PLAN_TICKET_PROB[plan] * 10 * prob_multiplier)

    tickets = []
    for _ in range(num_tickets):
        issue_type = np.random.choice(ISSUE_TYPES, p=[0.3,0.4,0.2,0.1])
        resolved = np.random.rand() < PLAN_RESOLVE_PROB[plan]
        tickets.append({
            "customer_id": customer["customer_id"],
            "ticket_date": simulated_day.isoformat(),
            "issue_type": issue_type,
            "resolved": resolved
        })
    return tickets

# -------------------
# Main function
# -------------------
def produce_support(simulated_day):
    customers = fetch_active_customers(simulated_day)
    print(f"[{simulated_day.date()}] Producing support tickets for {len(customers)} customers")

    for customer in customers:
        tickets = simulate_support(customer, simulated_day)
        for ticket in tickets:
            producer.produce(SUPPORT_TOPIC, json.dumps(ticket), callback=delivery_report)
    producer.flush()
