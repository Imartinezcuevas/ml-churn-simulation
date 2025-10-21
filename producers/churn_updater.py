import psycopg2
import numpy as np
from datetime import datetime, timedelta

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "churn_db",
    "user": "admin",
    "password": "admin"
}

# Probabilidades base por plan
BASE_CHURN_PROB = {
    "free": 0.02,
    "basic": 0.01,
    "premium": 0.005
}

# Factores adicionales
INACTIVITY_FACTOR = 0.1   # +10% si no ha habido actividad
FAILED_PAYMENT_FACTOR = 0.15  # +15% si hay pagos fallidos
TICKET_FACTOR = 0.05  # +5% por ticket sin resolver

def update_churn(simulated_day):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Selecciona clientes activos
    cur.execute("""
        SELECT c.customer_id,
               c.plan,
               COALESCE(MAX(a.session_start), c.signup_date) AS last_activity,
               COUNT(b.id) FILTER (WHERE b.status != 'paid') AS unpaid_bills,
               COUNT(s.id) FILTER (WHERE s.resolved = FALSE) AS unresolved_tickets
        FROM customers c
        LEFT JOIN activity a ON a.customer_id = c.customer_id
        LEFT JOIN billing b ON b.customer_id = c.customer_id
        LEFT JOIN support s ON s.customer_id = c.customer_id
        WHERE c.churned = FALSE
        GROUP BY c.customer_id, c.plan, c.signup_date
    """)
    customers = cur.fetchall()

    for customer_id, plan, last_activity, unpaid_bills, unresolved_tickets in customers:
        # Calcula probabilidad
        churn_prob = BASE_CHURN_PROB[plan]

        # Aumenta si hay inactividad
        days_since_last_activity = (simulated_day - last_activity).days
        if days_since_last_activity > 7:
            churn_prob += INACTIVITY_FACTOR

        # Aumenta si hay pagos fallidos
        if unpaid_bills > 0:
            churn_prob += FAILED_PAYMENT_FACTOR

        # Aumenta por tickets sin resolver
        churn_prob += unresolved_tickets * TICKET_FACTOR

        # Decide si churnea
        if np.random.rand() < churn_prob:
            cur.execute("""
                UPDATE customers
                SET churned = TRUE
                WHERE customer_id = %s
            """, (customer_id,))

    conn.commit()
    cur.close()
    conn.close()
