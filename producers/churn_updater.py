import psycopg2
import numpy as np
from datetime import datetime
from drift_config import DriftConfig

SEED = 42
np.random.seed(SEED)

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "churn_db",
    "user": "admin",
    "password": "admin"
}

def calculate_daily_churn_prob(plan, days_inactive, failed_payments, unresolved_tickets, day_index):
    """
    Calcula probabilidad de churn con drift temporal
    """
    # Obtener parámetros dependientes del tiempo
    monthly_churn_probs = DriftConfig.get_churn_probs(day_index)
    inactivity_threshold = DriftConfig.get_inactivity_threshold(day_index)
    payment_multiplier = DriftConfig.get_payment_importance(day_index)
    ticket_multiplier = DriftConfig.get_ticket_importance(day_index)
    
    # Probabilidad base (mensual a diaria)
    base_prob = monthly_churn_probs[plan] / 30.0
    
    # Multiplicadores
    multiplier = 1.0
    
    # Inactividad (umbral dinámico)
    if days_inactive > inactivity_threshold:
        multiplier *= 2.5
    
    # Pagos fallidos (importancia dinámica)
    if failed_payments > 0:
        multiplier *= payment_multiplier
    
    # Tickets sin resolver (importancia dinámica)
    if unresolved_tickets > 0:
        multiplier *= (ticket_multiplier ** min(unresolved_tickets, 3))
    
    # Probabilidad final
    final_prob = min(base_prob * multiplier, 0.5)
    
    return final_prob

def update_churn(simulated_day):
    day_index = (simulated_day - datetime(2025,1,1)).days
    drift_factor = DriftConfig.get_drift_factor(day_index)
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM customers WHERE churned = FALSE")
    active_count = cur.fetchone()[0]
    
    # Advertencia de drift
    if day_index == 180:
        print("\n" + "="*60)
        print("⚠️  DRIFT STARTING - Behavior patterns beginning to shift")
        print("="*60 + "\n")
    elif day_index == 270:
        print("\n" + "="*60)
        print("🔴 DRIFT COMPLETE - Model retraining recommended")
        print("="*60 + "\n")

    cur.execute("""
        SELECT 
            c.customer_id,
            c.plan,
            c.signup_date::date,
            MAX(a.session_start::date) AS last_activity_date,
            COUNT(DISTINCT b.id) FILTER (WHERE b.status = 'failed') AS failed_payments,
            COUNT(DISTINCT s.id) FILTER (WHERE s.resolved = FALSE) AS unresolved_tickets
        FROM customers c
        LEFT JOIN activity a ON a.customer_id = c.customer_id
        LEFT JOIN billing b ON b.customer_id = c.customer_id
        LEFT JOIN support s ON s.customer_id = c.customer_id
        WHERE c.churned = FALSE
          AND c.signup_date <= %s
        GROUP BY c.customer_id, c.plan, c.signup_date
    """, (simulated_day,))
    
    customers = cur.fetchall()

    if not customers:
        cur.close()
        conn.close()
        return

    churned_today = 0
    churn_stats = {"free": 0, "basic": 0, "premium": 0}

    for customer_id, plan, signup_date, last_activity_date, failed_payments, unresolved_tickets in customers:
        effective_last_activity = last_activity_date if last_activity_date else signup_date
        
        if isinstance(effective_last_activity, str):
            effective_last_activity = datetime.fromisoformat(effective_last_activity).date()
        
        if isinstance(simulated_day, datetime):
            simulated_day_date = simulated_day.date()
        else:
            simulated_day_date = simulated_day

        days_since_last_activity = (simulated_day_date - effective_last_activity).days
        
        # ✅ Usar función con drift
        churn_prob = calculate_daily_churn_prob(
            plan, 
            days_since_last_activity, 
            failed_payments, 
            unresolved_tickets,
            day_index  # ← Parámetro nuevo
        )

        if np.random.rand() < churn_prob:
            cur.execute("""
                UPDATE customers
                SET churned = TRUE
                WHERE customer_id = %s
            """, (customer_id,))
            churned_today += 1
            churn_stats[plan] += 1

    conn.commit()
    cur.close()
    conn.close()
    
    churn_rate = (churned_today / len(customers) * 100) if customers else 0
    print(f"Day {day_index+1} | Drift: {drift_factor:.2f} | "
          f"Churned: {churned_today}/{len(customers)} ({churn_rate:.2f}%) | "
          f"Free:{churn_stats['free']} Basic:{churn_stats['basic']} Premium:{churn_stats['premium']}")