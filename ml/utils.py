# ml/utils.py
import os
import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor
from datetime import datetime
import numpy as np
import random

SEED = int(os.getenv("SEED", 42))
np.random.seed(SEED)
random.seed(SEED)

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "churn_db"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASS", "admin")
}

START_DATE = datetime(2025,1,1).date()

def get_db_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    return conn

def load_feature_table(as_of_date=None):
    """
    Crea dataset de features por cliente usando las tablas que ya tienes.
    as_of_date: fecha para considerar (datetime.date). Si None usa CURRENT_DATE.
    """
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    if as_of_date is None:
        cur.execute("SELECT CURRENT_DATE AS as_of_date;")
        as_of_date = cur.fetchone()["as_of_date"]

    query = """
    SELECT
      c.customer_id,
      c.plan,
      c.region,
      c.device,
      c.signup_date::date AS signup_date,
      COALESCE(MAX(a.session_start::date), c.signup_date::date) AS last_activity_date,
      ( %s::date - COALESCE(MAX(a.session_start::date), c.signup_date::date) ) AS days_since_last_activity,
      COALESCE(SUM(CASE WHEN b.status = 'failed' THEN 1 ELSE 0 END),0) AS failed_payments,
      COALESCE(SUM(CASE WHEN s.resolved = false THEN 1 ELSE 0 END),0) AS unresolved_tickets,
      c.churned
    FROM customers c
    LEFT JOIN activity a ON a.customer_id = c.customer_id
    LEFT JOIN billing b ON b.customer_id = c.customer_id
    LEFT JOIN support s ON s.customer_id = c.customer_id
    WHERE c.signup_date <= %s
    GROUP BY c.customer_id, c.plan, c.region, c.device, c.signup_date, c.churned
    ORDER BY c.customer_id;
    """
    cur.execute(query, (as_of_date, as_of_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)
