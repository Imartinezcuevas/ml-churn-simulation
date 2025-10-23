CREATE MATERIALIZED VIEW IF NOT EXISTS daily_signups_summary AS
SELECT DATE(signup_date) AS day,
       COUNT(*) AS total_signups,
       COUNT(DISTINCT region) as unique_regions
FROM customers
GROUP BY DATE(signup_date)
ORDER BY day;

CREATE MATERIALIZED VIEW IF NOT EXISTS plan_distribution_summary AS
SELECT plan, COUNT(*) AS count
FROM customers
GROUP BY plan;

CREATE MATERIALIZED VIEW IF NOT EXISTS signups_by_region_device AS
SELECT region, device, COUNT(*) AS count
FROM customers
GROUP BY region, device;

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_active_users AS
SELECT DATE(session_start) as day,
       COUNT(DISTINCT customer_id) AS active_users
FROM activity
GROUP BY day
ORDER BY day;

CREATE MATERIALIZED VIEW IF NOT EXISTS avg_session_length_per_plan AS
SELECT c.plan,
       ROUND(AVG(a.session_length)::numeric, 2) AS avg_session_length
FROM activity a
JOIN customers c ON a.customer_id = c.customer_id
GROUP BY c.plan;

CREATE MATERIALIZED VIEW IF NOT EXISTS support_summary AS
SELECT DATE(ticket_date) AS day,
       COUNT(*) as total_tickets,
       SUM(CASE WHEN resolved THEN 1 ELSE 0 END) AS resolved_tickets,
       ROUND(100.0 * SUM(CASE WHEN resolved THEN 1 ELSE 0 END) / COUNT(*), 2) AS resolution_rate
FROM support
GROUP BY day
ORDER BY day;

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_revenue_summary AS
SELECT
    DATE(payment_date) AS day,
    COUNT(*) AS transactions,
    SUM(amount) AS total_revenue,
    ROUND(AVG(amount)::numeric, 2) AS avg_payment
FROM billing
WHERE status = 'paid'
GROUP BY day
ORDER BY day;

CREATE MATERIALIZED VIEW IF NOT EXISTS customer_lifetime_value AS
SELECT
    b.customer_id,
    SUM(b.amount) AS total_revenue,
    COUNT(b.id) AS payments_made,
    MAX(b.payment_date) AS last_payment
FROM billing b
WHERE b.status = 'paid'
GROUP BY b.customer_id;


CREATE MATERIALIZED VIEW IF NOT EXISTS weekly_retention AS
SELECT
    DATE_TRUNC('week', c.signup_date) AS signup_week,
    DATE_TRUNC('week', a.session_start) AS active_week,
    COUNT(DISTINCT a.customer_id) AS active_users
FROM customers c
JOIN activity a ON c.customer_id = a.customer_id
GROUP BY signup_week, active_week
ORDER BY signup_week, active_week;

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_arpu AS
SELECT
    DATE(b.payment_date) AS day,
    ROUND(SUM(b.amount)::numeric / COUNT(DISTINCT b.customer_id), 2) AS arpu
FROM billing b
WHERE b.status = 'paid'
GROUP BY DATE(b.payment_date)
ORDER BY day;

CREATE MATERIALIZED VIEW IF NOT EXISTS support_load AS
SELECT
    s.day,
    s.total_tickets,
    a.active_users,
    ROUND(s.total_tickets::numeric / a.active_users, 3) AS tickets_per_user
FROM support_summary s
JOIN daily_active_users a ON s.day = a.day;

CREATE MATERIALIZED VIEW IF NOT EXISTS churn_summary AS
SELECT
    DATE(signup_date) AS signup_day,
    COUNT(*) FILTER (WHERE churned) AS churned_users,
    COUNT(*) AS total_users,
    ROUND(100.0 * COUNT(*) FILTER (WHERE churned) / COUNT(*), 2) AS churn_rate
FROM customers
GROUP BY signup_day
ORDER BY signup_day;

CREATE MATERIALIZED VIEW IF NOT EXISTS churned_user_activity AS
SELECT
    c.customer_id,
    COUNT(a.id) AS sessions_before_churn,
    ROUND(AVG(a.session_length)::numeric, 2) AS avg_session_length,
    MAX(a.session_start) AS last_activity_date,
    (CURRENT_DATE - MAX(a.session_start)::date) AS inactive_days
FROM customers c
LEFT JOIN activity a ON c.customer_id = a.customer_id
WHERE c.churned = TRUE
GROUP BY c.customer_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS churned_billing_summary AS
SELECT
    c.customer_id,
    COUNT(b.id) AS payments_made,
    SUM(b.amount) AS total_spent,
    MAX(b.payment_date) AS last_payment_date
FROM customers c
LEFT JOIN billing b ON c.customer_id = b.customer_id
WHERE c.churned = TRUE AND b.status = 'success'
GROUP BY c.customer_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS churned_support_summary AS
SELECT
    c.customer_id,
    COUNT(s.id) AS total_tickets,
    ROUND(100.0 * SUM(CASE WHEN s.resolved THEN 1 ELSE 0 END) / COUNT(s.id), 2) AS resolution_rate
FROM customers c
LEFT JOIN support s ON c.customer_id = s.customer_id
WHERE c.churned = TRUE
GROUP BY c.customer_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS churn_insights AS
SELECT
    c.customer_id,
    c.signup_date,
    c.region,
    c.plan,
    a.sessions_before_churn,
    a.inactive_days,
    b.total_spent,
    b.payments_made,
    s.total_tickets,
    s.resolution_rate
FROM customers c
LEFT JOIN churned_user_activity a ON c.customer_id = a.customer_id
LEFT JOIN churned_billing_summary b ON c.customer_id = b.customer_id
LEFT JOIN churned_support_summary s ON c.customer_id = s.customer_id
WHERE c.churned = TRUE;