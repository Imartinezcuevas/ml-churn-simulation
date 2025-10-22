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
WHERE status = 'success'
GROUP BY day
ORDER BY day;

CREATE MATERIALIZED VIEW IF NOT EXISTS customer_lifetime_value AS
SELECT
    b.customer_id,
    SUM(b.amount) AS total_revenue,
    COUNT(b.id) AS payments_made,
    MAX(b.payment_date) AS last_payment
FROM billing b
WHERE b.status = 'success'
GROUP BY b.customer_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS signup_activity_conversion AS
SELECT
    DATE(c.signup_date) AS signup_day,
    COUNT(DISTINCT c.customer_id) AS signups,
    COUNT(DISTINCT a.customer_id) AS active,
    ROUND(100.0 * COUNT(DISTINCT a.customer_id) / COUNT(DISTINCT c.customer_id), 2) AS conversion_rate
FROM customers c
LEFT JOIN activity a ON DATE(a.session_start) = DATE(c.signup_date)
GROUP BY DATE(c.signup_date)
ORDER BY signup_day;