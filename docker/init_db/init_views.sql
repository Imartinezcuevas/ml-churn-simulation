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