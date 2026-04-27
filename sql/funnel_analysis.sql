-- funnel_analysis.sql
-- Aggregation queries that power the funnel summary table and dashboard.

USE ecommerce_db;

-- Create the materialized summary table
CREATE TABLE IF NOT EXISTS funnel_summary_final (
    event_date DATE,
    event_type VARCHAR(20),
    unique_sessions INT,
    unique_users INT,
    total_events INT,
    total_revenue DECIMAL(15, 2)
);

-- Load funnel metrics per day
-- Run this after raw_events is fully populated.
INSERT INTO funnel_summary_final (event_date, event_type, unique_sessions, unique_users, total_events, total_revenue)
SELECT
    DATE(event_time) AS event_date,
    event_type,
    COUNT(DISTINCT user_session) AS unique_sessions,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(*) AS total_events,
    COALESCE(SUM(price), 0) AS total_revenue
FROM raw_events
GROUP BY DATE(event_time), event_type
ORDER BY event_date, FIELD(event_type, 'view', 'cart', 'purchase');

-- Overall funnel conversion rates
SELECT
    event_type,
    COUNT(DISTINCT user_session) AS unique_sessions,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(*) AS total_events
FROM raw_events
GROUP BY event_type
ORDER BY FIELD(event_type, 'view', 'cart', 'purchase');

-- Drop-off between funnel stages
WITH funnel AS (
    SELECT
        event_type,
        COUNT(DISTINCT user_session) AS sessions
    FROM raw_events
    GROUP BY event_type
)
SELECT
    curr.event_type AS stage,
    curr.sessions,
    prev.sessions AS prev_sessions,
    ROUND((curr.sessions / prev.sessions) * 100, 2) AS conversion_pct,
    ROUND(((prev.sessions - curr.sessions) / prev.sessions) * 100, 2) AS dropoff_pct
FROM funnel curr
LEFT JOIN funnel prev
    ON (curr.event_type = 'cart' AND prev.event_type = 'view')
    OR (curr.event_type = 'purchase' AND prev.event_type = 'cart')
ORDER BY FIELD(curr.event_type, 'view', 'cart', 'purchase');

-- Top 10 categories by drop-off (cart but no purchase)
WITH cart_sessions AS (
    SELECT DISTINCT user_session, category_code
    FROM raw_events
    WHERE event_type = 'cart' AND category_code IS NOT NULL
),
purchase_sessions AS (
    SELECT DISTINCT user_session
    FROM raw_events
    WHERE event_type = 'purchase'
)
SELECT
    cs.category_code,
    COUNT(DISTINCT cs.user_session) AS cart_sessions,
    COUNT(DISTINCT ps.user_session) AS purchase_sessions,
    COUNT(DISTINCT cs.user_session) - COUNT(DISTINCT ps.user_session) AS abandoned,
    ROUND(
        (COUNT(DISTINCT cs.user_session) - COUNT(DISTINCT ps.user_session))
        / COUNT(DISTINCT cs.user_session) * 100, 2
    ) AS abandonment_rate
FROM cart_sessions cs
LEFT JOIN purchase_sessions ps ON cs.user_session = ps.user_session
GROUP BY cs.category_code
ORDER BY abandoned DESC
LIMIT 10;
