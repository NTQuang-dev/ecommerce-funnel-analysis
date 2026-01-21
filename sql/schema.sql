-- schema.sql
-- Database for project

CREATE DATABASE IF NOT EXISTS ecommerce_db;
USE ecommerce_db;

CREATE TABLE IF NOT EXISTS raw_events (
    event_time DATETIME,
    event_type VARCHAR(20),
    product_id INT,
    category_id BIGINT,
    category_code VARCHAR(255),
    brand VARCHAR(100),
    price DECIMAL(10, 2),
    user_id INT,
    user_session VARCHAR(50)
);

-- Creating index on user_session and event_type for Funnel Analysis
CREATE INDEX idx_session ON raw_events(user_session);
CREATE INDEX idx_event ON raw_events(event_type);