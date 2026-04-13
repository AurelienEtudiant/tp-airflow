-- =============================================================================
-- Init DWH PostgreSQL - Formation Airflow IPSSI
-- Schemas : staging, dwh, analytics
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ===== STAGING =====

CREATE TABLE IF NOT EXISTS staging.orders (
    id VARCHAR(30) PRIMARY KEY,
    date DATE NOT NULL,
    customer_id VARCHAR(20) NOT NULL,
    product VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.customers (
    id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200),
    city VARCHAR(100),
    registered_date DATE,
    active BOOLEAN DEFAULT TRUE,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.products (
    id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10,2),
    stock INTEGER,
    active BOOLEAN DEFAULT TRUE,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.metrics (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    hour INTEGER NOT NULL,
    metric_type VARCHAR(50) NOT NULL,
    value DECIMAL(12,2) NOT NULL,
    loaded_at TIMESTAMP DEFAULT NOW()
);

-- ===== DWH =====

CREATE TABLE IF NOT EXISTS dwh.orders (
    id VARCHAR(30) PRIMARY KEY,
    dt DATE NOT NULL,
    customer_id VARCHAR(20) NOT NULL,
    product VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dwh_orders_dt ON dwh.orders(dt);

-- ===== ANALYTICS =====

CREATE TABLE IF NOT EXISTS analytics.daily_summary (
    dt DATE NOT NULL,
    total_orders INTEGER,
    total_revenue DECIMAL(12,2),
    avg_order_value DECIMAL(10,2),
    unique_customers INTEGER,
    top_product VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (dt)
);
