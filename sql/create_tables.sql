-- ==============================================================================
-- Schema DDL: Enterprise Retail Star Schema & Audit Framework
-- Optimized for PostgreSQL and Power BI DirectQuery / Import
-- Author: Prabhat Sharma (prabhat84sharma-ui)
-- ==============================================================================

-- Create schema if required
CREATE SCHEMA IF NOT EXISTS analytics;
SET search_path TO analytics, public;

-- ==============================================================================
-- 1. DIMENSION: DATE (Calendar Dimension)
-- ==============================================================================
CREATE TABLE IF NOT EXISTS dim_date (
    date_key            INT PRIMARY KEY,              -- Format: YYYYMMDD
    full_date           DATE NOT NULL UNIQUE,
    day_of_week         INT NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
    day_name            VARCHAR(10) NOT NULL,
    day_of_month        INT NOT NULL CHECK (day_of_month BETWEEN 1 AND 31),
    day_of_year         INT NOT NULL CHECK (day_of_year BETWEEN 1 AND 366),
    week_of_year        INT NOT NULL CHECK (week_of_year BETWEEN 1 AND 53),
    month_num           INT NOT NULL CHECK (month_num BETWEEN 1 AND 12),
    month_name          VARCHAR(15) NOT NULL,
    quarter             INT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    quarter_name        VARCHAR(10) NOT NULL,
    year_num            INT NOT NULL,
    is_weekend          BOOLEAN NOT NULL,
    fiscal_year         INT NOT NULL,
    fiscal_quarter      VARCHAR(10) NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_date_full_date ON dim_date (full_date);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON dim_date (year_num, month_num);

-- ==============================================================================
-- 2. DIMENSION: CUSTOMER
-- ==============================================================================
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key        BIGSERIAL PRIMARY KEY,
    customer_id         VARCHAR(64) NOT NULL UNIQUE,  -- Natural Key from Source
    first_name          VARCHAR(100) NOT NULL,
    last_name           VARCHAR(100) NOT NULL,
    full_name           VARCHAR(200) GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
    email               VARCHAR(255) NOT NULL,
    phone               VARCHAR(50),
    city                VARCHAR(100),
    state               VARCHAR(100),
    country             VARCHAR(100) NOT NULL DEFAULT 'USA',
    postal_code         VARCHAR(20),
    customer_segment    VARCHAR(50) NOT NULL DEFAULT 'Standard',
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_customer_id ON dim_customer (customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_customer_email ON dim_customer (email);
CREATE INDEX IF NOT EXISTS idx_dim_customer_segment ON dim_customer (customer_segment);
CREATE INDEX IF NOT EXISTS idx_dim_customer_geo ON dim_customer (country, state, city);

-- ==============================================================================
-- 3. DIMENSION: PRODUCT
-- ==============================================================================
CREATE TABLE IF NOT EXISTS dim_product (
    product_key         BIGSERIAL PRIMARY KEY,
    product_id          VARCHAR(64) NOT NULL UNIQUE,  -- Natural Key
    product_name        VARCHAR(255) NOT NULL,
    category            VARCHAR(100) NOT NULL,
    subcategory         VARCHAR(100),
    brand               VARCHAR(100),
    unit_cost_usd       NUMERIC(12, 4) NOT NULL DEFAULT 0.0000,
    unit_price_usd      NUMERIC(12, 4) NOT NULL DEFAULT 0.0000,
    margin_usd          NUMERIC(12, 4) GENERATED ALWAYS AS (unit_price_usd - unit_cost_usd) STORED,
    status              VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_product_id ON dim_product (product_id);
CREATE INDEX IF NOT EXISTS idx_dim_product_cat_sub ON dim_product (category, subcategory);
CREATE INDEX IF NOT EXISTS idx_dim_product_brand ON dim_product (brand);

-- ==============================================================================
-- 4. DIMENSION: SALES CHANNEL
-- ==============================================================================
CREATE TABLE IF NOT EXISTS dim_channel (
    channel_key         SERIAL PRIMARY KEY,
    channel_id          VARCHAR(32) NOT NULL UNIQUE,
    channel_name        VARCHAR(100) NOT NULL,
    channel_type        VARCHAR(50) NOT NULL,        -- 'E-COMMERCE', 'MOBILE_APP', 'RETAIL', 'B2B'
    platform            VARCHAR(50) NOT NULL,        -- 'Shopify', 'Amazon', 'Direct Web', 'POS'
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_channel_id ON dim_channel (channel_id);

-- ==============================================================================
-- 5. FACT TABLE: ORDERS (Grain: One row per order line item)
-- ==============================================================================
CREATE TABLE IF NOT EXISTS fact_orders (
    order_key              BIGSERIAL PRIMARY KEY,
    order_id               VARCHAR(64) NOT NULL,
    order_line_item_id     VARCHAR(64) NOT NULL,
    customer_key           BIGINT NOT NULL REFERENCES dim_customer (customer_key),
    product_key            BIGINT NOT NULL REFERENCES dim_product (product_key),
    channel_key            INT NOT NULL REFERENCES dim_channel (channel_key),
    date_key               INT NOT NULL REFERENCES dim_date (date_key),
    order_timestamp        TIMESTAMPTZ NOT NULL,
    quantity               INT NOT NULL CHECK (quantity > 0),
    unit_price_raw         NUMERIC(12, 4) NOT NULL,
    currency_code          VARCHAR(3) NOT NULL DEFAULT 'USD',
    exchange_rate_to_usd   NUMERIC(12, 6) NOT NULL DEFAULT 1.000000,
    unit_price_usd         NUMERIC(12, 4) NOT NULL,
    discount_pct           NUMERIC(5, 2) NOT NULL DEFAULT 0.00 CHECK (discount_pct BETWEEN 0 AND 100),
    discount_amount_usd    NUMERIC(12, 4) NOT NULL DEFAULT 0.0000,
    tax_amount_usd         NUMERIC(12, 4) NOT NULL DEFAULT 0.0000,
    gross_amount_usd       NUMERIC(12, 4) GENERATED ALWAYS AS (quantity * unit_price_usd) STORED,
    total_amount_usd       NUMERIC(12, 4) NOT NULL,
    order_status           VARCHAR(30) NOT NULL DEFAULT 'COMPLETED',
    created_at             TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fact_orders_natural_key UNIQUE (order_id, order_line_item_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_orders_order_id ON fact_orders (order_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer ON fact_orders (customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_product ON fact_orders (product_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_channel ON fact_orders (channel_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_date ON fact_orders (date_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_timestamp ON fact_orders (order_timestamp);
CREATE INDEX IF NOT EXISTS idx_fact_orders_status ON fact_orders (order_status);
CREATE INDEX IF NOT EXISTS idx_fact_orders_cust_date ON fact_orders (customer_key, date_key);

-- ==============================================================================
-- 6. AGGREGATED FACT: DAILY SALES SUMMARY
-- ==============================================================================
CREATE TABLE IF NOT EXISTS fact_daily_sales_summary (
    summary_key             BIGSERIAL PRIMARY KEY,
    date_key                INT NOT NULL REFERENCES dim_date (date_key),
    channel_key             INT NOT NULL REFERENCES dim_channel (channel_key),
    category                VARCHAR(100) NOT NULL,
    total_orders            INT NOT NULL DEFAULT 0,
    total_units_sold        INT NOT NULL DEFAULT 0,
    gross_revenue_usd       NUMERIC(14, 4) NOT NULL DEFAULT 0.0000,
    total_discount_usd      NUMERIC(14, 4) NOT NULL DEFAULT 0.0000,
    total_tax_usd           NUMERIC(14, 4) NOT NULL DEFAULT 0.0000,
    net_revenue_usd         NUMERIC(14, 4) NOT NULL DEFAULT 0.0000,
    average_order_value_usd NUMERIC(12, 4) NOT NULL DEFAULT 0.0000,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_daily_summary UNIQUE (date_key, channel_key, category)
);

CREATE INDEX IF NOT EXISTS idx_daily_summary_date ON fact_daily_sales_summary (date_key);

-- ==============================================================================
-- 7. AUDIT LOG: ETL PIPELINE RUN AUDIT
-- ==============================================================================
CREATE TABLE IF NOT EXISTS etl_pipeline_audit (
    audit_id                BIGSERIAL PRIMARY KEY,
    run_id                  VARCHAR(100) NOT NULL UNIQUE,
    pipeline_name           VARCHAR(100) NOT NULL,
    environment             VARCHAR(20) NOT NULL,
    status                  VARCHAR(20) NOT NULL,  -- 'RUNNING', 'SUCCESS', 'FAILED', 'WARNING'
    records_extracted       INT NOT NULL DEFAULT 0,
    records_transformed     INT NOT NULL DEFAULT 0,
    records_loaded          INT NOT NULL DEFAULT 0,
    start_time              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_time                TIMESTAMPTZ,
    duration_seconds        NUMERIC(10, 2),
    error_message           TEXT,
    executed_by             VARCHAR(100) NOT NULL DEFAULT 'Airflow_Scheduler'
);

CREATE INDEX IF NOT EXISTS idx_etl_audit_run_id ON etl_pipeline_audit (run_id);
CREATE INDEX IF NOT EXISTS idx_etl_audit_status ON etl_pipeline_audit (status);
CREATE INDEX IF NOT EXISTS idx_etl_audit_start_time ON etl_pipeline_audit (start_time DESC);

-- ==============================================================================
-- 8. AUDIT LOG: DATA QUALITY AUDIT
-- ==============================================================================
CREATE TABLE IF NOT EXISTS etl_data_quality_log (
    log_id                  BIGSERIAL PRIMARY KEY,
    run_id                  VARCHAR(100) NOT NULL,
    table_name              VARCHAR(100) NOT NULL,
    check_name              VARCHAR(100) NOT NULL,
    check_type              VARCHAR(50) NOT NULL,  -- 'NULL_CHECK', 'DUPLICATE_CHECK', 'RANGE_CHECK', 'INTEGRITY'
    column_name             VARCHAR(100),
    status                  VARCHAR(20) NOT NULL,  -- 'PASS', 'FAIL', 'WARN'
    total_records           INT NOT NULL,
    failed_records          INT NOT NULL,
    failure_rate_pct        NUMERIC(6, 3) NOT NULL,
    quality_score           NUMERIC(5, 2) NOT NULL,
    details                 JSONB,
    evaluated_at            TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dq_log_run_id ON etl_data_quality_log (run_id);
CREATE INDEX IF NOT EXISTS idx_dq_log_table ON etl_data_quality_log (table_name);
CREATE INDEX IF NOT EXISTS idx_dq_log_status ON etl_data_quality_log (status);
