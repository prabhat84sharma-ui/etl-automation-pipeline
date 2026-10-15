-- ==============================================================================
-- Analytical & Materialized Views for Power BI Dashboards
-- Optimized for high performance DirectQuery and scheduled semantic models
-- Author: Prabhat Sharma (prabhat84sharma-ui)
-- ==============================================================================

SET search_path TO analytics, public;

-- ==============================================================================
-- VIEW 1: CUSTOMER RFM & LIFETIME VALUE ANALYSIS (Power BI Customer Segment)
-- ==============================================================================
CREATE OR REPLACE VIEW vw_customer_rfm_analytics AS
WITH customer_order_metrics AS (
    SELECT
        c.customer_key,
        c.customer_id,
        c.full_name,
        c.email,
        c.customer_segment,
        c.country,
        c.state,
        COUNT(DISTINCT f.order_id)                                  AS total_orders,
        SUM(f.quantity)                                             AS total_items_purchased,
        ROUND(SUM(f.total_amount_usd)::NUMERIC, 2)                  AS lifetime_revenue_usd,
        ROUND(AVG(f.total_amount_usd)::NUMERIC, 2)                  AS average_order_value_usd,
        MIN(f.order_timestamp)                                      AS first_purchase_date,
        MAX(f.order_timestamp)                                      AS last_purchase_date,
        DATE_PART('day', CURRENT_TIMESTAMP - MAX(f.order_timestamp))::INT AS recency_days
    FROM dim_customer c
    INNER JOIN fact_orders f ON c.customer_key = f.customer_key
    WHERE f.order_status = 'COMPLETED'
    GROUP BY c.customer_key, c.customer_id, c.full_name, c.email, c.customer_segment, c.country, c.state
),
rfm_scoring AS (
    SELECT
        *,
        NTILE(5) OVER (ORDER BY recency_days DESC)     AS r_score,
        NTILE(5) OVER (ORDER BY total_orders ASC)      AS f_score,
        NTILE(5) OVER (ORDER BY lifetime_revenue_usd)  AS m_score
    FROM customer_order_metrics
)
SELECT
    customer_key,
    customer_id,
    full_name,
    email,
    customer_segment,
    country,
    state,
    total_orders,
    total_items_purchased,
    lifetime_revenue_usd,
    average_order_value_usd,
    first_purchase_date,
    last_purchase_date,
    recency_days,
    r_score,
    f_score,
    m_score,
    (r_score + f_score + m_score) AS composite_rfm_score,
    CASE
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
        WHEN f_score >= 4 AND m_score >= 3                 THEN 'Loyal Customers'
        WHEN r_score >= 3 AND f_score <= 2                 THEN 'Promising / New'
        WHEN r_score <= 2 AND f_score >= 3                 THEN 'At Risk'
        WHEN r_score = 1 AND f_score = 1                   THEN 'Hibernating / Lost'
        ELSE 'Potential Loyalist'
    END AS behavioral_segment
FROM rfm_scoring;

-- ==============================================================================
-- VIEW 2: PRODUCT CATEGORY & MARGIN PERFORMANCE (Power BI Merchandising)
-- ==============================================================================
CREATE OR REPLACE VIEW vw_product_category_performance AS
SELECT
    d.year_num,
    d.quarter_name,
    d.month_name,
    p.category,
    p.subcategory,
    p.brand,
    COUNT(DISTINCT f.order_id)                                  AS total_orders,
    SUM(f.quantity)                                             AS units_sold,
    ROUND(SUM(f.gross_amount_usd)::NUMERIC, 2)                  AS gross_sales_usd,
    ROUND(SUM(f.discount_amount_usd)::NUMERIC, 2)               AS total_discounts_usd,
    ROUND(SUM(f.total_amount_usd)::NUMERIC, 2)                  AS net_sales_usd,
    ROUND(SUM(f.quantity * p.unit_cost_usd)::NUMERIC, 2)       AS total_cost_usd,
    ROUND((SUM(f.total_amount_usd) - SUM(f.quantity * p.unit_cost_usd))::NUMERIC, 2) AS estimated_gross_profit_usd,
    ROUND(
        CASE 
            WHEN SUM(f.total_amount_usd) > 0 
            THEN ((SUM(f.total_amount_usd) - SUM(f.quantity * p.unit_cost_usd)) / SUM(f.total_amount_usd) * 100)::NUMERIC
            ELSE 0 
        END, 2
    ) AS profit_margin_pct
FROM fact_orders f
INNER JOIN dim_date d ON f.date_key = d.date_key
INNER JOIN dim_product p ON f.product_key = p.product_key
WHERE f.order_status = 'COMPLETED'
GROUP BY d.year_num, d.quarter_name, d.month_name, p.category, p.subcategory, p.brand;

-- ==============================================================================
-- VIEW 3: SALES CHANNEL ATTRIBUTION & EFFICIENCY (Power BI Omnichannel)
-- ==============================================================================
CREATE OR REPLACE VIEW vw_channel_attribution_efficiency AS
WITH channel_totals AS (
    SELECT
        ch.channel_key,
        ch.channel_name,
        ch.channel_type,
        ch.platform,
        COUNT(DISTINCT f.order_id)                                       AS order_count,
        SUM(f.quantity)                                                  AS units_sold,
        ROUND(SUM(f.total_amount_usd)::NUMERIC, 2)                       AS net_revenue_usd,
        ROUND(AVG(f.total_amount_usd)::NUMERIC, 2)                       AS average_order_value_usd,
        ROUND(AVG(f.discount_pct)::NUMERIC, 2)                           AS avg_discount_pct,
        COUNT(CASE WHEN f.order_status = 'CANCELLED' THEN 1 END)         AS cancelled_orders
    FROM dim_channel ch
    LEFT JOIN fact_orders f ON ch.channel_key = f.channel_key
    GROUP BY ch.channel_key, ch.channel_name, ch.channel_type, ch.platform
)
SELECT
    channel_key,
    channel_name,
    channel_type,
    platform,
    order_count,
    units_sold,
    net_revenue_usd,
    average_order_value_usd,
    avg_discount_pct,
    cancelled_orders,
    ROUND(
        (net_revenue_usd / NULLIF(SUM(net_revenue_usd) OVER (), 0) * 100)::NUMERIC, 2
    ) AS revenue_share_pct,
    ROUND(
        (cancelled_orders::NUMERIC / NULLIF(order_count, 0) * 100)::NUMERIC, 2
    ) AS cancellation_rate_pct
FROM channel_totals;

-- ==============================================================================
-- VIEW 4: DAILY EXECUTIVE KPI SCORECARD (Power BI Executive Dashboard)
-- ==============================================================================
CREATE OR REPLACE VIEW vw_executive_daily_kpis AS
SELECT
    d.full_date,
    d.year_num,
    d.month_name,
    d.day_name,
    d.is_weekend,
    COUNT(DISTINCT f.order_id)                                  AS total_orders,
    COUNT(DISTINCT f.customer_key)                              AS unique_purchasers,
    SUM(f.quantity)                                             AS total_units,
    ROUND(SUM(f.gross_amount_usd)::NUMERIC, 2)                  AS gross_revenue_usd,
    ROUND(SUM(f.discount_amount_usd)::NUMERIC, 2)               AS total_discount_usd,
    ROUND(SUM(f.tax_amount_usd)::NUMERIC, 2)                    AS total_tax_usd,
    ROUND(SUM(f.total_amount_usd)::NUMERIC, 2)                  AS net_revenue_usd,
    ROUND(AVG(f.total_amount_usd)::NUMERIC, 2)                  AS average_order_value_usd,
    ROUND(
        SUM(SUM(f.total_amount_usd)) OVER (
            PARTITION BY d.year_num, d.month_num 
            ORDER BY d.full_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )::NUMERIC, 2
    ) AS mtd_cumulative_revenue_usd
FROM dim_date d
LEFT JOIN fact_orders f ON d.date_key = f.date_key AND f.order_status = 'COMPLETED'
GROUP BY d.full_date, d.year_num, d.month_num, d.month_name, d.day_name, d.is_weekend;

-- ==============================================================================
-- VIEW 5: DATAOPS QUALITY HEALTH DASHBOARD (Monitoring & Governance)
-- ==============================================================================
CREATE OR REPLACE VIEW vw_data_quality_health_dashboard AS
SELECT
    l.run_id,
    l.table_name,
    l.check_type,
    l.status,
    l.total_records,
    l.failed_records,
    l.failure_rate_pct,
    l.quality_score,
    l.evaluated_at,
    a.pipeline_name,
    a.environment,
    a.duration_seconds,
    a.status AS pipeline_run_status
FROM etl_data_quality_log l
LEFT JOIN etl_pipeline_audit a ON l.run_id = a.run_id
ORDER BY l.evaluated_at DESC;

-- ==============================================================================
-- MATERIALIZED VIEW: MONTHLY SALES TREND (YoY & MoM Performance)
-- ==============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_monthly_sales_trend AS
WITH monthly_metrics AS (
    SELECT
        d.year_num,
        d.month_num,
        d.month_name,
        p.category,
        ch.channel_name,
        COUNT(DISTINCT f.order_id)                   AS total_orders,
        SUM(f.quantity)                              AS units_sold,
        ROUND(SUM(f.total_amount_usd)::NUMERIC, 2)   AS total_revenue_usd
    FROM fact_orders f
    INNER JOIN dim_date d ON f.date_key = d.date_key
    INNER JOIN dim_product p ON f.product_key = p.product_key
    INNER JOIN dim_channel ch ON f.channel_key = ch.channel_key
    WHERE f.order_status = 'COMPLETED'
    GROUP BY d.year_num, d.month_num, d.month_name, p.category, ch.channel_name
)
SELECT
    year_num,
    month_num,
    month_name,
    category,
    channel_name,
    total_orders,
    units_sold,
    total_revenue_usd,
    LAG(total_revenue_usd, 1) OVER (
        PARTITION BY category, channel_name ORDER BY year_num, month_num
    ) AS prior_month_revenue_usd,
    LAG(total_revenue_usd, 12) OVER (
        PARTITION BY category, channel_name ORDER BY year_num, month_num
    ) AS prior_year_revenue_usd,
    ROUND(
        CASE
            WHEN LAG(total_revenue_usd, 1) OVER (PARTITION BY category, channel_name ORDER BY year_num, month_num) > 0
            THEN (
                (total_revenue_usd - LAG(total_revenue_usd, 1) OVER (PARTITION BY category, channel_name ORDER BY year_num, month_num))
                / LAG(total_revenue_usd, 1) OVER (PARTITION BY category, channel_name ORDER BY year_num, month_num) * 100
            )::NUMERIC
            ELSE NULL
        END, 2
    ) AS mom_growth_pct,
    ROUND(
        CASE
            WHEN LAG(total_revenue_usd, 12) OVER (PARTITION BY category, channel_name ORDER BY year_num, month_num) > 0
            THEN (
                (total_revenue_usd - LAG(total_revenue_usd, 12) OVER (PARTITION BY category, channel_name ORDER BY year_num, month_num))
                / LAG(total_revenue_usd, 12) OVER (PARTITION BY category, channel_name ORDER BY year_num, month_num) * 100
            )::NUMERIC
            ELSE NULL
        END, 2
    ) AS yoy_growth_pct
FROM monthly_metrics;

-- Create unique index to allow CONCURRENT REFRESH
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_monthly_sales_trend 
ON mv_monthly_sales_trend (year_num, month_num, category, channel_name);
