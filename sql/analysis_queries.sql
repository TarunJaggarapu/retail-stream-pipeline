-- =====================================================
-- Standalone Analytical SQL Queries
-- Retail Streaming Analytics Pipeline
-- Run directly against BigQuery dbt-modeled tables
-- =====================================================

-- 1. Top 5 customers by total spend
SELECT
    user_name,
    user_city,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(total_amount_usd) AS total_spend_usd
FROM `retail-stream-pipeline.retail_analytics_dbt.fact_orders`
GROUP BY user_name, user_city
ORDER BY total_spend_usd DESC
LIMIT 5;


-- 2. Revenue contribution percentage by category
SELECT
    category,
    SUM(total_revenue_usd) AS category_revenue,
    ROUND(
        SUM(total_revenue_usd) * 100.0 / SUM(SUM(total_revenue_usd)) OVER (),
        2
    ) AS pct_of_total_revenue
FROM `retail-stream-pipeline.retail_analytics_dbt.kpi_category_revenue`
GROUP BY category
ORDER BY category_revenue DESC;


-- 3. Day-over-day revenue growth
SELECT
    order_day,
    total_revenue_usd,
    LAG(total_revenue_usd) OVER (ORDER BY order_day) AS prev_day_revenue,
    ROUND(
        (total_revenue_usd - LAG(total_revenue_usd) OVER (ORDER BY order_day))
        / NULLIF(LAG(total_revenue_usd) OVER (ORDER BY order_day), 0) * 100,
        2
    ) AS pct_change
FROM `retail-stream-pipeline.retail_analytics_dbt.kpi_daily_revenue`
ORDER BY order_day;


-- 4. Average order value by city
SELECT
    user_city,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(AVG(total_amount_usd), 2) AS avg_order_value_usd
FROM `retail-stream-pipeline.retail_analytics_dbt.fact_orders`
GROUP BY user_city
ORDER BY avg_order_value_usd DESC;


-- 5. Products that appear in the most distinct orders (popularity, not just revenue)
SELECT
    product_title,
    category,
    COUNT(DISTINCT order_id) AS num_orders_containing_product,
    SUM(quantity) AS total_units_sold
FROM `retail-stream-pipeline.retail_analytics_dbt.stg_order_items`
GROUP BY product_title, category
ORDER BY num_orders_containing_product DESC
LIMIT 10;


-- 6. Running total of revenue over time (cumulative revenue)
SELECT
    order_day,
    total_revenue_usd,
    SUM(total_revenue_usd) OVER (ORDER BY order_day) AS cumulative_revenue_usd
FROM `retail-stream-pipeline.retail_analytics_dbt.kpi_daily_revenue`
ORDER BY order_day;