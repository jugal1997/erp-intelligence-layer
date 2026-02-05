{{
  config(
    materialized='table',
    tags=['mart', 'alert', 'dead_stock']
  )
}}

/*
Mart: Dead Stock Analysis
Purpose: Identify products not selling for extended periods
Business Impact: Cash locked in inventory, warehouse space wasted
*/

WITH sales AS (
  SELECT * FROM {{ ref('int_sales_with_metrics') }}
),

-- Get the most recent sale date for each product
product_last_sale AS (
  SELECT
    product_id,
    product_name,
    MAX(transaction_date) AS last_sale_date,
    DATE_DIFF(CURRENT_DATE(), MAX(transaction_date), DAY) AS days_since_last_sale,
    SUM(quantity) AS total_quantity_ever_sold,
    AVG(margin_percentage) AS avg_margin
    
  FROM sales
  GROUP BY product_id, product_name
),

-- Classify by severity
dead_stock_analysis AS (
  SELECT
    product_id,
    product_name,
    last_sale_date,
    days_since_last_sale,
    total_quantity_ever_sold,
    avg_margin,
    
    -- Estimated value locked (placeholder calculation)
    -- In real scenario, you'd join with inventory table for current stock
    ROUND(total_quantity_ever_sold * 0.1 * 4000, 2) AS estimated_value_locked,
    
    -- Alert severity
    CASE
      WHEN days_since_last_sale >= 180 THEN 'CRITICAL'
      WHEN days_since_last_sale >= 120 THEN 'HIGH'
      WHEN days_since_last_sale >= {{ var('dead_stock_days') }} THEN 'MEDIUM'
      ELSE 'LOW'
    END AS alert_severity,
    
    -- Recommended action
    CASE
      WHEN days_since_last_sale >= 180 THEN 'Liquidate immediately at 20-30% discount'
      WHEN days_since_last_sale >= 120 THEN 'Run promotional offer within 2 weeks'
      WHEN days_since_last_sale >= 90 THEN 'Monitor closely, prepare promotion'
      ELSE 'Normal - continue monitoring'
    END AS recommended_action,
    
    -- Analysis metadata
    CURRENT_DATE() AS analysis_date,
    CURRENT_TIMESTAMP() AS created_at
    
  FROM product_last_sale
  WHERE days_since_last_sale >= {{ var('dead_stock_days') }}
)

SELECT * FROM dead_stock_analysis
ORDER BY alert_severity DESC, days_since_last_sale DESC
