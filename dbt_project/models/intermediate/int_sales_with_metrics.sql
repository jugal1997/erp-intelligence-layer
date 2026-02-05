{{
  config(
    materialized='ephemeral',
    tags=['intermediate', 'sales']
  )
}}

/*
Intermediate model: Sales with customer and product metrics
Purpose: Add aggregated metrics for deeper analysis
*/

WITH sales AS (
  SELECT * FROM {{ ref('stg_sales_transactions') }}
),

-- Calculate customer-level metrics
customer_metrics AS (
  SELECT
    customer_id,
    customer_name,
    COUNT(DISTINCT transaction_id) AS total_transactions,
    SUM(total_amount) AS lifetime_value,
    AVG(margin_percentage) AS avg_customer_margin,
    MAX(transaction_date) AS last_purchase_date,
    DATE_DIFF(CURRENT_DATE(), MAX(transaction_date), DAY) AS days_since_last_purchase,
    
    -- Credit metrics
    SUM(CASE WHEN payment_status != 'PAID' THEN total_amount ELSE 0 END) AS total_outstanding,
    COUNT(CASE WHEN days_overdue > 0 THEN 1 END) AS overdue_invoice_count
    
  FROM sales
  GROUP BY customer_id, customer_name
),

-- Calculate product-level metrics
product_metrics AS (
  SELECT
    product_id,
    product_name,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(quantity) AS total_quantity_sold,
    AVG(margin_percentage) AS avg_product_margin,
    MAX(transaction_date) AS last_sold_date,
    DATE_DIFF(CURRENT_DATE(), MAX(transaction_date), DAY) AS days_since_last_sold,
    
    -- Sales velocity (last 30 days)
    SUM(CASE 
      WHEN transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
      THEN quantity 
      ELSE 0 
    END) AS quantity_sold_last_30d
    
  FROM sales
  GROUP BY product_id, product_name
),

-- Join everything together
enriched_sales AS (
  SELECT
    s.*,
    
    -- Customer metrics
    cm.lifetime_value AS customer_lifetime_value,
    cm.days_since_last_purchase,
    cm.avg_customer_margin,
    cm.total_outstanding AS customer_total_outstanding,
    cm.overdue_invoice_count AS customer_overdue_count,
    
    -- Product metrics
    pm.avg_product_margin,
    pm.last_sold_date AS product_last_sold_date,
    pm.days_since_last_sold AS product_days_since_last_sold,
    pm.quantity_sold_last_30d AS product_velocity_30d
    
  FROM sales s
  LEFT JOIN customer_metrics cm 
    ON s.customer_id = cm.customer_id
  LEFT JOIN product_metrics pm 
    ON s.product_id = pm.product_id
)

SELECT * FROM enriched_sales
