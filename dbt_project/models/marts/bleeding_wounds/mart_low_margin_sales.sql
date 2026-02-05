{{
  config(
    materialized='table',
    tags=['mart', 'alert', 'low_margin']
  )
}}

/*
Mart: Low Margin Sales Analysis
Purpose: Identify sales with insufficient profit margins
Business Impact: Revenue without profit, unsustainable pricing
*/

WITH sales AS (
  SELECT * FROM {{ ref('int_sales_with_metrics') }}
),

low_margin_analysis AS (
  SELECT
    transaction_id,
    transaction_date,
    customer_id,
    customer_name,
    product_id,
    product_name,
    quantity,
    unit_price,
    cost_price,
    margin_percentage,
    total_amount,
    
    -- Calculate margin loss
    unit_margin * quantity AS total_margin_earned,
    ({{ var('low_margin_threshold') }} * unit_price * quantity) - (unit_margin * quantity) AS margin_gap_vs_target,
    
    -- Comparison to product average
    avg_product_margin,
    margin_percentage - avg_product_margin AS margin_vs_product_avg,
    
    -- Alert severity
    CASE
      WHEN margin_percentage < 0 THEN 'CRITICAL'  -- Selling at loss
      WHEN margin_percentage < 0.05 THEN 'HIGH'    -- Less than 5%
      WHEN margin_percentage < {{ var('low_margin_threshold') }} THEN 'MEDIUM'
      ELSE 'LOW'
    END AS alert_severity,
    
    -- Root cause analysis
    CASE
      WHEN margin_percentage < 0 THEN 'Selling below cost price'
      WHEN cost_price IS NULL THEN 'Missing cost data - cannot calculate margin'
      WHEN margin_percentage < avg_product_margin THEN 'Below product average - excessive discount'
      ELSE 'General low margin issue'
    END AS probable_cause,
    
    -- Recommended action
    CASE
      WHEN margin_percentage < 0 THEN 'URGENT: Review pricing. Currently making loss!'
      WHEN margin_percentage < 0.05 THEN 'Renegotiate with customer or stop sales'
      ELSE 'Reduce discounts, increase price'
    END AS recommended_action,
    
    -- Analysis metadata
    CURRENT_DATE() AS analysis_date,
    CURRENT_TIMESTAMP() AS created_at
    
  FROM sales
  WHERE margin_percentage < {{ var('low_margin_threshold') }}
    AND cost_price IS NOT NULL
    AND transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)  -- Last 90 days only
)

SELECT * FROM low_margin_analysis
ORDER BY alert_severity DESC, margin_gap_vs_target DESC
